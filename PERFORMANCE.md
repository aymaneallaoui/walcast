# Performance

Numbers from one machine, so read them as ratios and orders of magnitude, not as promises.

- CPU: Intel Core i9-13980HX (32 threads), Linux under WSL2
- Go 1.27.1, Postgres 17 in Docker on the same host (`postgres:17-alpine`, `wal_level=logical`)
- Rows of six columns (`bigint`, two `text`, `boolean`, `numeric`, `timestamptz`), about 300 bytes as a JSON event

Reproduce with `make bench` for the micro benchmarks and `make bench-e2e` for the end-to-end ones, which start their own Postgres and need only Docker.

## End to end

A sink that acknowledges at once, so these measure walcast and Postgres, not a receiver. Three runs each, 200,000 rows per run.

| Path | Throughput |
| --- | --- |
| Streaming, transactions of 1,000 rows committed while connected | 255k to 279k events/s, about 80 MB/s |
| Catch-up, rows already in the WAL when walcast connects | 269k to 310k events/s, about 88 MB/s |
| Backfill, `BACKFILL_CHUNK_ROWS=500` | 27k rows/s, 8 MB/s |
| Backfill, `BACKFILL_CHUNK_ROWS=2000` | 100k rows/s, 30 MB/s |
| Backfill, `BACKFILL_CHUNK_ROWS=10000` (default) | 357k rows/s, 106 MB/s |

A real sink is the limit in practice. The webhook sink sends one request at a time and the Kafka sink waits for broker acknowledgements, so end-to-end rate is whatever the receiver sustains; walcast applies backpressure to Postgres and keeps status updates flowing meanwhile.

## What measuring the backfill found

The first backfill measurement was 2,400, 9,800 and 48,000 rows/s for the three chunk sizes: throughput proportional to chunk size, so every chunk cost a fixed 204 ms whatever it held. That is a wait, not work. Three causes, found by timing each phase of a chunk:

1. **The marker was not flushed.** A non-transactional `pg_logical_emit_message` is not flushed when it is written, and a walsender only streams flushed WAL, so on a quiet database the marker waited for the WAL writer (`wal_writer_delay`, 200 ms). Postgres 17 can flush with the message; on 14 to 16 walcast commits a tiny transaction right after it. 204 ms per chunk became 28 ms.
2. **The chunk's last batch waited for the batch linger** (5 ms). A chunk's end is a flush point, because the worker sends nothing more until that batch is delivered, so it is now sealed at once.
3. **Delivery was noticed by a 5 ms poll.** It is 1 ms now, and only runs while a chunk is awaited.

Together: 7 to 11 times faster. What remains of a 2,000-row chunk's 20 ms is the read itself (16.5 ms for the snapshot and the `SELECT`), which larger chunks amortise; that is why the default went from 2,000 to 10,000 rows. Memory is bounded by `BACKFILL_CHUNK_BYTES`, not by the row count.

## Where the CPU goes

CPU profile of the catch-up run. The owner goroutine is one core by design: WAL is a single ordered stream, and one goroutine that receives, decodes and encodes keeps ordering free.

| Share of samples | What |
| --- | --- |
| 27% | reading the socket (`syscall.Read` under `pgproto3`) |
| 13% + 10% | garbage collection and allocation |
| 9% | `pglogrepl.Parse` decoding messages into structs |
| 6% | walcast's JSON encoder |

Nearly all allocation comes from `pglogrepl.Parse`, 8 allocations per message; the encoder makes none. So the ceiling is the decode library's allocations and the collection they cause, not the encoder. An allocation-free decoder for insert, update and delete is the known next step if more throughput is ever needed. It was not worth its complexity at these rates.

## Micro benchmarks

| Benchmark | Result |
| --- | --- |
| `BenchmarkInsert` (encode one row change to JSON) | about 130 ns, 0 allocs |
| `BenchmarkSession_handleXLogData` (parse, encode, batch) | about 212 ns, 144 B, 8 allocs, all from `pglogrepl.Parse` |

Every change to these paths was measured against the previous commit with `benchstat` over interleaved runs of both binaries, because runs minutes apart on a laptop drift by more than the effects in question. Findings kept from that:

- Ignoring walcast's own state table first cost 14.7% on the encoder. Ignored relations now live in their own set filled once per `Relation` message, and splitting the lookup's miss path brought it under the inlining budget: no measurable difference from before the feature.
- In the Kafka sink, binding a method value per record allocated a closure per record (about 500 to 190 allocations per batch), and franz-go's default 10 ms producer linger made a batch take 10.5 ms instead of 0.2 ms.
- Metrics cost 0.6% on the receive path (212.1 to 213.5 ns, p=0.001). The first version cost 2.7%; keeping the commit time as 8 bytes instead of a 24-byte `time.Time` and moving cold fields to the end of the session struct removed most of it. Counters and latencies are updated once per delivered batch on the sink's goroutine, and gauges are read at scrape time, so nothing else touches the decode path.

Two guesses that measuring disproved, recorded so nobody repeats them: reordering the encoder's struct fields made no difference, and an atomic store per WAL message was not what the metrics cost.
