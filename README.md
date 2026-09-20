# walcast

Postgres change data capture pipeline. Streams row changes from the write-ahead log over logical replication (`pgoutput`) and emits them as JSON.

![architecture](docs/architecture.png)

## Dev

Needs Go, Docker, and [golangci-lint](https://golangci-lint.run) v2.

```sh
cp .env.example .env
make up     # Postgres with wal_level=logical
make run    # events on stdout, logs on stderr, Ctrl+C for graceful shutdown
```

Other targets: `make build`, `make test`, `make test-integration` (needs `DATABASE_URL`, read from `.env`), `make bench`, `make fuzz`, `make vuln`, `make lint`, `make fmt`, `make down`. If port 5432 is taken, set `POSTGRES_PORT` in `.env` and match it in `DATABASE_URL`.

The publication and the replication slot are created on first start if missing. An existing publication is never altered; a mismatch with `PUBLICATION_TABLES` is logged. If the slot disappears or is invalidated while walcast runs, it exits instead of recreating it, because a fresh slot would silently skip every change made in between.

## Events

One JSON object per line:

```json
{"table":"public.users","op":"update","lsn":"0/16B3700","commit_lsn":"0/16B3748","seq":0,"txid":742,"ts":"2026-09-20T10:00:00Z","old":{"id":7},"new":{"id":7,"name":"x"},"unchanged":["bio"]}
```

- `op` is `insert`, `update`, `delete` or `truncate`.
- `commit_lsn` + `seq` identify an event; use them to deduplicate.
- `old` holds the replica identity columns only, or the full row with `REPLICA IDENTITY FULL`.
- `unchanged` lists TOASTed columns Postgres did not resend. They are absent from `new`, not null.
- Values: booleans, integers and floats are native JSON (`NaN` and `±Infinity` become strings), `jsonb` is embedded as is and `json` with its raw line breaks removed, `numeric` is a string to keep precision, everything else keeps its Postgres text form. Invalid UTF-8 becomes U+FFFD.

## Sinks

`SINK=stdout` (default) writes the events to stdout, logs go to stderr.

`SINK=webhook` POSTs each batch to `WEBHOOK_URL` as `application/x-ndjson`, one event per line, one request in flight at a time so a retry is never overtaken by a later batch.

| Header | Meaning |
| --- | --- |
| `X-Walcast-Signature` | `t=<unix>,v1=<hex>`, where `v1` is HMAC-SHA256 of `<unix>.<body>` keyed with `WEBHOOK_SECRET`. Verify it and reject timestamps outside your tolerance to stop replays. |
| `X-Walcast-Idempotency-Key` | Stable across retries of the same batch. |
| `X-Walcast-Events` | Number of events in the body. |
| `X-Walcast-Attempt` | 1 for the first try. |

A 2xx response means the batch is safely stored on your side: only then is its LSN confirmed to Postgres. 5xx, 408, 429 and network errors are retried forever with capped backoff (`Retry-After` in seconds is honoured); Postgres keeps the WAL meanwhile, so set `max_slot_wal_keep_size`. Any other status, including a redirect, is a permanent rejection and stops the process rather than skipping the batch. Batch boundaries can differ after a restart, so deduplicate on `commit_lsn` + `seq`, not on the idempotency key alone.

## Delivery

At-least-once. A transaction's end LSN is confirmed to Postgres only after every batch holding its events, and every batch before it, has been delivered by the sink. A restart or reconnect resumes from the slot's confirmed LSN, so unconfirmed events are replayed, never lost. The stdout sink is best-effort: a successful write is treated as delivered. The webhook sink treats a 2xx response as delivered.

Deterministic failures (an unencodable change, an unusable slot, a batch the webhook receiver rejects) stop the process instead of retrying forever.

Large transactions are split across batches; only the batch holding the commit carries an LSN to confirm. A slow sink applies backpressure to Postgres while status updates keep flowing, so `wal_sender_timeout` does not drop the connection. Transactions are decoded with protocol version 1, so Postgres buffers a transaction until it commits.

## Config

Read from the environment. A `.env` file is loaded if present and never overrides real variables.

| Variable | Default | Notes |
| --- | --- | --- |
| `DATABASE_URL` | required | Postgres connection string, user needs `REPLICATION` |
| `SLOT_NAME` | `walcast_slot` | lowercase letters, digits, underscore |
| `PUBLICATION_NAME` | `walcast_pub` | same charset |
| `PUBLICATION_TABLES` | all tables | comma separated `schema.table`; all tables needs superuser |
| `LOG_LEVEL` | `info` | `trace`, `debug`, `info`, `warn`, `error` |
| `LOG_FORMAT` | `json` | `json` or `console` |
| `SHUTDOWN_TIMEOUT` | `10s` | drain deadline on SIGINT/SIGTERM |
| `FEEDBACK_INTERVAL` | `5s` | standby status updates, keep well under `wal_sender_timeout` |
| `SERVER_TIMEOUT` | `60s` | reconnect when Postgres sends nothing for this long |
| `BATCH_MAX_BYTES` | `65536` | batch size before it is sealed |
| `BATCH_LINGER` | `5ms` | max wait for more events at a commit boundary |
| `INFLIGHT_MAX_BYTES` | `67108864` | memory bound for undelivered batches |
| `RECONNECT_MIN_DELAY` | `500ms` | backoff floor, doubles with jitter |
| `RECONNECT_MAX_DELAY` | `30s` | backoff cap |
| `SINK` | `stdout` | `stdout` or `webhook` |
| `WEBHOOK_URL` | required for webhook | `https`, or `http` to a loopback host; no credentials in the URL |
| `WEBHOOK_SECRET` | required for webhook | at least 16 characters, signs every request |
| `WEBHOOK_TIMEOUT` | `10s` | per request |
| `WEBHOOK_RETRY_MIN` | `500ms` | retry backoff floor, doubles with jitter |
| `WEBHOOK_RETRY_MAX` | `30s` | retry backoff cap |

## Layout

```
cmd/walcast            entrypoint, signal handling
internal/app           supervisor: reconnect with backoff
internal/replication   connection, publication + slot setup, receive loop, feedback
internal/event         pgoutput to JSON encoder, pooled batches
internal/ledger        in-order ack tracking
internal/sink          Sink interface, writer and webhook sinks
internal/backoff       jittered exponential backoff
internal/config        env config
internal/logger        zerolog setup
```
