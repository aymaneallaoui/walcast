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

Other targets: `make build`, `make test`, `make test-integration` (uses `DATABASE_URL` from `.env` when set, otherwise starts its own Postgres container and needs only Docker), `make bench`, `make fuzz`, `make vuln`, `make lint`, `make fmt`, `make down`. If port 5432 is taken, set `POSTGRES_PORT` in `.env` and match it in `DATABASE_URL`.

The publication and the replication slot are created on first start if missing. An existing publication is never altered; a mismatch with `PUBLICATION_TABLES` is logged. If the slot disappears or is invalidated while walcast runs, it exits instead of recreating it, because a fresh slot would silently skip every change made in between.

## Events

One JSON object per line:

```json
{"table":"public.users","op":"update","lsn":"0/16B3700","commit_lsn":"0/16B3748","seq":0,"txid":742,"ts":"2026-09-20T10:00:00Z","old":{"id":7},"new":{"id":7,"name":"x"},"unchanged":["bio"]}
```

- `op` is `insert`, `update`, `delete` or `truncate`.
- An update that changes the row's replica identity (its primary key) is emitted as a `delete` of the old key followed by an `insert` of the new one, both carrying `"origin":"update"`. Every key's history then stays self-consistent for consumers partitioned by key.
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

A 2xx response means the batch is safely stored on your side: only then is its LSN confirmed to Postgres. 5xx, 408, 429 and network errors are retried forever with capped backoff (`Retry-After` is honoured in full, up to one hour, even above `WEBHOOK_RETRY_MAX`); Postgres keeps the WAL meanwhile, so set `max_slot_wal_keep_size`. Any other status, including a redirect, is a permanent rejection and stops the process rather than skipping the batch. The error names the status and your `X-Request-Id` response header if you send one; the response body is never logged, since it may echo row data. Batch boundaries can differ after a restart, so deduplicate on `commit_lsn` + `seq`, not on the idempotency key alone.

`SINK=kafka` produces one record per event to the topic `KAFKA_TOPIC_PREFIX` + `schema.table`, for example `walcast.public.users`. Topics must exist unless the broker auto-creates them; a missing topic is retried with backoff until it appears.

- Value: the event JSON. Key: the row's replica identity as compact JSON, for example `{"id":7}`. All changes of a row share a partition, so consumers see them in commit order.
- Rows without a stable identity (no primary key, `REPLICA IDENTITY FULL` or `NOTHING`, truncates) are keyed by table name, which keeps them ordered in one partition.
- Truncates are not sent unless `KAFKA_EMIT_TRUNCATE=true`. A truncate is keyed by table while rows are keyed by identity, so a consumer could read it after a later insert from another partition and wipe that new row. A skipped truncate is logged.
- The producer is idempotent with `acks=all`: broker-side retries neither duplicate nor reorder records within a partition. A replay after a restart can still duplicate, so deduplicate on `commit_lsn` + `seq`.
- An LSN is confirmed to Postgres only after every record of its batch was acknowledged by the brokers. Errors Kafka marks non-retriable (record too large, authorization, invalid topic) stop the process.
- TLS is required unless every broker is a loopback host, and always with SASL (`plain`, `scram-sha-256`, `scram-sha-512`). Without TLS, traffic also goes in plaintext to whatever brokers the bootstrap ones advertise. A private CA is picked up from the system pool, which `SSL_CERT_FILE` or `SSL_CERT_DIR` can point at.
- Table names that cannot map to a topic unambiguously (quoted identifiers with dots, spaces or non-ASCII, over 249 bytes with the prefix) are sanitised and get a short hash of the exact schema and table, so two tables never share a topic; the chosen topic is logged.
- A record above `KAFKA_MAX_MESSAGE_BYTES` stops the process. The error names the topic, the size and the event's `commit_lsn` and `seq`, never the row key or data. Raise it together with the topic's `max.message.bytes`.

## Delivery

At-least-once. A transaction's end LSN is confirmed to Postgres only after every batch holding its events, and every batch before it, has been delivered by the sink. A restart or reconnect resumes from the slot's confirmed LSN, so unconfirmed events are replayed, never lost. The stdout sink is best-effort: a successful write is treated as delivered. The webhook sink treats a 2xx response as delivered, the Kafka sink a broker acknowledgement of every record.

Failures a reconnect cannot fix (an unencodable change, an unusable slot, deliveries that never settle, a batch the webhook receiver or Kafka rejects for good) stop the process instead of retrying forever.

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
| `SINK_SETTLE_TIMEOUT` | `5m` | after a failed session, how long to wait for its in-flight deliveries before exiting so a restart can drop the stuck client |
| `FEEDBACK_INTERVAL` | `5s` | standby status updates, keep well under `wal_sender_timeout` |
| `SERVER_TIMEOUT` | `60s` | reconnect when Postgres sends nothing for this long |
| `BATCH_MAX_BYTES` | `65536` | batch size before it is sealed |
| `BATCH_LINGER` | `5ms` | max wait for more events at a commit boundary |
| `INFLIGHT_MAX_BYTES` | `67108864` | memory bound for undelivered batches |
| `RECONNECT_MIN_DELAY` | `500ms` | backoff floor, doubles with jitter |
| `RECONNECT_MAX_DELAY` | `30s` | backoff cap |
| `SINK` | `stdout` | `stdout`, `webhook` or `kafka` |
| `WEBHOOK_URL` | required for webhook | `https`, or `http` to a loopback host; no credentials in the URL |
| `WEBHOOK_SECRET` | required for webhook | at least 16 characters, signs every request |
| `WEBHOOK_TIMEOUT` | `10s` | per request |
| `WEBHOOK_RETRY_MIN` | `500ms` | retry backoff floor, doubles with jitter |
| `WEBHOOK_RETRY_MAX` | `30s` | retry backoff cap |
| `KAFKA_BROKERS` | required for kafka | comma separated `host:port` |
| `KAFKA_TOPIC_PREFIX` | `walcast.` | topic is prefix + `schema.table` |
| `KAFKA_CLIENT_ID` | `walcast` | |
| `KAFKA_TLS` | `false` | must be `true` unless every broker is a loopback host |
| `KAFKA_SASL_MECHANISM` | none | `plain`, `scram-sha-256` or `scram-sha-512` |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | | required with a mechanism |
| `KAFKA_EMIT_TRUNCATE` | `false` | send truncate events, see the ordering caveat above |
| `KAFKA_MAX_MESSAGE_BYTES` | `1000012` | largest record batch, match the topic's `max.message.bytes` |

## Layout

```
cmd/walcast            entrypoint, signal handling
internal/app           supervisor: reconnect with backoff
internal/replication   connection, publication + slot setup, receive loop, feedback
internal/event         pgoutput to JSON encoder, pooled batches
internal/ledger        in-order ack tracking
internal/sink          Sink interface, writer, webhook and Kafka sinks
internal/backoff       jittered exponential backoff
internal/config        env config
internal/logger        zerolog setup
```
