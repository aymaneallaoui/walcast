# walcast

Postgres change data capture pipeline. Streams row changes from the write-ahead log over logical replication.

Status: scaffolding only. No replication logic yet.

## Dev

Needs Go, Docker, and [golangci-lint](https://golangci-lint.run) v2.

```sh
cp .env.example .env
make up     # Postgres with wal_level=logical
make run    # Ctrl+C for graceful shutdown
```

Other targets: `make build`, `make test`, `make lint`, `make fmt`, `make down`.

## Config

Read from the environment. A `.env` file is loaded if present and never overrides real variables.

| Variable | Default | Notes |
| --- | --- | --- |
| `DATABASE_URL` | required | Postgres connection string |
| `LOG_LEVEL` | `info` | `trace`, `debug`, `info`, `warn`, `error` |
| `LOG_FORMAT` | `json` | `json` or `console` |
| `SHUTDOWN_TIMEOUT` | `10s` | Go duration |

## Layout

```
cmd/walcast        entrypoint, signal handling
internal/app       application lifecycle
internal/config    env config
internal/logger    zerolog setup
```
