# Solution (WIP)

## How to run

Requires Go 1.25+ (https://go.dev/dl/). No other system dependencies.

```bash
cd my_solution
go run .
```

The first run downloads the Go module dependencies (chi router) automatically. The service listens on `:8080`:

```
POST /events                              event ingestion
GET  /devices/{device_id}/health
GET  /rooms/{room_id}/occupancy?window=1m|5m|1h
GET  /alarms?since=<unix_ts>
```

State is currently in-memory only -- restart the service between eval runs.

## Architecture

TODO -- sharded workers fed by bounded channels, dedicated priority lane for `fall_warn`.

## Decisions

TODO

## Observability

The service logs a queue snapshot every 5 seconds:

```
queues: total=18432 max_shard=412 falls=0
```

- `total` -- events waiting across all 64 shard queues
- `max_shard` -- depth of the most backed-up shard queue (capacity 4096 each)
- `falls` -- fall warnings waiting in the priority queue

Rising numbers mean ingest is outpacing processing and backpressure is engaged: senders block on full queues, so events are delayed, never dropped. A shard pinned near capacity across several ticks means senders are actively blocking.
