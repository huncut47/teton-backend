# Solution (WIP)

## How to run

TODO

## Architecture

TODO — sharded workers fed by bounded channels, dedicated priority lane for `fall_warn`.

## Decisions

TODO

## Observability

The service logs a queue snapshot every 5 seconds:

```
queues: total=18432 max_shard=412 falls=0
```

- `total` — events waiting across all 64 shard queues
- `max_shard` — depth of the most backed-up shard queue (capacity 4096 each)
- `falls` — fall warnings waiting in the priority queue

Rising numbers mean ingest is outpacing processing and backpressure is engaged: senders block on full queues, so events are delayed, never dropped. A shard pinned near capacity across several ticks means senders are actively blocking.
