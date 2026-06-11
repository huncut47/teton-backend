# Solution (WIP)

## How to run

Requires Go 1.25+ (https://go.dev/dl/) and a C compiler (gcc or clang) -- the SQLite driver uses cgo.

```bash
cd my_solution
make run      # or: go run .
make fresh    # delete persisted state, then run
make clean    # delete persisted state (data.db*)
```

The first run downloads the Go module dependencies (chi router) automatically. The service listens on `:8080`:

```
POST /events                              event ingestion
GET  /devices/{device_id}/health
GET  /rooms/{room_id}/occupancy?window=1m|5m|1h
GET  /alarms?since=<unix_ts>
GET  /feed                                live alarm feed (SSE)
```

The feed delivers alarms in ingestion order with a monotonic `seq` as the SSE event id. To resume after a disconnect (including a service restart), reconnect with the `Last-Event-ID` header (standard SSE client behavior) or `?cursor=<seq>` -- every alarm after that seq is replayed before the live stream continues. Cursors stay valid across restarts because `seq` is the alarm's rowid in SQLite.

The service persists state to `./data.db` (SQLite) and recovers it on startup. For a fresh run, delete `data.db*` before starting (`make fresh` does both).

## Architecture

Events arrive as HTTP POSTs and split into two paths:

- **Routine events** (heartbeat, presence) are hashed by device or room onto one of 64 bounded queues (Go channels, 4096 slots each), each drained by its own worker goroutine that owns that shard's in-memory state. The bounded queues are the backpressure mechanism: a 10x spike fills them and delays senders instead of dropping events. A given device or room always hashes to the same shard, so no two workers ever touch the same state.
- **Fall warnings** skip the queues entirely: the request handler dedups them and commits them to SQLite before acking, so an acknowledged alarm survives any crash. New alarms are pushed to all SSE feed subscribers within milliseconds.

Reads (`/devices/.../health`, `/rooms/.../occupancy`, `/alarms`) are served from the in-memory state under per-shard read locks -- SQLite is never on the read path.

Aggregations are computed against event timestamps, not arrival time: presence history is kept sorted by `ts` (late replays insert into place and the occupancy windows recompute correctly), and heartbeats land in a per-second ring buffer covering the 5m window.

Persistence is two-tier: alarms are durable at ingestion (see above); everything else is snapshotted to SQLite every 5 seconds, which bounds the loss window on a hard kill to data that self-heals (see Drawbacks). On startup the service loads the snapshot and recent alarms and resumes. The same 5s tick prunes anything older than the largest query window, so memory stays bounded by fleet size, not uptime.

File layout: `main.go` (startup + routes), `ingest.go` (write path + validation), `shard.go` (sharded state + aggregation), `alarm.go` (dedup + persistence + feed broadcast), `handlers.go` (read endpoints + SSE feed), `db.go` (SQLite, snapshot/restore).

## Decisions

TODO

- **SSE for the alarm feed, over WebSocket and long-poll.** The feed is one-directional (nothing to receive from consumers), so WebSocket would add a dependency (no stdlib support) and a hand-rolled resume protocol for no benefit; long-polling makes sub-1s latency awkward through reconnect churn. SSE is push over plain HTTP, implemented with just `http.Flusher` and a channel per subscriber -- and resume is built into the protocol: standard SSE clients automatically resend the last event id as `Last-Event-ID` on reconnect, so the durable `seq` cursor slots straight in and "resume without missing alarms" is the protocol's default behavior rather than custom machinery.

- **Alarm dedup state is retained for 2h, not the 1h lateness limit.** A duplicate of an alarm with timestamp T can legally arrive until wall-clock T+1h (late-replay allowance), so pruning dedup keys at exactly 1h would delete a key at the last moment its duplicate could still arrive -- and the boundary is fuzzy: device clocks drift (±30s, occasionally more), pruning runs on a 5s tick, and the cutoff compares device timestamps against server wall clock. Doubling to 2h makes the boundary analysis unnecessary, at the cost of a few thousand small structs (alarms are rare). If memory mattered, 1h + max skew + tick interval would be the precise bound. The DB primary key catches anything past the in-memory window regardless.

## Drawbacks

- **Up to 5 seconds of heartbeat/presence data can be lost on a hard kill.** This is a deliberate two-tier durability choice: alarms are committed to SQLite synchronously before the event is acked because they are the invariant that must never be lost, while aggregate state is only snapshotted every 5s because its loss window self-heals -- missing heartbeats age out of the 5m availability window within moments, and room presence corrects itself on the next transition.

- **Future-skewed heartbeats within the 1h limit still land in the ring buffer and can nudge availability.** Events more than 1h in the future are rejected, but a device clock running e.g. 30s ahead produces heartbeats counted in buckets the 5m window hasn't reached yet, slightly inflating availability (capped at 1.0). Bounded by the ±30s normal drift, so the effect is small.

- **Snapshot size scales with fleet size.** The 5s snapshot gob-encodes every device's full ring buffer (2 × 300 ints), so at 5,000 devices each snapshot is a ~25MB blob rewritten every 5 seconds. Fine locally; at a few hundred thousand devices this would need incremental or per-shard snapshots instead of one full blob.

## Observability

The service logs a queue snapshot every 5 seconds:

```
queues: total=18432 max_shard=412 | ingested: hb=51022 pres=4180 motion=15321 sleep=2011 fall=483 net=3990 | alarms=470 rejected=12
```

- `total` -- events waiting across all 64 shard queues
- `max_shard` -- depth of the most backed-up shard queue (capacity 4096 each)
- `ingested: ...` -- cumulative accepted events by type since startup
- `alarms` -- distinct fall alarms emitted (after dedup)
- `rejected` -- events refused with 400 (malformed, unknown type, missing fields, or ts more than 1h in the future)

Rising numbers mean ingest is outpacing processing and backpressure is engaged: senders block on full queues, so events are delayed, never dropped. A shard pinned near capacity across several ticks means senders are actively blocking.
