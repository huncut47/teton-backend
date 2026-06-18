# Submission, Real-time Streaming Backend

**Your name:** Rafael Rohal
**Email:** me@rohalrafael.com
**Link to your fork or solution:** github.com/huncut47/teton-backend

---

## Stack and storage

(What you chose, and why, 2–3 sentences each for compute, storage, transport.)

- Language: Go -- goroutines/channels map directly onto the problem (bounded channels = backpressure, worker per shard); earlier Python prototype hit a GIL-bound ~2.5k req/s HTTP ceiling, making 50k/s architectural
- Storage: SQLite -- write load is tiny (~1 alarm/s + one snapshot/5s), no DB server earns its place; WAL mode survives SIGKILL; tradeoff: needs cgo/C compiler
- Two-tier durability: alarms committed synchronously before the 202 (acked = durable), aggregates snapshotted every 5s because their loss window self-heals
- Reads never touch SQLite -- served from in-memory shard state under read locks
- Transport: HTTP POST per event because that is all the provided generator speaks; with control of both ends would use gRPC client-streaming (flow control = transport-level backpressure)

## Ordering and late events

(How you handle per-device ordering and out-of-order arrivals from offline devices.)

- Aggregate by `ts`, never arrival time; presence history kept sorted by `ts`, late replays binary-search-insert into place, occupancy windows recompute correctly
- "Latest presence wins" enforced by ts comparison -- replayed old events cannot overwrite newer state
- Dedup: exact (device_id, ts) key at millisecond precision; found-and-fixed story: second-level truncation collided distinct same-second falls under burst (21/470 lost)
- Dedup state retained 2h (1h lateness limit + skew/tick margin doubled away); DB primary key as backstop
- Acceptance rules: 400 on malformed/missing fields, reject ts >1h future, accept 1h past

## Backpressure

(What happens during a 10x burst. What you delay, what you prioritize.)

- 64 shards, each a bounded channel (4096) + worker goroutine owning that shard's state; same device/room always hashes to same shard
- Full queue blocks the sender: events delayed, never silently dropped; queue depth visible in 5s log line
- Prioritization is structural: fall_warn never enters the queues -- synchronous fast path to durable storage + feed broadcast
- Honest ceiling: generator times out after 5s, so blocking-based backpressure has a hard limit (never reached in testing)
- Slow feed consumers severed instead of blocking ingest; they resume losslessly via cursor

## Restart correctness

(How state survives a hard kill.)

- Alarms: in SQLite before the ack, so anything acked survives kill -9 by construction
- Aggregates: gob snapshot of all shard state every 5s; on boot load snapshot + recent alarms, resume; ≤5s loss window, self-healing
- Feed resume: seq = SQLite rowid = SSE event id; reconnect with Last-Event-ID replays everything after the cursor, backlog + subscribe atomic so nothing falls between
- Verified by automated kill_restart_test.sh: kill -9 mid-adversarial-run at 5000 devices, all 6680 acked alarms survived, consumer resumed at exactly cursor+1, no gaps/duplicates

## How to run it locally

```bash
# Go 1.25+ and a C compiler (cgo) required
cd my_solution
make fresh          # deletes data.db*, builds and runs; listens on :8080
# then from the challenge repo root: make smoke / burst / offline / adversarial
```

## Reported metrics

- Sustained ingest rate: 1.04M events / 4min adversarial run at 5000 devices, zero failures (generator itself is the bottleneck, single Python process ~4.3k req/s)
- Alarm feed latency p50 / p95: TODO -- measure (broadcast is synchronous in-process push at ingest, expected well under 1s; needs actual numbers)
- Behavior under hard kill + restart: 6680 alarms before kill -> 6697 after restart; feed resumed from id 6681, strictly increasing, no duplicates
- Aggregation correctness on replayed events: offline scenario 506k events, dedup exact 6509/6509 including buffered falls; adversarial (skew + offline + burst) dedup exact 13661/13661

## With another week

I would replace the full-state snapshot with an incremental, per-shard one. Today, every 5 seconds the service serializes all shard state into a single ~25MB blob and rewrites it -- fine for 5,000 devices, but it scales linearly with fleet size and would become wasteful at the few-hundred-thousand devices the brief points toward, rewriting mostly-unchanged data on every tick. I would snapshot each shard independently and only when it has changed since its last write, so snapshot cost tracks the churn rate rather than the total device count. That also shrinks the recovery read on boot and removes the periodic write spike that currently competes with ingest.
