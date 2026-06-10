package main

import (
	"time"
)

const heartbeatWindow = 300

type RingBuffer struct {
    buckets [heartbeatWindow]int
    seconds [heartbeatWindow]int
}

func (r *RingBuffer) Add(ts float64) {
    sec := int(ts)
    slot := sec % heartbeatWindow
    if r.seconds[slot] != sec {
        r.buckets[slot] = 0
        r.seconds[slot] = sec
    }
    r.buckets[slot]++
}

func (r *RingBuffer) Count(now float64) int {
    cutoff := int(now) - heartbeatWindow
    total := 0
    for i := 0; i < heartbeatWindow; i++ {
        if r.seconds[i] > cutoff {
            total += r.buckets[i]
        }
    }
    return total
}

func (s *shard) recordHeartbeat(deviceID string, ts float64) {
	now := float64(time.Now().Unix())
    s.deviceLastHeartbeat[deviceID] = max(ts, s.deviceLastHeartbeat[deviceID])
    if s.deviceHeartbeats[deviceID] == nil {
        s.deviceHeartbeats[deviceID] = &RingBuffer{}
    }
	if int(ts) <= int(now) - 300 {
		return
	}
    s.deviceHeartbeats[deviceID].Add(ts)
}

// TODO: persist the buffer in case of outage
// and the last heartbeat

// TODO: think about how to handle future events
// if there is an event 30m in the future
// we will need to remember that and add
// it in that 5m window later
