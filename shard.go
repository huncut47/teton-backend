package main

import (
	"hash/fnv"
	"log"
	"sort"
	"sync"
	"time"
)

const (
	numShards       = 64
	queueSize       = 4096
	heartbeatWindow = 300
)

type ingestedEvent struct {
	ev Event
	ts float64
}

type lastHeartbeat struct {
	Ts  float64
	ISO string
}

type presenceEvent struct {
	InRoom bool
	Ts     float64
}

type shard struct {
	mu                  sync.RWMutex
	events              chan ingestedEvent
	lastHeartbeats      map[string]lastHeartbeat
	deviceHeartbeats    map[string]*RingBuffer
	roomPresence        map[string]presenceEvent
	roomPresenceHistory map[string][]presenceEvent
}

var shards [numShards]*shard

func shardFor(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return shards[h.Sum32()%numShards]
}

func initShards() {
	for i := range shards {
		shards[i] = &shard{
			events:              make(chan ingestedEvent, queueSize),
			lastHeartbeats:      map[string]lastHeartbeat{},
			deviceHeartbeats:    map[string]*RingBuffer{},
			roomPresence:        map[string]presenceEvent{},
			roomPresenceHistory: map[string][]presenceEvent{},
		}
	}
}

func startWorkers() {
	for _, s := range shards {
		go s.run()
	}
	go logQueues()
	go snapshotLoop()
}

func (s *shard) run() {
	for ie := range s.events {
		s.mu.Lock()
		switch ie.ev.Type {
		case "heartbeat":
			s.recordHeartbeat(ie.ev.DeviceID, ie.ts, ie.ev.TS)
		case "presence":
			s.recordPresence(ie.ev.RoomID, ie.ts, ie.ev.InRoom)
		}
		s.mu.Unlock()
	}
}

type slot struct {
	Sec   int
	Count int
}

type RingBuffer struct {
	Slots [heartbeatWindow]slot
}

func (r *RingBuffer) Add(ts float64) {
	sec := int(ts)
	s := &r.Slots[sec%heartbeatWindow]
	if s.Sec != sec {
		s.Sec = sec
		s.Count = 0
	}
	s.Count++
}

func (r *RingBuffer) Count(now float64) int {
	cutoff := int(now) - heartbeatWindow
	total := 0
	for i := range r.Slots {
		if r.Slots[i].Sec > cutoff {
			total += r.Slots[i].Count
		}
	}
	return total
}

func (s *shard) recordHeartbeat(deviceID string, ts float64, iso string) {
	if prev := s.lastHeartbeats[deviceID]; ts >= prev.Ts {
		s.lastHeartbeats[deviceID] = lastHeartbeat{Ts: ts, ISO: iso}
	}

	rb := s.deviceHeartbeats[deviceID]
	if rb == nil {
		rb = &RingBuffer{}
		s.deviceHeartbeats[deviceID] = rb
	}

	if int(ts) <= int(nowSeconds())-heartbeatWindow {
		return
	}
	rb.Add(ts)
}

// TODO: think about how to handle future events
// if there is an event 30m in the future
// we will need to remember that and add
// it in that 5m window later

func searchPresence(events []presenceEvent, ts float64) int {
	return sort.Search(len(events), func(i int) bool {
		return events[i].Ts >= ts
	})
}

func (s *shard) recordPresence(roomID string, ts float64, inRoom bool) {
	if prev, ok := s.roomPresence[roomID]; !ok || ts >= prev.Ts {
		s.roomPresence[roomID] = presenceEvent{InRoom: inRoom, Ts: ts}
	}

	events := s.roomPresenceHistory[roomID]

	if len(events) == 0 || ts >= events[len(events)-1].Ts {
		s.roomPresenceHistory[roomID] = append(events, presenceEvent{InRoom: inRoom, Ts: ts})
		return
	}

	idx := searchPresence(events, ts)
	events = append(events, presenceEvent{})
	copy(events[idx+1:], events[idx:])
	events[idx] = presenceEvent{InRoom: inRoom, Ts: ts}
	s.roomPresenceHistory[roomID] = events
}

func (s *shard) prunePresence(now float64) {
	cutoff := now - 3600

	for roomID, events := range s.roomPresenceHistory {
		idx := searchPresence(events, cutoff)
		if idx > 1 {
			s.roomPresenceHistory[roomID] = append([]presenceEvent{}, events[idx-1:]...)
		}
	}
}

func (s *shard) occupancy(roomID string, now float64, windowSeconds float64) float64 {
	events := s.roomPresenceHistory[roomID]
	if len(events) == 0 {
		return 0
	}

	start := now - windowSeconds
	idx := searchPresence(events, start)

	occupied := 0.0
	currentState := false
	lastTs := start

	if idx > 0 {
		currentState = events[idx-1].InRoom
	}

	for ; idx < len(events) && events[idx].Ts <= now; idx++ {
		if currentState {
			occupied += events[idx].Ts - lastTs
		}
		currentState = events[idx].InRoom
		lastTs = events[idx].Ts
	}

	if currentState {
		occupied += now - lastTs
	}

	return occupied / windowSeconds
}

func logQueues() {
	for range time.Tick(5 * time.Second) {
		total, maxDepth := 0, 0
		for _, s := range shards {
			n := len(s.events)
			total += n
			if n > maxDepth {
				maxDepth = n
			}
		}
		log.Printf("queues: total=%d max_shard=%d | ingested: hb=%d pres=%d motion=%d sleep=%d fall=%d net=%d | alarms=%d rejected=%d",
			total, maxDepth,
			ingestedByType["heartbeat"].Load(),
			ingestedByType["presence"].Load(),
			ingestedByType["motion"].Load(),
			ingestedByType["sleep_state"].Load(),
			ingestedByType["fall_warn"].Load(),
			ingestedByType["net_status"].Load(),
			alarmsEmitted.Load(),
			rejectedCount.Load())
	}
}
