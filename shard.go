package main

import (
	"hash/fnv"
	"log"
	"sync"
	"time"
)

const (
	numShards = 64
	queueSize = 4096
)

type ingestedEvent struct {
	ev Event
	ts float64
}

type shard struct {
	mu                    sync.RWMutex
	events                chan ingestedEvent
	deviceLastHeartbeat   map[string]float64
	deviceLastHeartbeatTS map[string]string
	deviceHeartbeats      map[string]*RingBuffer
	roomPresence          map[string]presenceEvent
	roomPresenceHistory   map[string][]presenceEvent
}

var shards [numShards]*shard

func shardFor(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return shards[h.Sum32()%numShards]
}

func startWorkers() {
	for i := range shards {
		s := &shard{
			events:                make(chan ingestedEvent, queueSize),
			deviceLastHeartbeat:   map[string]float64{},
			deviceLastHeartbeatTS: map[string]string{},
			deviceHeartbeats:      map[string]*RingBuffer{},
			roomPresence:          map[string]presenceEvent{},
			roomPresenceHistory:   map[string][]presenceEvent{},
		}
		shards[i] = s
		go s.run()
	}
	go runAlarmWorker()
	go logQueues()
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
		log.Printf("queues: total=%d max_shard=%d falls=%d", total, maxDepth, len(fallCh))
	}
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
