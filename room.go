package main

import (
	"sort"
)

type presenceEvent struct {
	inRoom bool
	ts     float64
}

func (s *shard) recordPresence(roomID string, ts float64, inRoom bool) {
	if prev, ok := s.roomPresence[roomID]; !ok || ts >= prev.ts {
    	s.roomPresence[roomID] = presenceEvent{
			inRoom: inRoom,
			ts: ts,
		}
	}

	events := s.roomPresenceHistory[roomID]

	if len(events) == 0 || ts >= events[len(events)-1].ts {
        s.roomPresenceHistory[roomID] = append(events, presenceEvent{
            inRoom: inRoom,
            ts:     ts,
        })
        return
    }

	idx := sort.Search(len(events), func(i int) bool {
        return events[i].ts >= ts
    })

	events = append(events, presenceEvent{})
    copy(events[idx+1:], events[idx:])
    events[idx] = presenceEvent{
        inRoom: inRoom,
        ts:     ts,
    }

	s.roomPresenceHistory[roomID] = events
}

func (s *shard) occupancy(roomID string, now float64, windowSeconds float64) float64 {
    events := s.roomPresenceHistory[roomID]
    if len(events) == 0 {
        return 0
    }

    start := now - windowSeconds

    idx := sort.Search(len(events), func(i int) bool {
        return events[i].ts >= start
    })

    occupied := 0.0

    currentState := false
    lastTs := start

    if idx > 0 {
        currentState = events[idx-1].inRoom
    }

    for ; idx < len(events) && events[idx].ts <= now; idx++ {
        ev := events[idx]

        if currentState {
            occupied += ev.ts - lastTs
        }

        currentState = ev.inRoom
        lastTs = ev.ts
    }

    if currentState {
        occupied += now - lastTs
    }

    return occupied / windowSeconds
}
