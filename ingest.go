package main

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

type Event struct {
	DeviceID   string  `json:"device_id"`
	RoomID     string  `json:"room_id"`
	Type       string  `json:"type"`
	TS         string  `json:"ts"`
	InRoom     bool    `json:"in_room"`
	Confidence float64 `json:"confidence"`
}

var (
	rejectedCount  atomic.Int64
	ingestedByType = map[string]*atomic.Int64{
		"heartbeat":   {},
		"presence":    {},
		"motion":      {},
		"sleep_state": {},
		"fall_warn":   {},
		"net_status":  {},
	}
)

var okResponse = []byte(`{"ok":true}` + "\n")

func reject(w http.ResponseWriter, msg string) {
	rejectedCount.Add(1)
	http.Error(w, msg, http.StatusBadRequest)
}

func parseEvent(w http.ResponseWriter, r *http.Request) {
	var ev Event

	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		reject(w, "invalid json")
		return
	}

	if ev.DeviceID == "" || ev.RoomID == "" || ev.TS == "" {
		reject(w, "missing required fields")
		return
	}

	counter, ok := ingestedByType[ev.Type]
	if !ok {
		reject(w, "unknown type")
		return
	}

	t, err := time.Parse(time.RFC3339Nano, ev.TS)
	if err != nil {
		reject(w, "invalid ts")
		return
	}
	ts := float64(t.UnixMilli()) / 1000.0

	if ts > nowSeconds()+3600 {
		reject(w, "ts too far in the future")
		return
	}

	counter.Add(1)
	ingest(ev, ts)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write(okResponse)
}

func ingest(ev Event, ts float64) {
	switch ev.Type {
	case "heartbeat":
		shardFor(ev.DeviceID).events <- ingestedEvent{ev: ev, ts: ts}
	case "presence":
		shardFor(ev.RoomID).events <- ingestedEvent{ev: ev, ts: ts}
	case "fall_warn":
		recordAlarm(ev, ts)
	}
}
