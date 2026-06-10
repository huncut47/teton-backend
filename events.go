package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type Event struct {
	DeviceID string `json:"device_id"`
	RoomID   string `json:"room_id"`
	Type     string `json:"type"`
	TS       string `json:"ts"`
	InRoom   bool   `json:"in_room"`
}

func parseEvent(w http.ResponseWriter, r *http.Request) {
	var ev Event

	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	ingest(ev)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func ingest(ev Event) {
	t, err := time.Parse(time.RFC3339Nano, ev.TS)
	if err != nil {
		log.Printf("invalid ts: %s", ev.TS)
		return
	}
	ts := float64(t.Unix())

	switch ev.Type {
	case "heartbeat":
		shardFor(ev.DeviceID).events <- ingestedEvent{ev: ev, ts: ts}
	case "presence":
		shardFor(ev.RoomID).events <- ingestedEvent{ev: ev, ts: ts}
	case "fall_warn":
		fallCh <- ingestedEvent{ev: ev, ts: ts}
	}
}
