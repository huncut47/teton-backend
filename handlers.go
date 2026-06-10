package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
)

func getDeviceHealth(w http.ResponseWriter, r *http.Request) {
	now := float64(time.Now().Unix())
	deviceID := chi.URLParam(r, "id")

	s := shardFor(deviceID)
	s.mu.RLock()
	var last interface{}
	if iso := s.deviceLastHeartbeatTS[deviceID]; iso != "" {
		last = iso
	}
	count := 0
	if rb := s.deviceHeartbeats[deviceID]; rb != nil {
		count = rb.Count(now)
	}
	s.mu.RUnlock()

	availability := float64(count) / float64(heartbeatWindow)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
    	"last_heartbeat_ts": last,
    	"availability_5m":   min(availability, 1.0),
	})
}

func getRoomOccupancy(w http.ResponseWriter, r *http.Request) {
	now := float64(time.Now().Unix())
	roomID := chi.URLParam(r, "id")

	windowSeconds := 300.0

	switch r.URL.Query().Get("window") {
	case "1m":
		windowSeconds = 60
	case "5m":
		windowSeconds = 300
	case "1h":
		windowSeconds = 3600
	}

	s := shardFor(roomID)
	s.mu.RLock()
	occ := s.occupancy(roomID, now, windowSeconds)
	inRoom := s.roomPresence[roomID].inRoom
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
        "in_room":        inRoom,
        "occupied_pct":   min(occ, 1.0),
        "window_seconds": int(windowSeconds),
    })
}

func getAlarms(w http.ResponseWriter, r *http.Request) {
    sinceStr := r.URL.Query().Get("since")

    if sinceStr == "" {
        sinceStr = "0"
    }

    since, err := strconv.ParseFloat(sinceStr, 64)
    if err != nil {
        http.Error(w, "invalid since", http.StatusBadRequest)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "alarms": getAlarmsSince(since),
        "since":  since,
    })
}
