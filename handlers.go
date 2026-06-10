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
	last := s.deviceLastHeartbeat[deviceID]
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
	s.mu.RUnlock()

	json.NewEncoder(w).Encode(map[string]float64{
        "occupancy": occ,
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

    json.NewEncoder(w).Encode(getAlarmsSince(since))
}
