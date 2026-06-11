package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
)

func nowSeconds() float64 {
	return float64(time.Now().UnixMilli()) / 1000.0
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

type healthResponse struct {
	LastHeartbeatTS *string `json:"last_heartbeat_ts"`
	Availability5m  float64 `json:"availability_5m"`
}

type occupancyResponse struct {
	InRoom        bool    `json:"in_room"`
	OccupiedPct   float64 `json:"occupied_pct"`
	WindowSeconds int     `json:"window_seconds"`
}

type alarmsResponse struct {
	Alarms []Alarm `json:"alarms"`
	Since  float64 `json:"since"`
}

func getDeviceHealth(w http.ResponseWriter, r *http.Request) {
	now := nowSeconds()
	deviceID := chi.URLParam(r, "id")

	s := shardFor(deviceID)
	s.mu.RLock()
	var last *string
	if hb, ok := s.lastHeartbeats[deviceID]; ok {
		last = &hb.ISO
	}
	count := 0
	if rb := s.deviceHeartbeats[deviceID]; rb != nil {
		count = rb.Count(now)
	}
	s.mu.RUnlock()

	writeJSON(w, healthResponse{
		LastHeartbeatTS: last,
		Availability5m:  min(float64(count)/float64(heartbeatWindow), 1.0),
	})
}

func getRoomOccupancy(w http.ResponseWriter, r *http.Request) {
	now := nowSeconds()
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
	inRoom := s.roomPresence[roomID].InRoom
	s.mu.RUnlock()

	writeJSON(w, occupancyResponse{
		InRoom:        inRoom,
		OccupiedPct:   min(occ, 1.0),
		WindowSeconds: int(windowSeconds),
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

	writeJSON(w, alarmsResponse{
		Alarms: getAlarmsSince(since),
		Since:  since,
	})
}

func getFeed(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	var cursor int64
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		cursor, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := r.URL.Query().Get("cursor"); v != "" {
		cursor, _ = strconv.ParseInt(v, 10, 64)
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch := make(chan Alarm, 256)

	alarmMu.Lock()
	backlog := make([]Alarm, 0, len(alarms))
	for _, a := range alarms {
		if a.Seq > cursor {
			backlog = append(backlog, a)
		}
	}
	subscribers[ch] = struct{}{}
	alarmMu.Unlock()

	defer func() {
		alarmMu.Lock()
		delete(subscribers, ch)
		alarmMu.Unlock()
	}()

	for _, a := range backlog {
		if err := writeFeedEvent(w, flusher, a); err != nil {
			return
		}
	}

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case a, open := <-ch:
			if !open {
				return
			}
			if err := writeFeedEvent(w, flusher, a); err != nil {
				return
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func writeFeedEvent(w http.ResponseWriter, flusher http.Flusher, a Alarm) error {
	data, err := json.Marshal(a)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\ndata: %s\n\n", a.Seq, data); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}
