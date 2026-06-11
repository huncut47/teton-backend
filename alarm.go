package main

import (
	"log"
	"sync"
	"sync/atomic"
)

type Alarm struct {
	Seq        int64   `json:"seq"`
	EventID    string  `json:"event_id"`
	DeviceID   string  `json:"device_id"`
	RoomID     string  `json:"room_id"`
	TS         string  `json:"ts"`
	Confidence float64 `json:"confidence"`
	ts         float64 `json:"-"`
}

type AlarmKey struct {
	deviceID string
	Ts       float64
}

var (
	alarmsEmitted atomic.Int64

	alarmMu     sync.RWMutex
	alarms      []Alarm
	seenAlarms  = map[AlarmKey]struct{}{}
	subscribers = map[chan Alarm]struct{}{}
)

func eventID(deviceID, ts string) string {
	return deviceID + ":" + ts
}

func recordAlarm(ev Event, ts float64) {
	alarmMu.Lock()
	defer alarmMu.Unlock()

	key := AlarmKey{
		deviceID: ev.DeviceID,
		Ts:       ts,
	}

	if _, exists := seenAlarms[key]; exists {
		return
	}

	res, err := db.Exec(`INSERT OR IGNORE INTO alarms (device_id, room_id, ts, tsf, confidence) VALUES (?, ?, ?, ?, ?)`,
		ev.DeviceID, ev.RoomID, ev.TS, ts, ev.Confidence)
	if err != nil {
		log.Printf("alarm insert failed: %v", err)
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return
	}

	seq, err := res.LastInsertId()
	if err != nil {
		log.Printf("alarm rowid lookup failed: %v", err)
		return
	}

	alarm := Alarm{
		Seq:        seq,
		EventID:    eventID(ev.DeviceID, ev.TS),
		DeviceID:   ev.DeviceID,
		RoomID:     ev.RoomID,
		TS:         ev.TS,
		Confidence: ev.Confidence,
		ts:         ts,
	}

	seenAlarms[key] = struct{}{}
	alarms = append(alarms, alarm)
	alarmsEmitted.Add(1)

	for ch := range subscribers {
		select {
		case ch <- alarm:
		default:
			// consumer too slow to keep up: sever it; it reconnects
			// with its cursor and resumes without loss
			close(ch)
			delete(subscribers, ch)
		}
	}
}

func pruneAlarms(now float64) {
	alarmMu.Lock()
	defer alarmMu.Unlock()

	cutoff := now - 7200

	kept := make([]Alarm, 0, len(alarms))
	for _, a := range alarms {
		if a.ts >= cutoff {
			kept = append(kept, a)
		}
	}
	alarms = kept

	for key := range seenAlarms {
		if key.Ts < cutoff {
			delete(seenAlarms, key)
		}
	}
}

func getAlarmsSince(ts float64) []Alarm {
	alarmMu.RLock()
	defer alarmMu.RUnlock()

	result := make([]Alarm, 0, len(alarms))

	for _, alarm := range alarms {
		if alarm.ts > ts {
			result = append(result, alarm)
		}
	}

	return result
}
