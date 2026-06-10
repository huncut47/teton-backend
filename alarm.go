package main

import "sync"

type Alarm struct {
	DeviceID string  `json:"device_id"`
	RoomID   string  `json:"room_id"`
	TS       string  `json:"ts"`
	ts       float64 `json:"-"`
}

type AlarmKey struct {
	deviceID string
	Ts       float64
}

var (
	alarmMu    sync.RWMutex
	alarms     []Alarm
	seenAlarms = map[AlarmKey]struct{}{}
	fallCh     = make(chan ingestedEvent, 1024)
)

func runAlarmWorker() {
	for ie := range fallCh {
		alarmMu.Lock()
		recordAlarm(ie.ev, ie.ts)
		alarmMu.Unlock()
	}
}

func recordAlarm(ev Event, ts float64) {
	key := AlarmKey{
		deviceID: ev.DeviceID,
		Ts:       ts,
	}

	if _, exists := seenAlarms[key]; exists {
		return
	}

	seenAlarms[key] = struct{}{}
	alarms = append(alarms, Alarm{
		DeviceID: ev.DeviceID,
		RoomID:   ev.RoomID,
		TS:       ev.TS,
		ts:       ts,
	})
}

func getAlarmsSince(ts float64) []Alarm {
	alarmMu.RLock()
	defer alarmMu.RUnlock()

	result := []Alarm{}

	for _, alarm := range alarms {
		if alarm.ts > ts {
			result = append(result, alarm)
		}
	}

	return result
}
