package main

import "sync"

type Alarm struct {
	ts       float64
	roomID   string
	deviceID string
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
		recordAlarm(ie.ev.DeviceID, ie.ev.RoomID, ie.ts)
		alarmMu.Unlock()
	}
}

func recordAlarm(deviceID string, roomID string, ts float64) {
	alarm := Alarm{
		ts:       ts,
		roomID:   roomID,
		deviceID: deviceID,
	}

	key := AlarmKey{
		deviceID: alarm.deviceID,
		Ts:       alarm.ts,
	}

	if _, exists := seenAlarms[key]; exists {
		return
	}

	seenAlarms[key] = struct{}{}
	alarms = append(alarms, alarm)
}

func getAlarmsSince(ts float64) []Alarm {
	alarmMu.RLock()
	defer alarmMu.RUnlock()

	var result []Alarm

	for _, alarm := range alarms {
		if alarm.ts > ts {
			result = append(result, alarm)
		}
	}

	return result
}
