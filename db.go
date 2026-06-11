package main

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

func initDB() {
	var err error
	db, err = sql.Open("sqlite3", "file:data.db?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS alarms (
		device_id  TEXT NOT NULL,
		room_id    TEXT NOT NULL,
		ts         TEXT NOT NULL,
		tsf        REAL NOT NULL,
		confidence REAL NOT NULL DEFAULT 0,
		PRIMARY KEY (device_id, tsf)
	)`); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS snapshot (
		id   INTEGER PRIMARY KEY CHECK (id = 1),
		data BLOB NOT NULL
	)`); err != nil {
		log.Fatal(err)
	}
}

type shardSnapshot struct {
	LastHeartbeats      map[string]lastHeartbeat
	DeviceHeartbeats    map[string]*RingBuffer
	RoomPresence        map[string]presenceEvent
	RoomPresenceHistory map[string][]presenceEvent
}

func writeSnapshot() error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	for _, s := range shards {
		s.mu.RLock()
		err := enc.Encode(shardSnapshot{
			LastHeartbeats:      s.lastHeartbeats,
			DeviceHeartbeats:    s.deviceHeartbeats,
			RoomPresence:        s.roomPresence,
			RoomPresenceHistory: s.roomPresenceHistory,
		})
		s.mu.RUnlock()
		if err != nil {
			return err
		}
	}

	_, err := db.Exec(`INSERT OR REPLACE INTO snapshot (id, data) VALUES (1, ?)`, buf.Bytes())
	return err
}

func snapshotLoop() {
	for range time.Tick(5 * time.Second) {
		now := nowSeconds()
		for _, s := range shards {
			s.mu.Lock()
			s.prunePresence(now)
			s.mu.Unlock()
		}
		pruneAlarms(now)

		if err := writeSnapshot(); err != nil {
			log.Printf("snapshot failed: %v", err)
		}
	}
}

func restoreState() {
	var data []byte
	err := db.QueryRow(`SELECT data FROM snapshot WHERE id = 1`).Scan(&data)
	if err == nil {
		dec := gob.NewDecoder(bytes.NewReader(data))
		for _, s := range shards {
			var snap shardSnapshot
			if err := dec.Decode(&snap); err != nil {
				log.Printf("snapshot decode failed: %v", err)
				break
			}
			// gob decodes empty maps as nil; keep the initialized maps in that case
			if snap.LastHeartbeats != nil {
				s.lastHeartbeats = snap.LastHeartbeats
			}
			if snap.DeviceHeartbeats != nil {
				s.deviceHeartbeats = snap.DeviceHeartbeats
			}
			if snap.RoomPresence != nil {
				s.roomPresence = snap.RoomPresence
			}
			if snap.RoomPresenceHistory != nil {
				s.roomPresenceHistory = snap.RoomPresenceHistory
			}
		}
	} else if err != sql.ErrNoRows {
		log.Printf("snapshot load failed: %v", err)
	}

	cutoff := nowSeconds() - 7200
	rows, err := db.Query(`SELECT rowid, device_id, room_id, ts, tsf, confidence FROM alarms WHERE tsf >= ? ORDER BY rowid`, cutoff)
	if err != nil {
		log.Printf("alarms load failed: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var a Alarm
		if err := rows.Scan(&a.Seq, &a.DeviceID, &a.RoomID, &a.TS, &a.ts, &a.Confidence); err != nil {
			log.Printf("alarm scan failed: %v", err)
			return
		}
		a.EventID = eventID(a.DeviceID, a.TS)
		seenAlarms[AlarmKey{deviceID: a.DeviceID, Ts: a.ts}] = struct{}{}
		alarms = append(alarms, a)
	}
}
