package main

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func main() {
	initDB()
	initShards()
	restoreState()
	startWorkers()

	r := chi.NewRouter()
	r.Post("/events", parseEvent)
	r.Get("/devices/{id}/health", getDeviceHealth)
	r.Get("/rooms/{id}/occupancy", getRoomOccupancy)
	r.Get("/alarms", getAlarms)
	r.Get("/feed", getFeed)

	http.ListenAndServe(":8080", r)
}
