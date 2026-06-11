package main

import (
	"log"
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

	log.Println("listening on :8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatal(err)
	}
}
