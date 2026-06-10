package main

import "github.com/go-chi/chi/v5"

func setupRoutes(r *chi.Mux) {
	r.Get("/devices/{id}/health", getDeviceHealth)
	r.Get("/rooms/{id}/occupancy", getRoomOccupancy)
	r.Get("/alarms", getAlarms)

	r.Post("/events", parseEvent)
}