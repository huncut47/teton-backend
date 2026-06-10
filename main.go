package main

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func main() {
	startWorkers()
	r := chi.NewRouter()
	setupRoutes(r)
	http.ListenAndServe(":8080", r)
}