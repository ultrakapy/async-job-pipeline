package api

import "net/http"

func NewRouter(h *Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/jobs", h.SubmitJob)
	mux.HandleFunc("GET /api/v1/jobs", h.ListJobs)
	mux.HandleFunc("GET /api/v1/jobs/{id}", h.GetJob)
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", h.CancelJob)
	mux.HandleFunc("GET /api/v1/queue/depth", h.QueueDepth)
	mux.HandleFunc("POST /api/v1/queue/drain", h.DrainQueue)
	mux.HandleFunc("GET /healthz", h.Health)
	return mux
}
