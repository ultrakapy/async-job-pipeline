package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ultrakapy/async-job-pipeline/internal/domain"
	"github.com/ultrakapy/async-job-pipeline/internal/queue"
	"github.com/ultrakapy/async-job-pipeline/internal/store"
)

type Handler struct {
	store store.Store
	queue queue.Queue
}

func NewHandler(s store.Store, q queue.Queue) *Handler {
	return &Handler{store: s, queue: q}
}

func (h *Handler) SubmitJob(w http.ResponseWriter, r *http.Request) {
	var req domain.SubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}
	if req.MaxRetries <= 0 {
		req.MaxRetries = 3
	}
	if req.TimeoutSecs <= 0 {
		req.TimeoutSecs = 30
	}

	now := time.Now().UTC()
	job := &domain.Job{
		ID:          newID(),
		Payload:     req.Payload,
		Priority:    req.Priority,
		MaxRetries:  req.MaxRetries,
		TimeoutSecs: req.TimeoutSecs,
		Status:      domain.StatusQueued,
		CreatedAt:   now,
		UpdatedAt:   now,
		RunAfter:    req.RunAfter,
	}

	if err := h.store.Save(job); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if req.RunAfter != nil && req.RunAfter.After(time.Now()) {
		go func() {
			time.Sleep(time.Until(*req.RunAfter))
			h.queue.Enqueue(job)
		}()
	} else {
		h.queue.Enqueue(job)
	}

	writeJSON(w, http.StatusAccepted, domain.SubmitResponse{JobID: job.ID})
}

func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := h.store.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.store.List()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (h *Handler) CancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := h.store.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	if job.Status != domain.StatusQueued && job.Status != domain.StatusFailed {
		writeError(w, http.StatusConflict, fmt.Sprintf("cannot cancel job with status %s", job.Status))
		return
	}
	job.Status = domain.StatusCancelled
	h.store.Update(job)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) QueueDepth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]int64{"depth": h.queue.Depth()})
}

func (h *Handler) DrainQueue(w http.ResponseWriter, r *http.Request) {
	drained := h.queue.Drain()
	for _, job := range drained {
		job.Status = domain.StatusCancelled
		h.store.Update(job)
	}
	writeJSON(w, http.StatusOK, map[string]int{"cancelled": len(drained)})
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func newID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
