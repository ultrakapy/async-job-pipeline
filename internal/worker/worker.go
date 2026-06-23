package worker

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/ultrakapy/async-job-pipeline/internal/domain"
	"github.com/ultrakapy/async-job-pipeline/internal/queue"
	"github.com/ultrakapy/async-job-pipeline/internal/store"
)

type Handler func(ctx context.Context, payload map[string]any) (map[string]any, error)

type Pool struct {
	workers int
	queue   queue.Queue
	store   store.Store
	dlStore store.DeadLetterStore
	handler Handler
	done    chan struct{}
}

func NewPool(size int, q queue.Queue, s store.Store, dl store.DeadLetterStore, h Handler) *Pool {
	return &Pool{workers: size, queue: q, store: s, dlStore: dl, handler: h, done: make(chan struct{})}
}

func (p *Pool) Start() {
	for i := 0; i < p.workers; i++ {
		go p.run(i)
	}
}

func (p *Pool) Stop() {
	close(p.done)
}

func (p *Pool) run(id int) {
	for {
		select {
		case <-p.done:
			return
		case job := <-p.queue.Dequeue():
			p.process(job)
		}
	}
}

func (p *Pool) process(job *domain.Job) {
	// Re-fetch from store to catch cancellations that happened after enqueue
	current, err := p.store.Get(job.ID)
	if err != nil || current.Status == domain.StatusCancelled {
		slog.Info("skipping cancelled/missing job", "job_id", job.ID)
		return
	}

	current.Status = domain.StatusRunning
	current.Attempts++
	if err := p.store.Update(current); err != nil {
		slog.Error("failed to set running", "job_id", current.ID, "err", err)
		return
	}

	timeout := time.Duration(current.TimeoutSecs) * time.Second
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, execErr := p.handler(ctx, current.Payload)

	if execErr != nil {
		current.LastError = execErr.Error()
		if current.Attempts >= current.MaxRetries {
			current.Status = domain.StatusDeadLettered
			slog.Warn("job dead-lettered", "job_id", current.ID, "attempts", current.Attempts, "err", execErr)
			p.store.Update(current)
			if err := p.dlStore.Add(current); err != nil {
				slog.Error("failed to persist to dead-letter store", "job_id", current.ID, "err", err)
			}
		} else {
			current.Status = domain.StatusFailed
			backoff := time.Duration(math.Pow(2, float64(current.Attempts))) * time.Second
			slog.Info("job failed, retrying", "job_id", current.ID, "attempt", current.Attempts, "backoff", backoff)
			p.store.Update(current)
			go func(jobID string) {
				time.Sleep(backoff)
				latest, err := p.store.Get(jobID)
				if err != nil || latest.Status == domain.StatusCancelled {
					return
				}
				latest.Status = domain.StatusQueued
				p.store.Update(latest)
				p.queue.Enqueue(latest)
			}(current.ID)
			return
		}
	} else {
		current.Status = domain.StatusSucceeded
		current.Result = result
		slog.Info("job succeeded", "job_id", current.ID, "attempts", current.Attempts)
	}

	p.store.Update(current)
}
