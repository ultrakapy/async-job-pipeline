package worker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ultrakapy/async-job-pipeline/internal/domain"
	"github.com/ultrakapy/async-job-pipeline/internal/queue"
	"github.com/ultrakapy/async-job-pipeline/internal/store"
	"github.com/ultrakapy/async-job-pipeline/internal/worker"
)

func waitStatus(t *testing.T, s store.Store, id string, want domain.Status, d time.Duration) *domain.Job {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if j, err := s.Get(id); err == nil && j.Status == want {
			return j
		}
		time.Sleep(50 * time.Millisecond)
	}
	j, _ := s.Get(id)
	t.Fatalf("timeout: want %s, got %s", want, j.Status)
	return nil
}

func enqueue(t *testing.T, s store.Store, q queue.Queue, id string, maxRetries, timeoutSecs int) {
	t.Helper()
	job := &domain.Job{ID: id, Status: domain.StatusQueued,
		MaxRetries: maxRetries, TimeoutSecs: timeoutSecs,
		CreatedAt: time.Now(), UpdatedAt: time.Now()}
	s.Save(job)
	q.Enqueue(job)
}

func TestSuccess(t *testing.T) {
	st, q := store.New(), queue.New(10)
	h := func(ctx context.Context, p map[string]any) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	}
	pool := worker.NewPool(1, q, st, store.NewDeadLetterStore(), h)
	pool.Start()
	defer pool.Stop()

	enqueue(t, st, q, "success", 3, 5)
	got := waitStatus(t, st, "success", domain.StatusSucceeded, 2*time.Second)
	if got.Result == nil {
		t.Fatal("expected result")
	}
}

func TestRetryThenSucceed(t *testing.T) {
	st, q := store.New(), queue.New(10)
	calls := 0
	h := func(ctx context.Context, p map[string]any) (map[string]any, error) {
		calls++
		if calls < 2 {
			return nil, errors.New("transient")
		}
		return map[string]any{"ok": true}, nil
	}
	pool := worker.NewPool(1, q, st, store.NewDeadLetterStore(), h)
	pool.Start()
	defer pool.Stop()

	enqueue(t, st, q, "retry", 3, 5)
	got := waitStatus(t, st, "retry", domain.StatusSucceeded, 6*time.Second)
	if got.Attempts < 2 {
		t.Fatalf("expected >=2 attempts, got %d", got.Attempts)
	}
}

func TestDeadLetter(t *testing.T) {
	st, q := store.New(), queue.New(10)
	h := func(ctx context.Context, p map[string]any) (map[string]any, error) {
		return nil, errors.New("always fails")
	}
	pool := worker.NewPool(1, q, st, store.NewDeadLetterStore(), h)
	pool.Start()
	defer pool.Stop()

	enqueue(t, st, q, "dl", 2, 5)
	// attempt 1 fail + 2s backoff + attempt 2 fail = dead-letter
	got := waitStatus(t, st, "dl", domain.StatusDeadLettered, 10*time.Second)
	if got.LastError == "" {
		t.Fatal("expected last_error set")
	}
}

func TestTimeout(t *testing.T) {
	st, q := store.New(), queue.New(10)
	h := func(ctx context.Context, p map[string]any) (map[string]any, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
			return nil, nil
		}
	}
	pool := worker.NewPool(1, q, st, store.NewDeadLetterStore(), h)
	pool.Start()
	defer pool.Stop()

	enqueue(t, st, q, "timeout", 2, 1) // 1s timeout, 2 max attempts
	waitStatus(t, st, "timeout", domain.StatusDeadLettered, 10*time.Second)
}
