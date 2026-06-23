package queue_test

import (
	"testing"

	"github.com/ultrakapy/async-job-pipeline/internal/domain"
	"github.com/ultrakapy/async-job-pipeline/internal/queue"
)

func TestEnqueueDequeue(t *testing.T) {
	q := queue.New(10)
	q.Enqueue(&domain.Job{ID: "j1"})
	got := <-q.Dequeue()
	if got.ID != "j1" {
		t.Fatalf("got %s", got.ID)
	}
}

func TestDepth(t *testing.T) {
	q := queue.New(10)
	q.Enqueue(&domain.Job{ID: "a"})
	q.Enqueue(&domain.Job{ID: "b"})
	if q.Depth() != 2 {
		t.Fatalf("expected 2, got %d", q.Depth())
	}
}

func TestDrain(t *testing.T) {
	q := queue.New(10)
	q.Enqueue(&domain.Job{ID: "a"})
	q.Enqueue(&domain.Job{ID: "b"})
	q.Enqueue(&domain.Job{ID: "c"})
	drained := q.Drain()
	if len(drained) != 3 {
		t.Fatalf("expected 3, got %d", len(drained))
	}
	if q.Depth() != 0 {
		t.Fatal("expected depth 0 after drain")
	}
}
