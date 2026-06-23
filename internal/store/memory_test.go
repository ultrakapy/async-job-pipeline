package store_test

import (
	"testing"
	"time"

	"github.com/ultrakapy/async-job-pipeline/internal/domain"
	"github.com/ultrakapy/async-job-pipeline/internal/store"
)

func newJob(id string) *domain.Job {
	return &domain.Job{ID: id, Status: domain.StatusQueued, MaxRetries: 3,
		CreatedAt: time.Now(), UpdatedAt: time.Now()}
}

func TestSaveAndGet(t *testing.T) {
	s := store.New()
	if err := s.Save(newJob("j1")); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get("j1")
	if err != nil || got.ID != "j1" {
		t.Fatalf("unexpected: %v %v", got, err)
	}
}

func TestGetNotFound(t *testing.T) {
	s := store.New()
	if _, err := s.Get("missing"); err == nil {
		t.Fatal("expected error")
	}
}

func TestUpdate(t *testing.T) {
	s := store.New()
	job := newJob("j2")
	s.Save(job)
	job.Status = domain.StatusSucceeded
	s.Update(job)
	got, _ := s.Get("j2")
	if got.Status != domain.StatusSucceeded {
		t.Fatalf("got %s", got.Status)
	}
}

func TestSaveDuplicate(t *testing.T) {
	s := store.New()
	s.Save(newJob("j3"))
	if err := s.Save(newJob("j3")); err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestReturnsCopy(t *testing.T) {
	s := store.New()
	s.Save(newJob("j4"))
	got, _ := s.Get("j4")
	got.Status = domain.StatusRunning
	original, _ := s.Get("j4")
	if original.Status != domain.StatusQueued {
		t.Fatal("store leaked reference instead of copy")
	}
}

func TestList(t *testing.T) {
	s := store.New()
	s.Save(newJob("a"))
	s.Save(newJob("b"))
	s.Save(newJob("c"))
	jobs, _ := s.List()
	if len(jobs) != 3 {
		t.Fatalf("expected 3, got %d", len(jobs))
	}
}
