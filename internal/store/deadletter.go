package store

import (
	"sync"

	"github.com/ultrakapy/async-job-pipeline/internal/domain"
)

type DeadLetterStore interface {
	Add(job *domain.Job) error
	List() ([]*domain.Job, error)
}

type MemoryDeadLetterStore struct {
	mu   sync.RWMutex
	jobs []*domain.Job
}

func NewDeadLetterStore() *MemoryDeadLetterStore {
	return &MemoryDeadLetterStore{}
}

func (s *MemoryDeadLetterStore) Add(job *domain.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *job
	s.jobs = append(s.jobs, &cp)
	return nil
}

func (s *MemoryDeadLetterStore) List() ([]*domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*domain.Job, len(s.jobs))
	for i, j := range s.jobs {
		cp := *j
		out[i] = &cp
	}
	return out, nil
}
