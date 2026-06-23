package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/ultrakapy/async-job-pipeline/internal/domain"
)

type Store interface {
	Save(job *domain.Job) error
	Get(id string) (*domain.Job, error)
	Update(job *domain.Job) error
	List() ([]*domain.Job, error)
}

type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]*domain.Job
}

func New() *MemoryStore {
	return &MemoryStore{jobs: make(map[string]*domain.Job)}
}

func (s *MemoryStore) Save(job *domain.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[job.ID]; ok {
		return fmt.Errorf("job %s already exists", job.ID)
	}
	j := *job
	s.jobs[job.ID] = &j
	return nil
}

func (s *MemoryStore) Get(id string) (*domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, fmt.Errorf("job %s not found", id)
	}
	cp := *job
	return &cp, nil
}

func (s *MemoryStore) Update(job *domain.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[job.ID]; !ok {
		return fmt.Errorf("job %s not found", job.ID)
	}
	job.UpdatedAt = time.Now().UTC()
	j := *job
	s.jobs[job.ID] = &j
	return nil
}

func (s *MemoryStore) List() ([]*domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	jobs := make([]*domain.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		cp := *j
		jobs = append(jobs, &cp)
	}
	return jobs, nil
}
