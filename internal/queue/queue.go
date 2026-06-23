package queue

import "github.com/ultrakapy/async-job-pipeline/internal/domain"

type Queue interface {
	Enqueue(job *domain.Job)
	Dequeue() <-chan *domain.Job
	Depth() int64
	Drain() []*domain.Job
}

type ChannelQueue struct {
	ch chan *domain.Job
}

func New(capacity int) *ChannelQueue {
	return &ChannelQueue{ch: make(chan *domain.Job, capacity)}
}

// Enqueue blocks when queue is full — intentional backpressure.
// In production, use TryEnqueue with a 503 response on full.
func (q *ChannelQueue) Enqueue(job *domain.Job) {
	q.ch <- job
}

func (q *ChannelQueue) Dequeue() <-chan *domain.Job {
	return q.ch
}

func (q *ChannelQueue) Depth() int64 {
	return int64(len(q.ch))
}

func (q *ChannelQueue) Drain() []*domain.Job {
	var drained []*domain.Job
	for {
		select {
		case job := <-q.ch:
			drained = append(drained, job)
		default:
			return drained
		}
	}
}
