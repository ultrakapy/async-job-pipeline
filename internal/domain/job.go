package domain

import "time"

type Status string

const (
	StatusQueued       Status = "queued"
	StatusRunning      Status = "running"
	StatusSucceeded    Status = "succeeded"
	StatusFailed       Status = "failed"
	StatusDeadLettered Status = "dead_lettered"
	StatusCancelled    Status = "cancelled"
)

type Priority int

const (
	PriorityLow    Priority = 0
	PriorityNormal Priority = 5
	PriorityHigh   Priority = 10
)

type Job struct {
	ID          string         `json:"id"`
	Payload     map[string]any `json:"payload"`
	Priority    Priority       `json:"priority"`
	MaxRetries  int            `json:"max_retries"`
	TimeoutSecs int            `json:"timeout_secs"`
	Status      Status         `json:"status"`
	Attempts    int            `json:"attempts"`
	LastError   string         `json:"last_error,omitempty"`
	Result      map[string]any `json:"result,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	RunAfter    *time.Time     `json:"run_after,omitempty"`
}

type SubmitRequest struct {
	Payload     map[string]any `json:"payload"`
	Priority    Priority       `json:"priority"`
	MaxRetries  int            `json:"max_retries"`
	TimeoutSecs int            `json:"timeout_secs"`
	RunAfter    *time.Time     `json:"run_after,omitempty"`
}

type SubmitResponse struct {
	JobID string `json:"job_id"`
}
