package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ultrakapy/async-job-pipeline/internal/api"
	"github.com/ultrakapy/async-job-pipeline/internal/domain"
	"github.com/ultrakapy/async-job-pipeline/internal/queue"
	"github.com/ultrakapy/async-job-pipeline/internal/store"
	"github.com/ultrakapy/async-job-pipeline/internal/worker"
)

func successHandler(_ context.Context, p map[string]any) (map[string]any, error) {
	return map[string]any{"ok": true}, nil
}

func newTestServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	st := store.New()
	q := queue.New(100)
	pool := worker.NewPool(2, q, st, store.NewDeadLetterStore(), successHandler)
	h := api.NewHandler(st, q, store.NewDeadLetterStore())
	pool.Start()
	ts := httptest.NewServer(api.NewRouter(h))
	return ts, func() { ts.Close(); pool.Stop() }
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestSubmitAndPoll(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()

	resp := post(t, ts.URL+"/api/v1/jobs", `{"payload":{"x":1},"max_retries":3,"timeout_secs":5}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}
	var sub domain.SubmitResponse
	json.NewDecoder(resp.Body).Decode(&sub)
	if sub.JobID == "" {
		t.Fatal("empty job_id")
	}

	var job domain.Job
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		r, _ := http.Get(ts.URL + "/api/v1/jobs/" + sub.JobID)
		json.NewDecoder(r.Body).Decode(&job)
		if job.Status == domain.StatusSucceeded {
			break
		}
	}
	if job.Status != domain.StatusSucceeded {
		t.Fatalf("expected succeeded, got %s", job.Status)
	}
}

func TestQueueDepthAndDrain(t *testing.T) {
	st := store.New()
	q := queue.New(100)
	ts := httptest.NewServer(api.NewRouter(api.NewHandler(st, q, store.NewDeadLetterStore())))
	defer ts.Close()

	for i := 0; i < 3; i++ {
		post(t, ts.URL+"/api/v1/jobs", `{"payload":{},"max_retries":1,"timeout_secs":5}`)
	}

	var depth map[string]int64
	r, _ := http.Get(ts.URL + "/api/v1/queue/depth")
	json.NewDecoder(r.Body).Decode(&depth)
	if depth["depth"] != 3 {
		t.Fatalf("expected 3, got %d", depth["depth"])
	}

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/queue/drain", nil)
	dr, _ := http.DefaultClient.Do(req)
	var drained map[string]int
	json.NewDecoder(dr.Body).Decode(&drained)
	if drained["cancelled"] != 3 {
		t.Fatalf("expected 3 cancelled, got %d", drained["cancelled"])
	}
}

func TestCancelJob(t *testing.T) {
	st := store.New()
	q := queue.New(100)
	ts := httptest.NewServer(api.NewRouter(api.NewHandler(st, q, store.NewDeadLetterStore())))
	defer ts.Close()

	resp := post(t, ts.URL+"/api/v1/jobs", `{"payload":{},"max_retries":1,"timeout_secs":5}`)
	var sub domain.SubmitResponse
	json.NewDecoder(resp.Body).Decode(&sub)

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/api/v1/jobs/"+sub.JobID, nil)
	cr, _ := http.DefaultClient.Do(req)
	if cr.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", cr.StatusCode)
	}

	var job domain.Job
	r, _ := http.Get(ts.URL + "/api/v1/jobs/" + sub.JobID)
	json.NewDecoder(r.Body).Decode(&job)
	if job.Status != domain.StatusCancelled {
		t.Fatalf("expected cancelled, got %s", job.Status)
	}
}

func TestHealth(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()
	r, _ := http.Get(ts.URL + "/health")
	if r.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", r.StatusCode)
	}
}

func TestNotFound(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()
	r, _ := http.Get(ts.URL + "/api/v1/jobs/nonexistent")
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", r.StatusCode)
	}
}
