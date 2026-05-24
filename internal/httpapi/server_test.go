package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestJobsLifecycleMetricsAndDelete(t *testing.T) {
	manager := NewJobManager(1)
	h := NewServerWithManager(slog.New(slog.NewTextHandler(io.Discard, nil)), manager)
	defer manager.Shutdown(context.Background())

	job := doJSON[Job](t, h, http.MethodPost, "/api/v1/jobs", `{"type":"report_generation","payload":{"duration_seconds":0.01}}`, http.StatusAccepted)
	waitForStatus(t, h, job.ID, "succeeded")

	got := doJSON[Job](t, h, http.MethodGet, "/api/v1/jobs/"+job.ID, ``, http.StatusOK)
	if got.StartedAt == nil || got.CompletedAt == nil {
		t.Fatalf("expected timestamps, got %+v", got)
	}
	list := doJSON[ListResponse[Job]](t, h, http.MethodGet, "/api/v1/jobs", ``, http.StatusOK)
	if list.Total != 1 {
		t.Fatalf("list total = %d, want 1", list.Total)
	}
	metrics := doJSON[Metrics](t, h, http.MethodGet, "/api/v1/metrics", ``, http.StatusOK)
	if metrics.RequestsTotal == 0 || metrics.JobsByStatus["succeeded"] != 1 {
		t.Fatalf("metrics = %+v", metrics)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/"+job.ID, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d", rec.Code)
	}
	doJSON[ErrorResponse](t, h, http.MethodGet, "/api/v1/jobs/"+job.ID, ``, http.StatusNotFound)
}

func TestCancelRunningJobAndValidation(t *testing.T) {
	manager := NewJobManager(1)
	h := NewServerWithManager(slog.New(slog.NewTextHandler(io.Discard, nil)), manager)
	defer manager.Shutdown(context.Background())

	doJSON[ErrorResponse](t, h, http.MethodPost, "/api/v1/jobs", `{"payload":{"duration_seconds":1}}`, http.StatusBadRequest)
	job := doJSON[Job](t, h, http.MethodPost, "/api/v1/jobs", `{"type":"report_generation","payload":{"duration_seconds":1}}`, http.StatusAccepted)
	cancelled := doJSON[Job](t, h, http.MethodPost, "/api/v1/jobs/"+job.ID+"/cancel", `{}`, http.StatusOK)
	if cancelled.Status != "cancelled" {
		t.Fatalf("status = %s, want cancelled", cancelled.Status)
	}
	doJSON[ErrorResponse](t, h, http.MethodPost, "/api/v1/jobs/"+job.ID+"/cancel", `{}`, http.StatusConflict)
}

func TestShutdownRejectsNewJobs(t *testing.T) {
	manager := NewJobManager(1)
	h := NewServerWithManager(slog.New(slog.NewTextHandler(io.Discard, nil)), manager)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = manager.Shutdown(ctx)

	doJSON[ErrorResponse](t, h, http.MethodPost, "/api/v1/jobs", `{"type":"report_generation","payload":{"duration_seconds":0}}`, http.StatusServiceUnavailable)
	metrics := doJSON[Metrics](t, h, http.MethodGet, "/api/v1/metrics", ``, http.StatusOK)
	if !metrics.Shutdown {
		t.Fatalf("shutdown metric = false")
	}
}

func waitForStatus(t *testing.T, h http.Handler, id, status string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job := doJSON[Job](t, h, http.MethodGet, "/api/v1/jobs/"+id, ``, http.StatusOK)
		if job.Status == status {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach status %s", id, status)
}

func doJSON[T any](t *testing.T, h http.Handler, method, path, body string, want int) T {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("%s %s status = %d, want %d, body=%s", method, path, rec.Code, want, rec.Body.String())
	}
	var out T
	if rec.Body.Len() > 0 {
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatalf("decode response: %v; body=%s", err, rec.Body.String())
		}
	}
	return out
}
