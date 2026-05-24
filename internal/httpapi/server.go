package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type ErrorResponse struct {
	Error     string            `json:"error"`
	Message   string            `json:"message"`
	RequestID string            `json:"request_id"`
	Details   map[string]string `json:"details,omitempty"`
}

type Job struct {
	ID          string         `json:"id"`
	Type        string         `json:"type"`
	Payload     map[string]any `json:"payload,omitempty"`
	Status      string         `json:"status"`
	CreatedAt   time.Time      `json:"created_at"`
	StartedAt   *time.Time     `json:"started_at,omitempty"`
	CompletedAt *time.Time     `json:"completed_at,omitempty"`
	Error       string         `json:"error,omitempty"`
}

type CreateJobRequest struct {
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload"`
}

type Metrics struct {
	RequestsTotal     uint64         `json:"requests_total"`
	ResponsesByStatus map[string]int `json:"responses_by_status"`
	AverageDurationMS float64        `json:"average_duration_ms"`
	JobsByStatus      map[string]int `json:"jobs_by_status"`
	Shutdown          bool           `json:"shutdown"`
}

type ListResponse[T any] struct {
	Items []T `json:"items"`
	Total int `json:"total"`
}

type appError struct {
	status  int
	code    string
	message string
	details map[string]string
}

func (e appError) Error() string { return e.message }

type JobManager struct {
	mu           sync.RWMutex
	nextID       int64
	jobs         map[string]*Job
	cancels      map[string]context.CancelFunc
	queue        chan string
	wg           sync.WaitGroup
	closeOnce    sync.Once
	shuttingDown atomic.Bool
	now          func() time.Time
}

func NewJobManager(workerCount int) *JobManager {
	if workerCount < 1 {
		workerCount = 1
	}
	m := &JobManager{
		jobs:    map[string]*Job{},
		cancels: map[string]context.CancelFunc{},
		queue:   make(chan string, 128),
		now:     func() time.Time { return time.Now().UTC() },
	}
	for i := 0; i < workerCount; i++ {
		m.wg.Add(1)
		go m.worker()
	}
	return m
}

func (m *JobManager) Create(req CreateJobRequest) (Job, error) {
	if m.shuttingDown.Load() {
		return Job{}, appError{status: http.StatusServiceUnavailable, code: "shutting_down", message: "new jobs are not accepted"}
	}
	if req.Type == "" {
		return Job{}, badRequest("invalid_job", "type is required")
	}
	duration, err := jobDuration(req)
	if err != nil {
		return Job{}, err
	}
	_ = duration
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.nextID++
	id := "job-" + strconv.FormatInt(m.nextID, 10)
	job := &Job{ID: id, Type: req.Type, Payload: req.Payload, Status: "queued", CreatedAt: m.now()}
	m.jobs[id] = job
	m.cancels[id] = cancel
	m.mu.Unlock()

	select {
	case m.queue <- id:
		return *job, nil
	case <-ctx.Done():
		return Job{}, appError{status: http.StatusServiceUnavailable, code: "job_cancelled", message: "job was cancelled before enqueue"}
	}
}

func (m *JobManager) List() []Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := make([]Job, 0, len(m.jobs))
	for _, job := range m.jobs {
		items = append(items, *job)
	}
	slices.SortFunc(items, func(a, b Job) int { return a.CreatedAt.Compare(b.CreatedAt) })
	return items
}

func (m *JobManager) Get(id string) (Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	job, ok := m.jobs[id]
	if !ok {
		return Job{}, notFound("job_not_found", "job not found")
	}
	return *job, nil
}

func (m *JobManager) Cancel(id string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[id]
	if !ok {
		return Job{}, notFound("job_not_found", "job not found")
	}
	if job.Status == "succeeded" || job.Status == "failed" || job.Status == "cancelled" {
		return Job{}, conflict("job_terminal", "job is already completed")
	}
	if cancel := m.cancels[id]; cancel != nil {
		cancel()
	}
	now := m.now()
	job.Status = "cancelled"
	job.CompletedAt = &now
	delete(m.cancels, id)
	return *job, nil
}

func (m *JobManager) Delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[id]
	if !ok {
		return notFound("job_not_found", "job not found")
	}
	if job.Status == "queued" || job.Status == "running" {
		return conflict("job_not_terminal", "only terminal jobs can be deleted")
	}
	delete(m.jobs, id)
	delete(m.cancels, id)
	return nil
}

func (m *JobManager) Metrics() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := map[string]int{"queued": 0, "running": 0, "succeeded": 0, "failed": 0, "cancelled": 0}
	for _, job := range m.jobs {
		out[job.Status]++
	}
	return out
}

func (m *JobManager) IsShuttingDown() bool {
	return m.shuttingDown.Load()
}

func (m *JobManager) Shutdown(ctx context.Context) error {
	m.shuttingDown.Store(true)
	m.mu.Lock()
	for _, cancel := range m.cancels {
		cancel()
	}
	m.mu.Unlock()
	m.closeOnce.Do(func() { close(m.queue) })

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *JobManager) worker() {
	defer m.wg.Done()
	for id := range m.queue {
		if m.shuttingDown.Load() {
			m.markCancelled(id)
			continue
		}
		m.runJob(id)
	}
}

func (m *JobManager) runJob(id string) {
	m.mu.Lock()
	job, ok := m.jobs[id]
	if !ok || job.Status != "queued" {
		m.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.cancels[id] = cancel
	start := m.now()
	job.Status = "running"
	job.StartedAt = &start
	duration, err := durationFromPayload(job.Payload)
	if err != nil {
		now := m.now()
		job.Status = "failed"
		job.Error = err.Error()
		job.CompletedAt = &now
		delete(m.cancels, id)
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()

	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		m.complete(id, "succeeded", "")
	case <-ctx.Done():
		m.complete(id, "cancelled", "")
	}
}

func (m *JobManager) complete(id, status, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[id]
	if !ok || job.Status == "cancelled" {
		return
	}
	now := m.now()
	job.Status = status
	job.Error = message
	job.CompletedAt = &now
	delete(m.cancels, id)
}

func (m *JobManager) markCancelled(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[id]
	if !ok || job.Status == "succeeded" || job.Status == "failed" || job.Status == "cancelled" {
		return
	}
	now := m.now()
	job.Status = "cancelled"
	job.CompletedAt = &now
	delete(m.cancels, id)
}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

type requestMetrics struct {
	mu              sync.RWMutex
	requests        uint64
	responses       map[string]int
	totalDurationNS int64
}

func newRequestMetrics() *requestMetrics {
	return &requestMetrics{responses: map[string]int{}}
}

func (m *requestMetrics) observe(status int, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests++
	m.responses[strconv.Itoa(status)]++
	m.totalDurationNS += duration.Nanoseconds()
}

func (m *requestMetrics) snapshot(jobs map[string]int, shutdown bool) Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()
	responses := map[string]int{}
	for k, v := range m.responses {
		responses[k] = v
	}
	avg := 0.0
	if m.requests > 0 {
		avg = float64(m.totalDurationNS) / float64(m.requests) / float64(time.Millisecond)
	}
	return Metrics{RequestsTotal: m.requests, ResponsesByStatus: responses, AverageDurationMS: avg, JobsByStatus: jobs, Shutdown: shutdown}
}

type App struct {
	Handler http.Handler
	jobs    *JobManager
}

func NewApp(logger *slog.Logger) *App {
	manager := NewJobManager(3)
	return &App{Handler: NewServerWithManager(logger, manager), jobs: manager}
}

func (a *App) Shutdown(ctx context.Context) error {
	return a.jobs.Shutdown(ctx)
}

func NewServer(logger *slog.Logger) http.Handler {
	return NewApp(logger).Handler
}

func NewServerWithManager(logger *slog.Logger, manager *JobManager) http.Handler {
	metrics := newRequestMetrics()
	s := &Server{logger: logger, jobs: manager, metrics: metrics}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/jobs", s.createJob)
	mux.HandleFunc("GET /api/v1/jobs", s.listJobs)
	mux.HandleFunc("GET /api/v1/jobs/{id}", s.getJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", s.cancelJob)
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", s.deleteJob)
	mux.HandleFunc("GET /api/v1/metrics", s.getMetrics)

	return recoverer(requestID(requestLogger(logger)(timeout(10 * time.Second)(metricsMiddleware(metrics)(rejectJobsOnShutdown(manager)(mux))))))
}

type Server struct {
	logger  *slog.Logger
	jobs    *JobManager
	metrics *requestMetrics
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req CreateJobRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	job, err := s.jobs.Create(req)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	items := s.jobs.List()
	writeJSON(w, http.StatusOK, ListResponse[Job]{Items: items, Total: len(items)})
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.jobs.Get(r.PathValue("id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.jobs.Cancel(r.PathValue("id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) deleteJob(w http.ResponseWriter, r *http.Request) {
	if err := s.jobs.Delete(r.PathValue("id")); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.metrics.snapshot(s.jobs.Metrics(), s.jobs.IsShuttingDown()))
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, r, badRequest("invalid_json", "invalid json body"))
		return false
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		writeError(w, r, badRequest("invalid_json", "body must contain a single json object"))
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, r *http.Request, err error) {
	var app appError
	if !errors.As(err, &app) {
		app = appError{status: http.StatusInternalServerError, code: "internal_error", message: "internal server error"}
	}
	writeJSON(w, app.status, ErrorResponse{Error: app.code, Message: app.message, RequestID: r.Header.Get("X-Request-ID"), Details: app.details})
}

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = randomID()
		}
		r.Header.Set("X-Request-ID", id)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r)
	})
}

func requestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			next.ServeHTTP(w, r)
			logger.Info("request", "request_id", r.Header.Get("X-Request-ID"), "method", r.Method, "path", r.URL.Path, "duration", time.Since(start))
		})
	}
}

func timeout(d time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.TimeoutHandler(next, d, `{"error":"timeout","message":"request timed out"}`)
	}
}

func metricsMiddleware(metrics *requestMetrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
			start := time.Now()
			next.ServeHTTP(rec, r)
			metrics.observe(rec.status, time.Since(start))
		})
	}
}

func rejectJobsOnShutdown(manager *JobManager) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if manager.IsShuttingDown() && r.Method == http.MethodPost && r.URL.Path == "/api/v1/jobs" {
				writeError(w, r, appError{status: http.StatusServiceUnavailable, code: "shutting_down", message: "new jobs are not accepted"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				writeError(w, r, appError{status: http.StatusInternalServerError, code: "internal_error", message: "internal server error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func jobDuration(req CreateJobRequest) (time.Duration, error) {
	return durationFromPayload(req.Payload)
}

func durationFromPayload(payload map[string]any) (time.Duration, error) {
	raw, ok := payload["duration_seconds"]
	if !ok {
		return 0, badRequest("invalid_job", "payload.duration_seconds is required")
	}
	var seconds float64
	switch value := raw.(type) {
	case float64:
		seconds = value
	case int:
		seconds = float64(value)
	default:
		return 0, badRequest("invalid_job", "payload.duration_seconds must be a number")
	}
	if seconds < 0 || seconds > 3600 {
		return 0, badRequest("invalid_job", "payload.duration_seconds must be between 0 and 3600")
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

func badRequest(code, message string) appError {
	return appError{status: http.StatusBadRequest, code: code, message: message}
}

func notFound(code, message string) appError {
	return appError{status: http.StatusNotFound, code: code, message: message}
}

func conflict(code, message string) appError {
	return appError{status: http.StatusConflict, code: code, message: message}
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b[:])
}
