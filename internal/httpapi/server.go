package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

type ErrorResponse struct {
	Error     string            `json:"error"`
	Message   string            `json:"message"`
	RequestID string            `json:"request_id"`
	Details   map[string]string `json:"details,omitempty"`
}

type Server struct {
	logger       *slog.Logger
	shuttingDown atomic.Bool
	requests     atomic.Uint64
}

func NewServer(logger *slog.Logger) http.Handler {
	s := &Server{logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/jobs", s.notImplemented("create job"))
	mux.HandleFunc("GET /api/v1/jobs", s.notImplemented("list jobs"))
	mux.HandleFunc("GET /api/v1/jobs/{id}", s.notImplemented("get job"))
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", s.notImplemented("cancel job"))
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", s.notImplemented("delete job"))
	mux.HandleFunc("GET /api/v1/metrics", s.metrics)

	return recoverer(requestID(requestLogger(logger)(timeout(5 * time.Second)(metricsMiddleware(s)(rejectJobsOnShutdown(s)(mux))))))
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"requests_total": s.requests.Load(),
		"shutdown":       s.shuttingDown.Load(),
	})
}

func (s *Server) notImplemented(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, http.StatusNotImplemented, "not_implemented", action+" is not implemented yet", nil)
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string, details map[string]string) {
	writeJSON(w, status, ErrorResponse{Error: code, Message: message, RequestID: r.Header.Get("X-Request-ID"), Details: details})
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

func metricsMiddleware(s *Server) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.requests.Add(1)
			next.ServeHTTP(w, r)
		})
	}
}

func rejectJobsOnShutdown(s *Server) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if s.shuttingDown.Load() && r.Method == http.MethodPost && r.URL.Path == "/api/v1/jobs" {
				writeError(w, r, http.StatusServiceUnavailable, "shutting_down", "new jobs are not accepted", nil)
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
				writeError(w, r, http.StatusInternalServerError, "internal_error", "internal server error", nil)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b[:])
}
