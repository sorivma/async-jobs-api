package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sorivma/async-jobs-api/internal/httpapi"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	app := httpapi.NewApp(logger)
	server := &http.Server{
		Addr:         ":8080",
		Handler:      app.Handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	done := make(chan struct{})
	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
		<-stop
		logger.Info("shutdown started")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = app.Shutdown(ctx)
		_ = server.Shutdown(ctx)
		close(done)
	}()

	logger.Info("async jobs api started", "addr", server.Addr)
	err := server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
	if err == http.ErrServerClosed {
		<-done
	}
}
