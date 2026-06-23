package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ultrakapy/async-job-pipeline/internal/api"
	"github.com/ultrakapy/async-job-pipeline/internal/config"
	"github.com/ultrakapy/async-job-pipeline/internal/queue"
	"github.com/ultrakapy/async-job-pipeline/internal/store"
	"github.com/ultrakapy/async-job-pipeline/internal/worker"
)

// echoHandler is the default pluggable handler — swap with real business logic.
func echoHandler(ctx context.Context, payload map[string]any) (map[string]any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(50 * time.Millisecond):
		return map[string]any{"echo": payload, "processed_at": time.Now().UTC()}, nil
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg := config.Load()

	st := store.New()
	q := queue.New(cfg.QueueCap)
	pool := worker.NewPool(cfg.WorkerCount, q, st, echoHandler)
	h := api.NewHandler(st, q)
	router := api.NewRouter(h)

	pool.Start()
	slog.Info("worker pool started", "workers", cfg.WorkerCount)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("server listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutdown signal received")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	pool.Stop()
	slog.Info("server exited cleanly")
}
