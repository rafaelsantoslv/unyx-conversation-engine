package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/config"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	transporthttp "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/handler"
)

func main() {
	cfg, err := config.Load(8080)
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbPool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to create postgres connection pool: %v", err)
	}
	defer dbPool.Close()

	if err := postgres.RunMigrations(ctx, dbPool, "migrations"); err != nil {
		log.Printf("warning: migrations execution failed: %v", err)
	}

	repo := postgres.NewRepository(dbPool)
	ingestionSvc := service.NewIngestionService(repo)
	msgHandler := handler.NewMessageHandler(ingestionSvc)

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := dbPool.Ping(r.Context()); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"degraded","service":"api","error":"database unreachable"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","service":"api"}`))
	})

	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"alive","service":"api"}`))
	})

	mux.HandleFunc("/v1/messages", msgHandler.HandleIngest)

	handlerWithMiddlewares := transporthttp.RequestIDMiddleware(
		transporthttp.LoggingMiddleware(mux),
	)

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           handlerWithMiddlewares,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("api server starting on :%d", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("api server listen error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down api server gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("api server graceful shutdown failed: %v", err)
	}
	log.Println("api server stopped")
}
