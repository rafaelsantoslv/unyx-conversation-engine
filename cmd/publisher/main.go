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
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/kafka"
)

func main() {
	cfg, err := config.Load(8082)
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1. Inicializa Pool com PostgreSQL
	dbPool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to create postgres connection pool: %v", err)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(ctx); err != nil {
		log.Printf("warning: postgres ping failed at startup: %v", err)
	}

	// 2. Inicializa Kafka Publisher (acks=all)
	kafkaPublisher := kafka.NewKafkaPublisher(cfg.KafkaBrokers)
	defer func() {
		if err := kafkaPublisher.Close(); err != nil {
			log.Printf("error closing kafka publisher: %v", err)
		}
	}()

	repo := postgres.NewRepository(dbPool)
	pubService := service.NewOutboxPublisherService(
		repo,
		kafkaPublisher,
		service.WithDefaultTopic("conversation.events"),
		service.WithBatchSize(50),
		service.WithPollInterval(100*time.Millisecond),
	)

	// 3. Servidor HTTP de Health Check e Métricas/Liveness
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := dbPool.Ping(r.Context()); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"degraded","error":%q}`, err.Error())))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","service":"publisher"}`))
	})

	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"alive","service":"publisher"}`))
	})

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("publisher health server listening on :%d", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("publisher health server error: %v", err)
		}
	}()

	// 4. Executa loop contínuo do Outbox Publisher
	go func() {
		if err := pubService.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("outbox publisher loop terminated with error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down publisher service gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("publisher server shutdown error: %v", err)
	}

	log.Println("publisher service stopped")
}
