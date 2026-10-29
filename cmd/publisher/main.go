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
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/handler"
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
	healthHandler := handler.NewHealthHandler(dbPool)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler.LivenessHandler)
	mux.HandleFunc("/livez", healthHandler.LivenessHandler)
	mux.HandleFunc("/readyz", healthHandler.ReadinessHandler)

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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("publisher server shutdown error: %v", err)
	}

	log.Println("publisher service stopped")
}
