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
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/runtime/mock"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	transkafka "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/kafka"
)

func main() {
	cfg, err := config.Load(8081)
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1. Conexão com PostgreSQL
	dbPool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to create postgres connection pool: %v", err)
	}
	defer dbPool.Close()

	if err := postgres.RunMigrations(ctx, dbPool, "migrations"); err != nil {
		log.Printf("warning: migrations execution failed: %v", err)
	}

	resultsRepo := postgres.NewResultsRepository(dbPool)

	// 2. Inicializa Kafka Consumer com Consumer Group "conversation-engine-workers"
	consumerCfg := transkafka.ConsumerConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    "conversation.events",
		GroupID:  "conversation-engine-workers",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  500 * time.Millisecond,
	}

	kafkaConsumer := transkafka.NewKafkaConsumer(consumerCfg)
	defer func() {
		if err := kafkaConsumer.Close(); err != nil {
			log.Printf("error closing kafka consumer: %v", err)
		}
	}()

	// 3. Inicializa MockRuntime e Worker Service com handler de pipeline integrado ao PostgreSQL
	mockRuntime := mock.NewMockRuntime(
		mock.WithLatency(150 * time.Millisecond), // Simula tempo de raciocínio da IA
		mock.WithSimulatedReply("Olá! Sou o assistente virtual Unyx. Como posso ajudar você hoje?"),
	)
	pipelineHandler := service.NewDefaultMessagePipelineHandler(mockRuntime, resultsRepo)
	workerService := service.NewConversationWorkerService(kafkaConsumer, pipelineHandler)

	// 4. Servidor HTTP de Health Check e Liveness
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
		_, _ = w.Write([]byte(`{"status":"ok","service":"worker"}`))
	})

	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"alive","service":"worker"}`))
	})

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("worker health server listening on :%d", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("worker health server error: %v", err)
		}
	}()

	// 5. Inicia o loop contínuo do worker em background
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		if err := workerService.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("worker loop stopped with error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down worker service gracefully...")

	// Drena com timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("worker health server shutdown error: %v", err)
	}

	<-workerDone
	log.Println("worker service stopped cleanly")
}
