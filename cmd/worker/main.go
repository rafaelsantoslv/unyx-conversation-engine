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
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/banner"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/config"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/runtime/mock"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/telemetry"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/handler"
	transkafka "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/kafka"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/worker"
)

func main() {
	banner.Print("Worker Engine", "1.0.0")
	telemetry.SetGlobalLogger(telemetry.NewJSONLogger(os.Stdout, 0))

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
	coordinator := postgres.NewExecutionCoordinator(dbPool)
	workerID := fmt.Sprintf("worker-%d", os.Getpid())

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
	pipelineHandler := service.NewDefaultMessagePipelineHandler(
		mockRuntime,
		resultsRepo,
		service.WithCoordinator(coordinator, workerID),
	)
	workerService := service.NewConversationWorkerService(
		kafkaConsumer,
		pipelineHandler,
		service.WithWorkerResultsRepo(resultsRepo),
	)

	// 4. Inicializa Reconciliador de Execuções Órfãs em background (INV-04, INV-05)
	reconciler := worker.NewExecutionReconciler(
		coordinator,
		resultsRepo,
		worker.ExecutionReconcilerConfig{
			PollInterval: 15 * time.Second,
			BatchSize:    20,
		},
	)
	go func() {
		if err := reconciler.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("execution reconciler stopped with error: %v", err)
		}
	}()

	// 4. Servidor HTTP de Health Check e Liveness
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
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("worker health server shutdown error: %v", err)
	}

	<-workerDone
	log.Println("worker service stopped cleanly")
}
