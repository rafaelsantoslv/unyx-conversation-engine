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
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/outbound"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/telemetry"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/handler"
)

func main() {
	telemetry.SetGlobalLogger(telemetry.NewJSONLogger(os.Stdout, 0))

	cfg, err := config.Load(8083)
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbPool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer dbPool.Close()

	coreRepo := postgres.NewRepository(dbPool)
	outboundRepo := postgres.NewOutboundIntentsRepository(dbPool)
	integrationRepo := postgres.NewIntegrationRepository(dbPool)
	mappingRepo := postgres.NewIdentityMappingRepository(dbPool)

	chatwootSender := chatwoot.NewChatwootOutboundSender(nil)
	echoTracker := chatwoot.NewInMemoryEchoTracker(10 * time.Minute)

	workerSvc := outbound.NewOutboundWorkerService(
		outboundRepo,
		integrationRepo,
		mappingRepo,
		coreRepo,
		chatwootSender,
		outbound.WithEchoTracker(echoTracker),
		outbound.WithPollInterval(200*time.Millisecond),
		outbound.WithBatchSize(20),
	)

	// Health and readiness HTTP server
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
		log.Printf("outbound worker health server listening on :%d", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("outbound worker health server error: %v", err)
		}
	}()

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		log.Println("outbound worker service running...")
		workerSvc.Start(ctx)
	}()

	<-ctx.Done()
	log.Println("shutting down outbound worker service gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("outbound worker health server shutdown error: %v", err)
	}

	<-workerDone
	log.Println("outbound worker service gracefully stopped")
}
