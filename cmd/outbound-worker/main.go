package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/config"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/outbound"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
)

func main() {
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

	log.Println("outbound worker service running...")
	workerSvc.Start(ctx)
	log.Println("outbound worker service gracefully stopped")
}
