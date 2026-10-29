package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
)

// ExecutionReconcilerCoordinator define as operações necessárias do coordenador para o reconciliador.
type ExecutionReconcilerCoordinator interface {
	ReconcileOrphanExecutions(ctx context.Context, batchSize int) ([]postgres.ReconciledExecution, error)
}

// ExecutionReconcilerQuarantine define a interface para isolar execuções que esgotaram tentativas.
type ExecutionReconcilerQuarantine interface {
	QuarantineEvent(ctx context.Context, qEvent domain.QuarantinedEvent) error
}

// ExecutionReconcilerConfig define as opções do reconciliador em background.
type ExecutionReconcilerConfig struct {
	PollInterval time.Duration
	BatchSize    int
}

// ExecutionReconciler varre periodicamente tarefas em estado RUNNING com lease_expires_at < NOW()
// e as recupera de forma atômica com incremento estrito do fencing_token (INV-04, INV-05).
type ExecutionReconciler struct {
	coordinator ExecutionReconcilerCoordinator
	quarantine  ExecutionReconcilerQuarantine
	pollInterval time.Duration
	batchSize    int
}

// NewExecutionReconciler inicializa uma nova instância do Reconciliador.
func NewExecutionReconciler(
	coordinator ExecutionReconcilerCoordinator,
	quarantine ExecutionReconcilerQuarantine,
	cfg ExecutionReconcilerConfig,
) *ExecutionReconciler {
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = 15 * time.Second
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 20
	}

	return &ExecutionReconciler{
		coordinator:  coordinator,
		quarantine:   quarantine,
		pollInterval: pollInterval,
		batchSize:    batchSize,
	}
}

// ReconcileOnce executa um ciclo síncrono de reconciliação de execuções órfãs.
// Retorna a quantidade de execuções recuperadas e eventual erro.
func (r *ExecutionReconciler) ReconcileOnce(ctx context.Context) (int, error) {
	reconciled, err := r.coordinator.ReconcileOrphanExecutions(ctx, r.batchSize)
	if err != nil {
		return 0, fmt.Errorf("reconciliation query error: %w", err)
	}

	for _, rec := range reconciled {
		log.Printf("[reconciler] WARN: orphan execution recovered! id=%s, conv=%s, tenant=%s, old_worker=%s, new_fencing_token=%d, attempt=%d/%d, quarantined=%v (INV-04, INV-05)",
			rec.ID, rec.ConversationID, rec.TenantID, rec.OldWorkerID, rec.FencingToken, rec.AttemptCount, rec.MaxAttempts, rec.IsQuarantined)

		if rec.IsQuarantined && r.quarantine != nil {
			payloadBytes, _ := json.Marshal(map[string]interface{}{
				"execution_id":    rec.ID,
				"conversation_id": rec.ConversationID,
				"old_worker_id":   rec.OldWorkerID,
				"attempts":        rec.AttemptCount,
			})

			qErr := r.quarantine.QuarantineEvent(ctx, domain.QuarantinedEvent{
				TenantID:       rec.TenantID,
				ConversationID: rec.ConversationID,
				EventID:        fmt.Sprintf("orphan-exhausted-%s", rec.ID),
				Payload:        payloadBytes,
				ErrorReason:    fmt.Sprintf("execution lease expired repeatedly, exceeded max attempts (%d)", rec.MaxAttempts),
				AttemptCount:   rec.AttemptCount,
				QuarantinedAt:  time.Now().UTC(),
			})
			if qErr != nil {
				log.Printf("[reconciler] warning: failed to quarantine exhausted orphan execution: %v", qErr)
			}
		}
	}

	return len(reconciled), nil
}

// Start inicia o loop de background do reconciliador até o cancelamento do context.
func (r *ExecutionReconciler) Start(ctx context.Context) error {
	log.Printf("[reconciler] execution reconciler started (interval: %v, batchSize: %d)", r.pollInterval, r.batchSize)
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[reconciler] execution reconciler shutting down...")
			return ctx.Err()
		case <-ticker.C:
			count, err := r.ReconcileOnce(ctx)
			if err != nil {
				log.Printf("[reconciler] error during reconciliation pass: %v", err)
			} else if count > 0 {
				log.Printf("[reconciler] successfully reconciled %d orphan execution(s)", count)
			}
		}
	}
}
