package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
)

// ScheduleClaimer define a interface para reivindicação atômica de agendamentos no banco.
type ScheduleClaimer interface {
	ClaimDueSchedulesWithOutbox(ctx context.Context, batchSize int, now time.Time) ([]domain.ConversationSchedule, error)
}

// ConversationSchedulerConfig define a configuração do agendador em background.
type ConversationSchedulerConfig struct {
	PollInterval time.Duration
	BatchSize    int
}

// ConversationScheduler é o serviço responsável por consultar periodicamente o PostgreSQL por
// agendamentos duráveis expirados e reivindicá-los atomicamente, gerando eventos na outbox (INV-03, INV-05, INV-08).
type ConversationScheduler struct {
	claimer      ScheduleClaimer
	pollInterval time.Duration
	batchSize    int
}

// NewConversationScheduler cria uma nova instância do ConversationScheduler.
func NewConversationScheduler(claimer ScheduleClaimer, cfg ConversationSchedulerConfig) *ConversationScheduler {
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 50
	}

	return &ConversationScheduler{
		claimer:      claimer,
		pollInterval: pollInterval,
		batchSize:    batchSize,
	}
}

// PollOnce executa um único ciclo de verificação e reivindicação de agendamentos devidos.
func (s *ConversationScheduler) PollOnce(ctx context.Context) (int, error) {
	now := time.Now().UTC()
	claimed, err := s.claimer.ClaimDueSchedulesWithOutbox(ctx, s.batchSize, now)
	if err != nil {
		return 0, fmt.Errorf("scheduler claim error: %w", err)
	}

	for _, item := range claimed {
		log.Printf("[scheduler] due schedule claimed for conversation %s (tenant %s, scheduled_for=%v) - outbox event emitted",
			item.ConversationID, item.TenantID, item.ScheduledFor)
	}

	return len(claimed), nil
}

// Start inicia o loop contínuo do agendador até que o contexto seja cancelado.
func (s *ConversationScheduler) Start(ctx context.Context) {
	log.Printf("[scheduler] starting conversation scheduler (poll_interval=%v, batch_size=%d)...", s.pollInterval, s.batchSize)
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[scheduler] stopping conversation scheduler...")
			return
		case <-ticker.C:
			count, err := s.PollOnce(ctx)
			if err != nil {
				log.Printf("[scheduler] poll error: %v", err)
			} else if count > 0 {
				log.Printf("[scheduler] processed and claimed %d due schedule(s)", count)
			}
		}
	}
}
