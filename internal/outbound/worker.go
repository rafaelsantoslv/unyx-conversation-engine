package outbound

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/ports"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
)

type OutboundIntentsStore interface {
	ClaimPendingIntents(ctx context.Context, limit int, now time.Time) ([]domain.OutboundIntent, error)
	MarkSent(ctx context.Context, intentID, externalMessageID string) error
	MarkFailed(ctx context.Context, intentID string, errReason string, retryAfter time.Duration) error
	MarkCancelled(ctx context.Context, intentID string, reason string) error
}

type TenantCredentialsStore interface {
	GetIntegration(ctx context.Context, tenantID, channel string) (*postgres.TenantIntegration, error)
}

type ChannelMappingStore interface {
	GetMappingByConversationID(ctx context.Context, tenantID, conversationID string) (*postgres.ConversationChannelMapping, error)
}

type ConversationStateChecker interface {
	GetConversationByID(ctx context.Context, conversationID string) (*domain.Conversation, error)
}

type ChatwootSender interface {
	SendMessage(ctx context.Context, params chatwoot.SendMessageParams) (*ports.DeliveryReceipt, error)
}

// OutboundWorkerService consome intenções prontas no Postgres e despacha via Chatwoot (INV-04, INV-05, INV-06, INV-07).
type OutboundWorkerService struct {
	intentsStore   OutboundIntentsStore
	credsStore     TenantCredentialsStore
	mappingStore   ChannelMappingStore
	stateChecker   ConversationStateChecker
	chatwootSender ChatwootSender
	echoTracker    chatwoot.OutboundIntentTracker
	pollInterval   time.Duration
	batchSize      int
}

type WorkerOption func(*OutboundWorkerService)

func WithPollInterval(interval time.Duration) WorkerOption {
	return func(w *OutboundWorkerService) {
		if interval > 0 {
			w.pollInterval = interval
		}
	}
}

func WithBatchSize(size int) WorkerOption {
	return func(w *OutboundWorkerService) {
		if size > 0 {
			w.batchSize = size
		}
	}
}

func WithEchoTracker(tracker chatwoot.OutboundIntentTracker) WorkerOption {
	return func(w *OutboundWorkerService) {
		w.echoTracker = tracker
	}
}

func NewOutboundWorkerService(
	intentsStore OutboundIntentsStore,
	credsStore TenantCredentialsStore,
	mappingStore ChannelMappingStore,
	stateChecker ConversationStateChecker,
	chatwootSender ChatwootSender,
	opts ...WorkerOption,
) *OutboundWorkerService {
	w := &OutboundWorkerService{
		intentsStore:   intentsStore,
		credsStore:     credsStore,
		mappingStore:   mappingStore,
		stateChecker:   stateChecker,
		chatwootSender: chatwootSender,
		pollInterval:   100 * time.Millisecond,
		batchSize:      10,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// ProcessBatch executa uma rodada de polling e despacho de intenções.
func (w *OutboundWorkerService) ProcessBatch(ctx context.Context) (int, error) {
	intents, err := w.intentsStore.ClaimPendingIntents(ctx, w.batchSize, time.Now().UTC())
	if err != nil {
		return 0, fmt.Errorf("failed to claim pending intents: %w", err)
	}

	for _, intent := range intents {
		if err := w.dispatchIntent(ctx, intent); err != nil {
			log.Printf("[outbound-worker] failed to dispatch intent %s: %v", intent.ID, err)
		}
	}

	return len(intents), nil
}

func (w *OutboundWorkerService) dispatchIntent(ctx context.Context, intent domain.OutboundIntent) error {
	// 1. Verificação de Elegibilidade (INV-05): Conversa deve continuar em AI_ACTIVE
	if w.stateChecker != nil {
		conv, err := w.stateChecker.GetConversationByID(ctx, intent.ConversationID)
		if err == nil && conv != nil {
			if conv.State != domain.StateAIActive {
				log.Printf("[outbound-worker] conversation %s state is %s (not AI_ACTIVE). Cancelling outbound intent %s",
					conv.ID, conv.State, intent.ID)
				return w.intentsStore.MarkCancelled(ctx, intent.ID, fmt.Sprintf("state_changed_to_%s", conv.State))
			}
		}
	}

	// 2. Busca credenciais de integração do Tenant
	cred, err := w.credsStore.GetIntegration(ctx, intent.TenantID, "chatwoot")
	if err != nil || cred == nil || cred.APIURL == "" || cred.APIAccessToken == "" {
		_ = w.intentsStore.MarkFailed(ctx, intent.ID, "missing chatwoot credentials for tenant", 30*time.Second)
		return fmt.Errorf("missing chatwoot integration credentials for tenant %s", intent.TenantID)
	}

	// 3. Busca mapeamento externo de conversa
	mapping, err := w.mappingStore.GetMappingByConversationID(ctx, intent.TenantID, intent.ConversationID)
	if err != nil || mapping == nil {
		_ = w.intentsStore.MarkFailed(ctx, intent.ID, "missing channel mapping for conversation", 30*time.Second)
		return fmt.Errorf("missing conversation channel mapping: %w", err)
	}

	// 4. Despacha mensagem para a API do Chatwoot
	receipt, err := w.chatwootSender.SendMessage(ctx, chatwoot.SendMessageParams{
		BaseURL:        cred.APIURL,
		APIAccessToken: cred.APIAccessToken,
		AccountID:      mapping.ExternalAccountID,
		ConversationID: mapping.ExternalConversationID,
		Content:        intent.Payload.Content,
		Private:        false,
	})

	if err != nil {
		retryAfter := 5 * time.Second
		if errors.Is(err, chatwoot.ErrRateLimited) {
			retryAfter = 15 * time.Second
		}
		_ = w.intentsStore.MarkFailed(ctx, intent.ID, err.Error(), retryAfter)
		return err
	}

	// 5. Registra no tracker de ecos para blindagem contra loop (INV-03)
	if w.echoTracker != nil && receipt != nil {
		w.echoTracker.RegisterSentMessage(ctx, intent.TenantID, receipt.ExternalMessageID, intent.ExecutionID, 5*time.Minute)
	}

	// 6. Atualiza status para SENT gravando o external_message_id retornado
	extMsgID := ""
	if receipt != nil {
		extMsgID = receipt.ExternalMessageID
	}
	return w.intentsStore.MarkSent(ctx, intent.ID, extMsgID)
}

// Start inicia o loop contínuo do Outbound Worker com suporte a cancelamento de contexto.
func (w *OutboundWorkerService) Start(ctx context.Context) {
	log.Printf("[outbound-worker] started (poll_interval=%v, batch_size=%d)...", w.pollInterval, w.batchSize)
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[outbound-worker] stopping...")
			return
		case <-ticker.C:
			_, _ = w.ProcessBatch(ctx)
		}
	}
}
