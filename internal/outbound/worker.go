package outbound

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
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
	MarkUnknown(ctx context.Context, intentID string, errReason string, retryAfter time.Duration) error
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
	ReconcileAmbiguousIntent(ctx context.Context, baseURL, apiAccessToken, accountID, conversationID string, expectedEchoID, expectedCorrelationID, expectedContent string) (*ports.DeliveryReceipt, error)
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

	// 4. Reconciliação de Entregas Ambíguas (INV-08)
	// Se a intenção já esteve em estado UNKNOWN (ou falhou por timeout anterior),
	// consulta ativamente a API do Chatwoot antes de enviar para evitar duplicar mensagem no WhatsApp.
	if intent.Status == domain.OutboundStatusUnknown || intent.Attempts > 1 {
		echoID := intent.ExecutionID
		reconciledReceipt, recErr := w.chatwootSender.ReconcileAmbiguousIntent(
			ctx,
			cred.APIURL,
			cred.APIAccessToken,
			mapping.ExternalAccountID,
			mapping.ExternalConversationID,
			echoID,
			intent.ExecutionID,
			intent.Payload.Content,
		)
		if recErr != nil {
			log.Printf("[outbound-worker] reconciliation check failed for intent %s: %v. Postponing retry.", intent.ID, recErr)
			_ = w.intentsStore.MarkUnknown(ctx, intent.ID, fmt.Sprintf("reconciliation_check_failed: %v", recErr), 10*time.Second)
			return recErr
		}

		if reconciledReceipt != nil {
			log.Printf("[outbound-worker] ambiguous intent %s RECONCILED successfully! Found message %s in Chatwoot (prevented duplicate send).",
				intent.ID, reconciledReceipt.ExternalMessageID)
			if w.echoTracker != nil {
				w.echoTracker.RegisterSentMessage(ctx, intent.TenantID, reconciledReceipt.ExternalMessageID, intent.ExecutionID, 5*time.Minute)
			}
			return w.intentsStore.MarkSent(ctx, intent.ID, reconciledReceipt.ExternalMessageID)
		}
	}

	// 5. Despacha mensagem para a API do Chatwoot com echo_id e correlation_id
	receipt, err := w.chatwootSender.SendMessage(ctx, chatwoot.SendMessageParams{
		BaseURL:        cred.APIURL,
		APIAccessToken: cred.APIAccessToken,
		AccountID:      mapping.ExternalAccountID,
		ConversationID: mapping.ExternalConversationID,
		Content:        intent.Payload.Content,
		Private:        false,
		EchoID:         intent.ExecutionID,
		CorrelationID:  intent.ExecutionID,
	})

	if err != nil {
		// Se for erro de timeout ou conexão de rede (onde a requisição pode ter chegado ao Chatwoot mas a resposta caiu),
		// classifica como UNKNOWN para reconciliar no próximo ciclo sem duplicar (INV-08).
		if isAmbiguousNetworkError(err) {
			log.Printf("[outbound-worker] network timeout/ambiguous error sending intent %s: %v. Marking as UNKNOWN for active reconciliation.", intent.ID, err)
			_ = w.intentsStore.MarkUnknown(ctx, intent.ID, fmt.Sprintf("ambiguous_network_timeout: %v", err), 5*time.Second)
			return err
		}

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

func isAmbiguousNetworkError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "deadline exceeded") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "temporary failure") ||
		strings.Contains(errStr, "eof") ||
		strings.Contains(errStr, "connection refused")
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
