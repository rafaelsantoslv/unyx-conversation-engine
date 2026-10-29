package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	transkafka "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/kafka"
	"github.com/segmentio/kafka-go"
)

// MessagePipelineHandler define o contrato para o pipeline de execução da mensagem.
type MessagePipelineHandler interface {
	HandleMessageReceived(ctx context.Context, evt domain.Event[domain.MessageReceivedPayload]) error
}

// ResultsRepository define a interface necessária para registrar o ciclo de vida da execução.
type ResultsRepository interface {
	GetResult(ctx context.Context, tenantID, conversationID, messageID string) (*domain.ProcessingResult, error)
	RecordExecutionStart(ctx context.Context, tenantID, conversationID, messageID string) (*domain.ProcessingResult, error)
	RecordExecutionSuccess(ctx context.Context, tenantID, conversationID, messageID string, output interface{}, executionTime time.Duration) error
	RecordExecutionFailure(ctx context.Context, tenantID, conversationID, messageID string, failureErr error, executionTime time.Duration) error
}

// DefaultMessagePipelineHandler é a implementação do pipeline que orquestra idempotência, AgentRuntime e persistência durável no PostgreSQL.
type DefaultMessagePipelineHandler struct {
	runtime     domain.AgentRuntime
	resultsRepo ResultsRepository
}

// NewDefaultMessagePipelineHandler cria um novo handler integrando o AgentRuntime e ResultsRepository.
func NewDefaultMessagePipelineHandler(runtime domain.AgentRuntime, resultsRepo ResultsRepository) *DefaultMessagePipelineHandler {
	return &DefaultMessagePipelineHandler{
		runtime:     runtime,
		resultsRepo: resultsRepo,
	}
}

func (h *DefaultMessagePipelineHandler) HandleMessageReceived(ctx context.Context, evt domain.Event[domain.MessageReceivedPayload]) error {
	log.Printf("[pipeline] message event received: event_id=%s, tenant=%s, conv=%s, msg_id=%s, role=%s",
		evt.EventID, evt.TenantID, evt.ConversationID, evt.Payload.MessageID, evt.Payload.Role)

	tenantID := evt.TenantID
	convID := evt.ConversationID
	msgID := evt.Payload.MessageID

	// 1. Verificação prévia de idempotência: se o evento já foi processado com sucesso, ignorar reexecução
	if h.resultsRepo != nil {
		existing, err := h.resultsRepo.GetResult(ctx, tenantID, convID, msgID)
		if err != nil {
			log.Printf("[pipeline] warning: failed to check existing processing result: %v", err)
			return fmt.Errorf("failed to check existing result: %w", err)
		}

		if existing != nil && existing.Status == domain.ExecutionStatusSucceeded {
			log.Printf("[pipeline] idempotency: message %s (conv=%s) already processed with status SUCCEEDED. Skipping execution.",
				msgID, convID)
			return nil
		}

		// 2. Registra início da execução como RUNNING
		if _, err := h.resultsRepo.RecordExecutionStart(ctx, tenantID, convID, msgID); err != nil {
			log.Printf("[pipeline] error recording execution start: %v", err)
			return fmt.Errorf("failed to record execution start: %w", err)
		}
	}

	start := time.Now()

	// 3. Execução do Runtime (IA / Mock)
	if h.runtime != nil {
		execInput := domain.ExecutionInput{
			TenantID:       tenantID,
			ConversationID: convID,
			MessageID:      msgID,
			Content:        evt.Payload.Content,
			Metadata: map[string]string{
				"channel":  evt.Payload.Channel,
				"role":     string(evt.Payload.Role),
				"event_id": evt.EventID,
			},
		}

		out, err := h.runtime.Execute(ctx, execInput)
		execDuration := time.Since(start)

		if err != nil {
			log.Printf("[pipeline] agent runtime failed for message %s: %v", msgID, err)
			if h.resultsRepo != nil {
				if recErr := h.resultsRepo.RecordExecutionFailure(ctx, tenantID, convID, msgID, err, execDuration); recErr != nil {
					log.Printf("[pipeline] warning: failed to record execution failure in db: %v", recErr)
				}
			}
			return fmt.Errorf("agent runtime execution error: %w", err)
		}

		log.Printf("[pipeline] agent runtime responded successfully: tokens=%d, reply_preview=%.40s...",
			out.TokensUsed, out.ResponseContent)

		// 4. Salva resultado de sucesso de forma durável no PostgreSQL
		if h.resultsRepo != nil {
			if err := h.resultsRepo.RecordExecutionSuccess(ctx, tenantID, convID, msgID, out, execDuration); err != nil {
				log.Printf("[pipeline] error saving execution success to db: %v", err)
				return fmt.Errorf("failed to record execution success in database: %w", err)
			}
		}
	}

	return nil
}

// ConversationWorkerService orquestra o consumo de eventos, desserialização, validação, execução e commit manual.
type ConversationWorkerService struct {
	consumer transkafka.Consumer
	handler  MessagePipelineHandler
}

// NewConversationWorkerService inicializa o serviço de processamento do worker.
func NewConversationWorkerService(consumer transkafka.Consumer, handler MessagePipelineHandler) *ConversationWorkerService {
	if handler == nil {
		handler = &DefaultMessagePipelineHandler{}
	}
	return &ConversationWorkerService{
		consumer: consumer,
		handler:  handler,
	}
}

// ProcessNextMessage busca a próxima mensagem no Kafka, valida e processa com commit manual.
func (w *ConversationWorkerService) ProcessNextMessage(ctx context.Context) error {
	msg, err := w.consumer.FetchMessage(ctx)
	if err != nil {
		return err
	}

	if err := w.handleMessage(ctx, msg); err != nil {
		return err
	}

	return nil
}

func (w *ConversationWorkerService) handleMessage(ctx context.Context, msg kafka.Message) error {
	var evt domain.Event[domain.MessageReceivedPayload]
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		// Evento totalmente corrompido ou JSON inválido:
		// Loga erro estruturado e efetua o commit para evitar loop infinito que trave a partição.
		log.Printf("ERROR: corrupted json payload in message (topic=%s, partition=%d, offset=%d, key=%s): %v. Payload: %s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), err, string(msg.Value))

		if commitErr := w.consumer.CommitMessages(ctx, msg); commitErr != nil {
			log.Printf("failed to commit corrupted message: %v", commitErr)
			return commitErr
		}
		return nil
	}

	// Validação de schema do Envelope e do Payload
	if err := evt.Validate(); err != nil {
		log.Printf("ERROR: invalid event schema (event_id=%s, key=%s, offset=%d): %v",
			evt.EventID, string(msg.Key), msg.Offset, err)

		if commitErr := w.consumer.CommitMessages(ctx, msg); commitErr != nil {
			log.Printf("failed to commit invalid schema message: %v", commitErr)
			return commitErr
		}
		return nil
	}

	if err := evt.Payload.Validate(); err != nil {
		log.Printf("ERROR: invalid message payload schema (event_id=%s, offset=%d): %v",
			evt.EventID, msg.Offset, err)

		if commitErr := w.consumer.CommitMessages(ctx, msg); commitErr != nil {
			log.Printf("failed to commit invalid payload message: %v", commitErr)
			return commitErr
		}
		return nil
	}

	// Encaminha para o pipeline de execução da conversa (que salva resultado no PostgreSQL)
	if err := w.handler.HandleMessageReceived(ctx, evt); err != nil {
		log.Printf("ERROR: pipeline execution failed for event %s: %v", evt.EventID, err)
		// NÃO commitar offset em caso de falha de execução de negócio ou de banco,
		// permitindo reprocessamento at-least-once pelo grupo.
		return fmt.Errorf("pipeline processing error: %w", err)
	}

	// Efetua commit manual do offset APÓS sucesso na persistência do resultado no PostgreSQL
	if err := w.consumer.CommitMessages(ctx, msg); err != nil {
		log.Printf("ERROR: failed to commit offset for event %s (offset=%d): %v",
			evt.EventID, msg.Offset, err)
		return fmt.Errorf("failed to commit offset: %w", err)
	}

	return nil
}

// Run executa o loop contínuo de consumo do worker até o cancelamento do context.
func (w *ConversationWorkerService) Run(ctx context.Context) error {
	log.Println("conversation worker loop started, listening for messages...")

	for {
		select {
		case <-ctx.Done():
			log.Println("conversation worker received stop signal, draining active loop...")
			return ctx.Err()
		default:
			err := w.ProcessNextMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || ctx.Err() != nil {
					return nil
				}
				log.Printf("worker process message error: %v", err)
			}
		}
	}
}
