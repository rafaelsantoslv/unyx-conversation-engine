package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	transkafka "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/kafka"
	"github.com/segmentio/kafka-go"
)

// MessagePipelineHandler define o contrato para o pipeline de execução da mensagem.
type MessagePipelineHandler interface {
	HandleMessageReceived(ctx context.Context, evt domain.Event[domain.MessageReceivedPayload]) error
}

// DefaultMessagePipelineHandler é uma implementação padrão para o worker encaminhar a mensagem.
type DefaultMessagePipelineHandler struct{}

func (h *DefaultMessagePipelineHandler) HandleMessageReceived(ctx context.Context, evt domain.Event[domain.MessageReceivedPayload]) error {
	log.Printf("[pipeline] message event received: event_id=%s, tenant=%s, conv=%s, role=%s, content_len=%d",
		evt.EventID, evt.TenantID, evt.ConversationID, evt.Payload.Role, len(evt.Payload.Content))
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

		// Mensagem com schema inválido também é commitada para não bloquear a partição
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

	// Encaminha para o pipeline de execução da conversa
	if err := w.handler.HandleMessageReceived(ctx, evt); err != nil {
		log.Printf("ERROR: pipeline execution failed for event %s: %v", evt.EventID, err)
		// NÃO commitar offset em caso de falha de execução de negócio transitória,
		// permitindo reprocessamento at-least-once pelo grupo.
		return fmt.Errorf("pipeline processing error: %w", err)
	}

	// Efetua commit manual do offset após processamento com sucesso
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
