package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/kafka"
)

// OutboxRepository define os métodos necessários para manipular os eventos da outbox.
type OutboxRepository interface {
	FetchPendingOutboxEvents(ctx context.Context, limit int) ([]*domain.OutboxEvent, error)
	MarkOutboxEventPublished(ctx context.Context, id string) error
}

// OutboxPublisherService orquestra o polling de eventos pendentes e envio para o Kafka.
type OutboxPublisherService struct {
	repo         OutboxRepository
	producer     kafka.Producer
	defaultTopic string
	batchSize    int
	pollInterval time.Duration
}

// OutboxPublisherOption define funções para configurar o serviço.
type OutboxPublisherOption func(*OutboxPublisherService)

// WithBatchSize define o tamanho do lote de leitura.
func WithBatchSize(size int) OutboxPublisherOption {
	return func(s *OutboxPublisherService) {
		if size > 0 {
			s.batchSize = size
		}
	}
}

// WithPollInterval define o intervalo de polling quando não há novos eventos.
func WithPollInterval(interval time.Duration) OutboxPublisherOption {
	return func(s *OutboxPublisherService) {
		if interval > 0 {
			s.pollInterval = interval
		}
	}
}

// WithDefaultTopic define o tópico padrão Kafka para onde os eventos de conversação são enviados.
func WithDefaultTopic(topic string) OutboxPublisherOption {
	return func(s *OutboxPublisherService) {
		if topic != "" {
			s.defaultTopic = topic
		}
	}
}

// NewOutboxPublisherService instancia o serviço de publicação.
func NewOutboxPublisherService(repo OutboxRepository, producer kafka.Producer, opts ...OutboxPublisherOption) *OutboxPublisherService {
	s := &OutboxPublisherService{
		repo:         repo,
		producer:     producer,
		defaultTopic: "conversation.events",
		batchSize:    50,
		pollInterval: 100 * time.Millisecond,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// ProcessBatch busca um lote de eventos pendentes e publica cada um no Kafka.
// Retorna a quantidade de eventos processados com sucesso e eventuais erros.
func (s *OutboxPublisherService) ProcessBatch(ctx context.Context) (int, error) {
	events, err := s.repo.FetchPendingOutboxEvents(ctx, s.batchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch pending outbox events: %w", err)
	}

	if len(events) == 0 {
		return 0, nil
	}

	processedCount := 0
	for _, evt := range events {
		if ctx.Err() != nil {
			return processedCount, ctx.Err()
		}

		// A chave de partição garante que eventos da mesma conversa vão para a mesma partição no Kafka
		partitionKey := evt.PartitionKey
		if partitionKey == "" {
			partitionKey = fmt.Sprintf("%s:%s", evt.TenantID, evt.ConversationID)
		}

		// Publica no Kafka garantindo acks=all
		if err := s.producer.Publish(ctx, s.defaultTopic, partitionKey, evt.Payload); err != nil {
			// Em caso de erro temporário no Kafka, logamos e paramos o lote para tentar novamente no próximo ciclo
			// sem marcar o evento como PUBLISHED (garantia at-least-once).
			return processedCount, fmt.Errorf("failed to publish outbox event id=%s: %w", evt.ID, err)
		}

		// Marca status como PUBLISHED no banco
		if err := s.repo.MarkOutboxEventPublished(ctx, evt.ID); err != nil {
			log.Printf("warning: failed to mark outbox event %s as PUBLISHED: %v", evt.ID, err)
			return processedCount, fmt.Errorf("failed to update outbox event status id=%s: %w", evt.ID, err)
		}

		processedCount++
	}

	return processedCount, nil
}

// Run inicia o loop contínuo de drenagem da outbox até que o contexto seja cancelado.
func (s *OutboxPublisherService) Run(ctx context.Context) error {
	log.Printf("outbox publisher loop started (topic: %s, batchSize: %d, pollInterval: %v)",
		s.defaultTopic, s.batchSize, s.pollInterval)

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("outbox publisher loop received shutdown signal")
			return ctx.Err()
		case <-ticker.C:
			count, err := s.ProcessBatch(ctx)
			if err != nil {
				log.Printf("error in outbox publisher batch processing: %v", err)
				continue
			}

			// Se processamos um lote cheio, executamos a próxima iteração imediatamente para drenar backlog
			if count >= s.batchSize {
				for count >= s.batchSize && ctx.Err() == nil {
					count, err = s.ProcessBatch(ctx)
					if err != nil {
						log.Printf("error in continuous backlog draining: %v", err)
						break
					}
				}
			}
		}
	}
}
