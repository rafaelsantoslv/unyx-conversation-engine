package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer define a interface para envio de mensagens ao Kafka.
type Producer interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
	Close() error
}

// KafkaPublisher implementa a interface Producer usando segmentio/kafka-go Writer.
type KafkaPublisher struct {
	writer *kafka.Writer
}

// ProducerConfig contém configurações do produtor Kafka.
type ProducerConfig struct {
	Brokers      []string
	Topic        string
	BatchTimeout time.Duration
	WriteTimeout time.Duration
}

// NewKafkaPublisher cria uma nova instância de KafkaPublisher com garantias de confiabilidade (acks=all).
func NewKafkaPublisher(brokers []string) *KafkaPublisher {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, // acks=all (-1) para garantir replicação durável em todos os ISRs
		Async:        false,            // entrega síncrona por mensagem/lote garantindo que retorno de WriteMessages confirma ACK do broker
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		MaxAttempts:  5,
		AllowAutoTopicCreation: true,
	}

	return &KafkaPublisher{
		writer: writer,
	}
}

// Publish publica uma mensagem com chave de partição no tópico indicado.
func (p *KafkaPublisher) Publish(ctx context.Context, topic, key string, payload []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
		Time:  time.Now().UTC(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to publish message to topic %s: %w", topic, err)
	}

	return nil
}

// Close fecha o writer e drena conexões pendentes.
func (p *KafkaPublisher) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}
