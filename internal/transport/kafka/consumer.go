package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Consumer define o contrato para leitura e commit manual de offsets.
type Consumer interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// KafkaConsumer implementa Consumer utilizando kafka.Reader configurado para commit manual.
type KafkaConsumer struct {
	reader *kafka.Reader
}

// ConsumerConfig opções de inicialização do consumidor Kafka.
type ConsumerConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string
	MinBytes int
	MaxBytes int
	MaxWait  time.Duration
}

// NewKafkaConsumer cria uma nova instância de KafkaConsumer com commit manual e consumer group.
func NewKafkaConsumer(cfg ConsumerConfig) *KafkaConsumer {
	minBytes := cfg.MinBytes
	if minBytes <= 0 {
		minBytes = 10e3 // 10KB
	}
	maxBytes := cfg.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 10e6 // 10MB
	}
	maxWait := cfg.MaxWait
	if maxWait <= 0 {
		maxWait = 500 * time.Millisecond
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,
		MaxWait:        maxWait,
		CommitInterval: 0, // 0 = desativa auto-commit periódico do reader; commits serão manuais e explícitos via CommitMessages
		StartOffset:    kafka.FirstOffset,
	})

	return &KafkaConsumer{
		reader: reader,
	}
}

// FetchMessage busca uma mensagem do Kafka sem commitar automaticamente o offset.
func (c *KafkaConsumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("failed to fetch message: %w", err)
	}
	return msg, nil
}

// CommitMessages efetua o commit manual dos offsets das mensagens fornecidas.
func (c *KafkaConsumer) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if err := c.reader.CommitMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("failed to commit messages: %w", err)
	}
	return nil
}

// Close fecha o reader e encerra as conexões ativas com o Kafka.
func (c *KafkaConsumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}
