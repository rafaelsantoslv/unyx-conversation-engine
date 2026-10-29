package mock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
)

var (
	ErrSimulatedFailure = errors.New("simulated agent runtime failure")
)

// MockRuntime simula o comportamento de um runtime de IA com latência e injeção de falhas configuráveis.
type MockRuntime struct {
	Latency        time.Duration
	ShouldFail     bool
	FailureError   error
	SimulatedReply string
}

// Option define funções de configuração funcional para MockRuntime.
type Option func(*MockRuntime)

// WithLatency define o tempo de espera simulado durante o processamento.
func WithLatency(d time.Duration) Option {
	return func(m *MockRuntime) {
		m.Latency = d
	}
}

// WithFailure configura o mock para retornar falha com um erro específico.
func WithFailure(err error) Option {
	return func(m *MockRuntime) {
		m.ShouldFail = true
		if err != nil {
			m.FailureError = err
		} else {
			m.FailureError = ErrSimulatedFailure
		}
	}
}

// WithSimulatedReply customiza o texto de resposta simulada.
func WithSimulatedReply(reply string) Option {
	return func(m *MockRuntime) {
		m.SimulatedReply = reply
	}
}

// NewMockRuntime instancia um MockRuntime configurado.
func NewMockRuntime(opts ...Option) *MockRuntime {
	m := &MockRuntime{
		Latency:        0,
		ShouldFail:     false,
		FailureError:   ErrSimulatedFailure,
		SimulatedReply: "Olá! Esta é uma resposta automática gerada pelo Mock Runtime.",
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Execute executa a simulação cooperativa, respeitando o context.Context durante a latência.
func (m *MockRuntime) Execute(ctx context.Context, input domain.ExecutionInput) (*domain.ExecutionOutput, error) {
	if m.Latency > 0 {
		select {
		case <-time.After(m.Latency):
			// latência simulada concluída
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	} else if err := ctx.Err(); err != nil {
		return nil, err
	}

	if m.ShouldFail {
		if m.FailureError != nil {
			return nil, m.FailureError
		}
		return nil, ErrSimulatedFailure
	}

	reply := m.SimulatedReply
	if reply == "" {
		reply = fmt.Sprintf("Mock response to message '%s'", input.Content)
	}

	return &domain.ExecutionOutput{
		ResponseContent: reply,
		TokensUsed:      42,
		ExecutedAt:      time.Now().UTC(),
		Metadata: map[string]string{
			"provider": "mock-runtime",
		},
	}, nil
}
