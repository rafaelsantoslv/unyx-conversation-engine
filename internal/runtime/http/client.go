package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
)

var (
	ErrRuntimeInvalidResponse = errors.New("invalid or malformed response from agent runtime")
	ErrRuntimePermanent       = errors.New("agent runtime permanent execution failure")
	ErrRuntimeTimeout         = errors.New("agent runtime request timeout")
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// HttpAgentRuntime implementa a interface domain.AgentRuntime comunicando com o serviço externo via HTTP REST.
type HttpAgentRuntime struct {
	baseURL    string
	httpClient HTTPClient
	timeout    time.Duration
}

type ClientOption func(*HttpAgentRuntime)

func WithHTTPClient(client HTTPClient) ClientOption {
	return func(r *HttpAgentRuntime) {
		if client != nil {
			r.httpClient = client
		}
	}
}

func WithTimeout(timeout time.Duration) ClientOption {
	return func(r *HttpAgentRuntime) {
		if timeout > 0 {
			r.timeout = timeout
		}
	}
}

func NewHttpAgentRuntime(baseURL string, opts ...ClientOption) *HttpAgentRuntime {
	baseURL = strings.TrimRight(baseURL, "/")
	r := &HttpAgentRuntime{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		timeout: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Execute dispara a requisição POST /v1/agents/execute propagando contexto e metadados.
func (r *HttpAgentRuntime) Execute(ctx context.Context, input domain.ExecutionInput) (*domain.ExecutionOutput, error) {
	endpoint := fmt.Sprintf("%s/v1/agents/execute", r.baseURL)

	payloadBytes, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal execution input: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to build http request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Tenant-ID", input.TenantID)
	req.Header.Set("X-Conversation-ID", input.ConversationID)
	req.Header.Set("X-Message-ID", input.MessageID)
	if input.ExecutionRequestID != "" {
		req.Header.Set("X-Correlation-ID", input.ExecutionRequestID)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ErrRuntimeTimeout
		}
		// Erros de conexão de rede são falhas transitórias
		return nil, fmt.Errorf("agent runtime transport error: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// 1. Tratamento de Sucesso
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var out domain.ExecutionOutput
		if err := json.Unmarshal(bodyBytes, &out); err != nil {
			return nil, fmt.Errorf("%w: failed to parse response: %v", ErrRuntimeInvalidResponse, err)
		}
		if out.ExecutedAt.IsZero() {
			out.ExecutedAt = time.Now().UTC()
		}
		return &out, nil
	}

	// 2. Classificação de Falhas:
	// A. Falhas Transitórias (502, 503, 504, 429) -> Retornam erro com mensagem que aciona retry
	if resp.StatusCode == http.StatusBadGateway ||
		resp.StatusCode == http.StatusServiceUnavailable ||
		resp.StatusCode == http.StatusGatewayTimeout ||
		resp.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("temporary failure: http status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// B. Falhas Permanentes (400 Bad Request, 401, 403, 422) -> Levam a quarentena
	return nil, fmt.Errorf("%w: status %d: %s", ErrRuntimePermanent, resp.StatusCode, string(bodyBytes))
}
