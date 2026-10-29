package chatwoot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	cwDto "github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot/dto"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/ports"
)

var (
	ErrRateLimited     = errors.New("chatwoot api rate limited")
	ErrServiceOutage   = errors.New("chatwoot api service unavailable")
	ErrChatwootClient  = errors.New("chatwoot client error")
)

type OutboundHTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// ChatwootOutboundSender despacha respostas geradas pela IA para a API do Chatwoot (INV-04, INV-06).
type ChatwootOutboundSender struct {
	httpClient OutboundHTTPClient
}

func NewChatwootOutboundSender(client OutboundHTTPClient) *ChatwootOutboundSender {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &ChatwootOutboundSender{httpClient: client}
}

type SendMessageParams struct {
	BaseURL           string
	APIAccessToken    string
	AccountID         string
	ConversationID    string
	Content           string
	Private           bool
	EchoID            string
	CorrelationID     string
	ContentAttributes map[string]interface{}
}

// SendMessage realiza a chamada POST /api/v1/accounts/{account_id}/conversations/{conversation_id}/messages
func (s *ChatwootOutboundSender) SendMessage(ctx context.Context, params SendMessageParams) (*ports.DeliveryReceipt, error) {
	if params.BaseURL == "" {
		return nil, errors.New("chatwoot baseURL is required")
	}
	if params.APIAccessToken == "" {
		return nil, errors.New("chatwoot apiAccessToken is required")
	}
	if params.AccountID == "" || params.ConversationID == "" {
		return nil, errors.New("accountID and conversationID are required")
	}

	endpoint := fmt.Sprintf("%s/api/v1/accounts/%s/conversations/%s/messages",
		strings.TrimRight(params.BaseURL, "/"), params.AccountID, params.ConversationID)

	contentAttrs := make(map[string]interface{})
	if params.ContentAttributes != nil {
		for k, v := range params.ContentAttributes {
			contentAttrs[k] = v
		}
	}
	if params.CorrelationID != "" {
		contentAttrs["correlation_id"] = params.CorrelationID
	}

	reqBody := cwDto.ChatwootSendRequestDTO{
		Content:           params.Content,
		MessageType:       "outgoing",
		Private:           params.Private,
		EchoID:            params.EchoID,
		ContentAttributes: contentAttrs,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("api_access_token", params.APIAccessToken)

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("network error sending message to chatwoot: %w", err)
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))

	// 1. Sucesso HTTP 200/201
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		var cwResp cwDto.ChatwootSendResponseDTO
		if err := json.Unmarshal(respBytes, &cwResp); err != nil {
			return nil, fmt.Errorf("failed to parse chatwoot response: %w", err)
		}

		return &ports.DeliveryReceipt{
			ExternalMessageID: fmt.Sprintf("%d", cwResp.ID),
			DeliveredAt:       time.Now().UTC(),
			Status:            "SENT",
			ResponsePayload:   respBytes,
		}, nil
	}

	// 2. HTTP 429 - Rate Limit
	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfterStr := resp.Header.Get("Retry-After")
		retrySec, _ := strconv.Atoi(retryAfterStr)
		if retrySec <= 0 {
			retrySec = 5
		}
		return nil, fmt.Errorf("%w: retry after %d seconds (status %d)", ErrRateLimited, retrySec, resp.StatusCode)
	}

	// 3. HTTP 5xx - Instabilidade Temporária
	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("%w: status %d: %s", ErrServiceOutage, resp.StatusCode, string(respBytes))
	}

	// 4. HTTP 4xx - Erros Permanentes de Cliente
	return nil, fmt.Errorf("%w: status %d: %s", ErrChatwootClient, resp.StatusCode, string(respBytes))
}

// ReconcileAmbiguousIntent consulta o histórico recente da conversa no Chatwoot para verificar se
// uma mensagem enviada com timeout/ambiguidade realmente foi persistida no canal (INV-08).
func (s *ChatwootOutboundSender) ReconcileAmbiguousIntent(
	ctx context.Context,
	baseURL, apiAccessToken, accountID, conversationID string,
	expectedEchoID, expectedCorrelationID, expectedContent string,
) (*ports.DeliveryReceipt, error) {
	if baseURL == "" || apiAccessToken == "" || accountID == "" || conversationID == "" {
		return nil, errors.New("missing parameters for ambiguous intent reconciliation")
	}

	endpoint := fmt.Sprintf("%s/api/v1/accounts/%s/conversations/%s/messages",
		strings.TrimRight(baseURL, "/"), accountID, conversationID)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build reconciliation request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("api_access_token", apiAccessToken)

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("network error during reconciliation: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
		return nil, fmt.Errorf("chatwoot messages list returned non-200 (%d): %s", resp.StatusCode, string(respBytes))
	}

	var listResp cwDto.ChatwootMessagesListResponseDTO
	if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
		return nil, fmt.Errorf("failed to decode chatwoot messages list: %w", err)
	}

	// Varre as mensagens da conversa buscando correspondência por echo_id, correlation_id ou conteúdo exato
	for _, msg := range listResp.Payload {
		// 1. Match por echo_id
		if expectedEchoID != "" && msg.EchoID == expectedEchoID {
			return &ports.DeliveryReceipt{
				ExternalMessageID: fmt.Sprintf("%d", msg.ID),
				DeliveredAt:       time.Unix(msg.CreatedAt, 0).UTC(),
				Status:            "SENT",
			}, nil
		}

		// 2. Match por correlation_id em content_attributes
		if expectedCorrelationID != "" && msg.ContentAttributes != nil {
			if cID, ok := msg.ContentAttributes["correlation_id"].(string); ok && cID == expectedCorrelationID {
				return &ports.DeliveryReceipt{
					ExternalMessageID: fmt.Sprintf("%d", msg.ID),
					DeliveredAt:       time.Unix(msg.CreatedAt, 0).UTC(),
					Status:            "SENT",
				}, nil
			}
		}

		// 3. Fallback: match por conteúdo idêntico recente
		if expectedContent != "" && strings.TrimSpace(msg.Content) == strings.TrimSpace(expectedContent) {
			return &ports.DeliveryReceipt{
				ExternalMessageID: fmt.Sprintf("%d", msg.ID),
				DeliveredAt:       time.Unix(msg.CreatedAt, 0).UTC(),
				Status:            "SENT",
			}, nil
		}
	}

	// Não encontrada no histórico: mensagem realmente não foi recebida pelo Chatwoot
	return nil, nil
}

