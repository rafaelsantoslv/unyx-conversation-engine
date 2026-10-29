package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	cwDto "github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot/dto"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/ports"
	transporthttp "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/dto"
)

type WebhookAuthValidator interface {
	ValidateWebhookToken(ctx context.Context, tenantID, channel, token string) (bool, error)
}

type ChatwootWebhookHandler struct {
	ingestionSvc  IngestionService
	authValidator WebhookAuthValidator
}

func NewChatwootWebhookHandler(ingestionSvc IngestionService, authValidator WebhookAuthValidator) *ChatwootWebhookHandler {
	return &ChatwootWebhookHandler{
		ingestionSvc:  ingestionSvc,
		authValidator: authValidator,
	}
}

// HandleWebhook processa requisições POST do Chatwoot no formato /v1/integrations/chatwoot/webhooks/{tenant_id}
func (h *ChatwootWebhookHandler) HandleWebhook(w http.ResponseWriter, r *http.Request) {
	reqID := transporthttp.RequestIDFromContext(r.Context())

	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "", reqID)
		return
	}

	// Extrai tenant_id do path: /v1/integrations/chatwoot/webhooks/{tenant_id}
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	var tenantID string
	if len(pathParts) >= 5 && pathParts[3] == "webhooks" {
		tenantID = pathParts[4]
	}
	if tenantID == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid url", "tenant_id path parameter is required", reqID)
		return
	}

	// 1. Validação de Autenticidade (INV-10): Token por Header ou Query Param
	token := r.Header.Get("X-Chatwoot-Webhook-Token")
	if token == "" {
		token = r.Header.Get("X-Webhook-Token")
	}
	if token == "" {
		token = r.URL.Query().Get("token")
	}

	if h.authValidator != nil {
		valid, err := h.authValidator.ValidateWebhookToken(r.Context(), tenantID, "chatwoot", token)
		if err != nil {
			log.Printf("[webhook] error validating token for tenant %s: %v", tenantID, err)
			writeJSONError(w, http.StatusInternalServerError, "auth check failed", err.Error(), reqID)
			return
		}
		if !valid {
			log.Printf("[webhook] unauthorized webhook access attempt for tenant %s (token mismatch)", tenantID)
			writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid or missing webhook token", reqID)
			return
		}
	}

	// 2. Leitura e Decodificação do Payload
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxPayloadBytes))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "payload too large", "payload exceeds 1MB limit", reqID)
			return
		}
		writeJSONError(w, http.StatusBadRequest, "failed to read body", err.Error(), reqID)
		return
	}

	var cwPayload cwDto.ChatwootWebhookPayload
	if err := json.Unmarshal(body, &cwPayload); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json payload", err.Error(), reqID)
		return
	}

	// 3. Normalização e Anti-Corruption Layer (ACL)
	canonicalEvt, err := cwPayload.ToCanonicalProviderEvent(tenantID)
	if err != nil {
		writeJSONError(w, http.StatusUnprocessableEntity, "mapping error", err.Error(), reqID)
		return
	}

	// Se não for evento de mensagem (ou for irrelevante/ignorado, como echoes de agentes), responde 200 OK sem disparar pipeline
	if canonicalEvt.Type != ports.ProviderEventMessageCreated || canonicalEvt.IncomingMessage == nil {
		log.Printf("[webhook] ignored non-actionable chatwoot event: type=%s, event=%s", canonicalEvt.Type, cwPayload.Event)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ignored","reason":"non_actionable_event"}`))
		return
	}

	// Prevenção de loop/eco de mensagem enviada pelo próprio bot/agente (INV-03)
	if canonicalEvt.IncomingMessage.IsEcho {
		log.Printf("[webhook] echo prevented for msg %s: outgoing/agent message ignored", canonicalEvt.IncomingMessage.ExternalMessageID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ignored","reason":"echo_prevented"}`))
		return
	}

	// 4. Ingestão assíncrona no IngestionService com Transactional Outbox (INV-02)
	ingestReq := dto.IngestMessageRequest{
		TenantID:          canonicalEvt.TenantID,
		ConversationID:    canonicalEvt.IncomingMessage.ExternalConversationID,
		ExternalMessageID: canonicalEvt.IncomingMessage.ExternalMessageID,
		Channel:           string(canonicalEvt.IncomingMessage.Channel),
		Role:              string(canonicalEvt.IncomingMessage.Role),
		Content:           canonicalEvt.IncomingMessage.Content,
	}

	messageID := generateID()
	if h.ingestionSvc != nil {
		if err := h.ingestionSvc.IngestMessage(r.Context(), ingestReq, messageID); err != nil {
			log.Printf("[webhook] error ingesting message: %v", err)
			writeJSONError(w, http.StatusInternalServerError, "failed to ingest message", err.Error(), reqID)
			return
		}
	}

	resp := map[string]interface{}{
		"status":          "accepted",
		"message_id":      messageID,
		"conversation_id": ingestReq.ConversationID,
		"accepted_at":     time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
