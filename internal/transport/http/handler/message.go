package handler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	transporthttp "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/dto"
)

const maxPayloadBytes = 1024 * 1024 // 1 MB

type IngestionService interface {
	IngestMessage(ctx context.Context, req dto.IngestMessageRequest, messageID string) error
}

type MessageHandler struct {
	service IngestionService
}

func NewMessageHandler(service IngestionService) *MessageHandler {
	return &MessageHandler{service: service}
}

func (h *MessageHandler) HandleIngest(w http.ResponseWriter, r *http.Request) {
	reqID := transporthttp.RequestIDFromContext(r.Context())

	// 1. Apenas POST permitido
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "", reqID)
		return
	}

	// 2. Proteção contra payloads gigantes (MaxBytesReader)
	r.Body = http.MaxBytesReader(w, r.Body, maxPayloadBytes)

	// 3. Desserialização estrita
	var req dto.IngestMessageRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "payload too large", "payload exceeds 1MB limit", reqID)
			return
		}
		writeJSONError(w, http.StatusBadRequest, "invalid json payload", err.Error(), reqID)
		return
	}

	// 4. Fail-fast de validação dos campos
	if err := req.Validate(); err != nil {
		writeJSONError(w, http.StatusUnprocessableEntity, "validation failed", err.Error(), reqID)
		return
	}

	// 5. Geração do message_id e delegação assíncrona
	messageID := generateID()
	if h.service != nil {
		if err := h.service.IngestMessage(r.Context(), req, messageID); err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to process message", err.Error(), reqID)
			return
		}
	}

	// 6. Resposta 202 Accepted imediata
	resp := dto.IngestMessageResponse{
		Status:         "accepted",
		MessageID:      messageID,
		ConversationID: req.ConversationID,
		AcceptedAt:     time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(resp)
}

func writeJSONError(w http.ResponseWriter, status int, errMessage, detail, reqID string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(dto.ErrorResponse{
		Error:     errMessage,
		Detail:    detail,
		RequestID: reqID,
	})
}

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
