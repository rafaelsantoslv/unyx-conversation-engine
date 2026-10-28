package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	transporthttp "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/dto"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/handler"
)

type mockIngestionService struct {
	called bool
	req    dto.IngestMessageRequest
	msgID  string
	err    error
}

func (m *mockIngestionService) IngestMessage(ctx context.Context, req dto.IngestMessageRequest, msgID string) error {
	m.called = true
	m.req = req
	m.msgID = msgID
	return m.err
}

func TestMessageHandler_HandleIngest(t *testing.T) {
	validBody := `{
		"tenant_id": "tenant-123",
		"conversation_id": "conv-456",
		"external_message_id": "wpp-789",
		"channel": "whatsapp",
		"role": "user",
		"content": "Olá, preciso de ajuda!"
	}`

	t.Run("returns 202 Accepted on valid payload and calls service", func(t *testing.T) {
		mockSvc := &mockIngestionService{}
		h := handler.NewMessageHandler(mockSvc)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewBufferString(validBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		transporthttp.RequestIDMiddleware(http.HandlerFunc(h.HandleIngest)).ServeHTTP(w, req)

		if w.Code != http.StatusAccepted {
			t.Fatalf("expected status 202 Accepted, got %d. Body: %s", w.Code, w.Body.String())
		}

		if w.Header().Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", w.Header().Get("Content-Type"))
		}

		if w.Header().Get("X-Request-ID") == "" {
			t.Errorf("expected X-Request-ID header in response")
		}

		var resp dto.IngestMessageResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if resp.Status != "accepted" {
			t.Errorf("expected status 'accepted', got %s", resp.Status)
		}
		if resp.MessageID == "" {
			t.Errorf("expected generated message_id")
		}
		if resp.ConversationID != "conv-456" {
			t.Errorf("expected conversation_id 'conv-456', got %s", resp.ConversationID)
		}
		if resp.AcceptedAt.IsZero() {
			t.Errorf("expected non-zero AcceptedAt")
		}

		if !mockSvc.called {
			t.Errorf("expected service IngestMessage to be called")
		}
		if mockSvc.msgID != resp.MessageID {
			t.Errorf("expected service called with messageID %s, got %s", resp.MessageID, mockSvc.msgID)
		}
	})

	t.Run("returns 405 Method Not Allowed when method is not POST", func(t *testing.T) {
		h := handler.NewMessageHandler(&mockIngestionService{})

		req := httptest.NewRequest(http.MethodGet, "/v1/messages", nil)
		w := httptest.NewRecorder()

		h.HandleIngest(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405 Method Not Allowed, got %d", w.Code)
		}
	})

	t.Run("returns 400 Bad Request on malformed JSON", func(t *testing.T) {
		h := handler.NewMessageHandler(&mockIngestionService{})

		req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewBufferString("{invalid-json"))
		w := httptest.NewRecorder()

		h.HandleIngest(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got %d", w.Code)
		}
	})

	t.Run("returns 400 Bad Request on unknown fields", func(t *testing.T) {
		h := handler.NewMessageHandler(&mockIngestionService{})

		bodyWithUnknown := `{
			"tenant_id": "tenant-123",
			"conversation_id": "conv-456",
			"external_message_id": "wpp-789",
			"channel": "whatsapp",
			"role": "user",
			"content": "Olá!",
			"unknown_field": "exploit"
		}`

		req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewBufferString(bodyWithUnknown))
		w := httptest.NewRecorder()

		h.HandleIngest(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request on unknown field, got %d", w.Code)
		}
	})

	t.Run("returns 422 Unprocessable Entity when required fields are missing", func(t *testing.T) {
		h := handler.NewMessageHandler(&mockIngestionService{})

		tests := []struct {
			name string
			body string
		}{
			{
				name: "missing tenant_id",
				body: `{"conversation_id":"c","external_message_id":"e","channel":"w","role":"user","content":"oi"}`,
			},
			{
				name: "missing conversation_id",
				body: `{"tenant_id":"t","external_message_id":"e","channel":"w","role":"user","content":"oi"}`,
			},
			{
				name: "missing external_message_id",
				body: `{"tenant_id":"t","conversation_id":"c","channel":"w","role":"user","content":"oi"}`,
			},
			{
				name: "missing channel",
				body: `{"tenant_id":"t","conversation_id":"c","external_message_id":"e","role":"user","content":"oi"}`,
			},
			{
				name: "invalid role",
				body: `{"tenant_id":"t","conversation_id":"c","external_message_id":"e","channel":"w","role":"robot","content":"oi"}`,
			},
			{
				name: "missing content",
				body: `{"tenant_id":"t","conversation_id":"c","external_message_id":"e","channel":"w","role":"user","content":"   "}`,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewBufferString(tc.body))
				w := httptest.NewRecorder()

				h.HandleIngest(w, req)

				if w.Code != http.StatusUnprocessableEntity {
					t.Fatalf("[%s] expected 422 Unprocessable Entity, got %d. Body: %s", tc.name, w.Code, w.Body.String())
				}
			})
		}
	})

	t.Run("returns 413 Payload Too Large when payload exceeds 1MB", func(t *testing.T) {
		h := handler.NewMessageHandler(&mockIngestionService{})

		largeContent := strings.Repeat("A", 1024*1024+100)
		body := `{"tenant_id":"t","conversation_id":"c","external_message_id":"e","channel":"w","role":"user","content":"` + largeContent + `"}`

		req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewBufferString(body))
		w := httptest.NewRecorder()

		h.HandleIngest(w, req)

		if w.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected 413 Payload Too Large, got %d", w.Code)
		}
	})

	t.Run("returns 500 Internal Server Error when service fails", func(t *testing.T) {
		mockSvc := &mockIngestionService{err: errors.New("failed to enqueue")}
		h := handler.NewMessageHandler(mockSvc)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewBufferString(validBody))
		w := httptest.NewRecorder()

		h.HandleIngest(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500 Internal Server Error, got %d", w.Code)
		}
	})
}
