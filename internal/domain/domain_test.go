package domain_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
)

func TestConversation_Validation(t *testing.T) {
	validConv := domain.Conversation{
		ID:                     "conv-123",
		TenantID:               "tenant-abc",
		ExternalConversationID: "ext-conv-999",
		Channel:                "whatsapp",
		State:                  domain.StateAIActive,
	}

	t.Run("valid conversation", func(t *testing.T) {
		if err := validConv.Validate(); err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	})

	t.Run("empty id", func(t *testing.T) {
		c := validConv
		c.ID = ""
		if err := c.Validate(); err != domain.ErrEmptyConversationID {
			t.Fatalf("expected ErrEmptyConversationID, got %v", err)
		}
	})

	t.Run("empty tenant id", func(t *testing.T) {
		c := validConv
		c.TenantID = ""
		if err := c.Validate(); err != domain.ErrEmptyTenantID {
			t.Fatalf("expected ErrEmptyTenantID, got %v", err)
		}
	})

	t.Run("empty external conversation id", func(t *testing.T) {
		c := validConv
		c.ExternalConversationID = ""
		if err := c.Validate(); err != domain.ErrEmptyExternalConversationID {
			t.Fatalf("expected ErrEmptyExternalConversationID, got %v", err)
		}
	})

	t.Run("empty channel", func(t *testing.T) {
		c := validConv
		c.Channel = ""
		if err := c.Validate(); err != domain.ErrEmptyChannel {
			t.Fatalf("expected ErrEmptyChannel, got %v", err)
		}
	})

	t.Run("invalid state", func(t *testing.T) {
		c := validConv
		c.State = "UNKNOWN_STATE"
		if err := c.Validate(); err != domain.ErrInvalidConversationState {
			t.Fatalf("expected ErrInvalidConversationState, got %v", err)
		}
	})
}

func TestConversation_JSONSerialization(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond).UTC()
	original := domain.Conversation{
		ID:                     "conv-1",
		TenantID:               "tenant-1",
		UserID:                 "user-42",
		ExternalConversationID: "ext-1",
		Channel:                "whatsapp",
		State:                  domain.StateHumanActive,
		Metadata: map[string]interface{}{
			"agent_id": "agent-42",
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal conversation: %v", err)
	}

	var restored domain.Conversation
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("failed to unmarshal conversation: %v", err)
	}

	if restored.ID != original.ID || restored.State != domain.StateHumanActive {
		t.Errorf("unmarshaled values differ: %+v", restored)
	}
	if restored.Metadata["agent_id"] != "agent-42" {
		t.Errorf("metadata not preserved, got %v", restored.Metadata)
	}
}

func TestMessage_Validation(t *testing.T) {
	validMsg := domain.Message{
		ID:                "msg-1",
		TenantID:          "tenant-1",
		ConversationID:    "conv-1",
		ExternalMessageID: "wpp-msg-123",
		Role:              domain.RoleUser,
		Content:           "Olá, preciso de ajuda",
		SequenceNumber:    1,
	}

	t.Run("valid message", func(t *testing.T) {
		if err := validMsg.Validate(); err != nil {
			t.Fatalf("expected valid message, got: %v", err)
		}
	})

	t.Run("empty message id", func(t *testing.T) {
		m := validMsg
		m.ID = ""
		if err := m.Validate(); err != domain.ErrEmptyMessageID {
			t.Fatalf("expected ErrEmptyMessageID, got %v", err)
		}
	})

	t.Run("empty tenant id", func(t *testing.T) {
		m := validMsg
		m.TenantID = ""
		if err := m.Validate(); err != domain.ErrEmptyTenantID {
			t.Fatalf("expected ErrEmptyTenantID, got %v", err)
		}
	})

	t.Run("empty conversation id", func(t *testing.T) {
		m := validMsg
		m.ConversationID = ""
		if err := m.Validate(); err != domain.ErrEmptyConversationRef {
			t.Fatalf("expected ErrEmptyConversationRef, got %v", err)
		}
	})

	t.Run("invalid role", func(t *testing.T) {
		m := validMsg
		m.Role = "bot"
		if err := m.Validate(); err != domain.ErrInvalidMessageRole {
			t.Fatalf("expected ErrInvalidMessageRole, got %v", err)
		}
	})

	t.Run("empty content", func(t *testing.T) {
		m := validMsg
		m.Content = "   "
		if err := m.Validate(); err != domain.ErrEmptyMessageContent {
			t.Fatalf("expected ErrEmptyMessageContent, got %v", err)
		}
	})

	t.Run("negative sequence number", func(t *testing.T) {
		m := validMsg
		m.SequenceNumber = -1
		if err := m.Validate(); err != domain.ErrInvalidSequenceNumber {
			t.Fatalf("expected ErrInvalidSequenceNumber, got %v", err)
		}
	})
}

func TestEventEnvelope_JSON(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond).UTC()

	payload := domain.MessageReceivedPayload{
		MessageID:         "msg-123",
		ExternalMessageID: "ext-msg-123",
		Channel:           "whatsapp",
		Role:              domain.RoleUser,
		Content:           "Gostaria de saber o preço",
		SequenceNumber:    2,
	}

	evt := domain.Event[domain.MessageReceivedPayload]{
		EventID:        "evt-001",
		TenantID:       "tenant-1",
		ConversationID: "conv-1",
		EventType:      domain.EventTypeMessageReceived,
		Version:        domain.EventVersionV1,
		OccurredAt:     now,
		Payload:        payload,
	}

	if err := evt.Validate(); err != nil {
		t.Fatalf("envelope validation failed: %v", err)
	}
	if err := evt.Payload.Validate(); err != nil {
		t.Fatalf("payload validation failed: %v", err)
	}

	bytes, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	var deserialized domain.Event[domain.MessageReceivedPayload]
	if err := json.Unmarshal(bytes, &deserialized); err != nil {
		t.Fatalf("failed to unmarshal event: %v", err)
	}

	if deserialized.EventID != evt.EventID {
		t.Errorf("expected event id %s, got %s", evt.EventID, deserialized.EventID)
	}
	if deserialized.Payload.Content != "Gostaria de saber o preço" {
		t.Errorf("expected payload content preserved, got %s", deserialized.Payload.Content)
	}
	if deserialized.EventType != domain.EventTypeMessageReceived {
		t.Errorf("expected event type preserved, got %s", deserialized.EventType)
	}
}
