package domain_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
)

func TestConversationLifecycle_StateTransitions(t *testing.T) {
	// =========================================================================
	// MATRIZ DE TRANSIÇÕES VÁLIDAS
	// =========================================================================
	t.Run("valid transitions matrix", func(t *testing.T) {
		validCases := []struct {
			name string
			from domain.ConversationState
			to   domain.ConversationState
		}{
			{"AI_ACTIVE -> HUMAN_ACTIVE (handoff)", domain.StateAIActive, domain.StateHumanActive},
			{"AI_ACTIVE -> CLOSED (resolved by AI)", domain.StateAIActive, domain.StateClosed},
			{"HUMAN_ACTIVE -> AI_ACTIVE (returned to AI)", domain.StateHumanActive, domain.StateAIActive},
			{"HUMAN_ACTIVE -> CLOSED (resolved by human)", domain.StateHumanActive, domain.StateClosed},
			{"CLOSED -> AI_ACTIVE (customer reopens)", domain.StateClosed, domain.StateAIActive},
			{"CLOSED -> HUMAN_ACTIVE (reopens directly to operator)", domain.StateClosed, domain.StateHumanActive},
			{"Idempotent AI_ACTIVE -> AI_ACTIVE", domain.StateAIActive, domain.StateAIActive},
			{"Idempotent HUMAN_ACTIVE -> HUMAN_ACTIVE", domain.StateHumanActive, domain.StateHumanActive},
			{"Idempotent CLOSED -> CLOSED", domain.StateClosed, domain.StateClosed},
		}

		for _, tc := range validCases {
			t.Run(tc.name, func(t *testing.T) {
				if !domain.CanTransition(tc.from, tc.to) {
					t.Errorf("expected transition from %s to %s to be valid", tc.from, tc.to)
				}

				conv := &domain.Conversation{
					ID:      "conv-test",
					State:   tc.from,
					Version: 1,
				}
				err := conv.TransitionTo(tc.to)
				if err != nil {
					t.Fatalf("expected successful transition, got: %v", err)
				}
				if conv.State != tc.to {
					t.Errorf("expected state %s, got %s", tc.to, conv.State)
				}
				if tc.from != tc.to && conv.Version != 2 {
					t.Errorf("expected version to be incremented to 2, got %d", conv.Version)
				}
			})
		}
	})

	// =========================================================================
	// MATRIZ DE TRANSIÇÕES INVÁLIDAS
	// =========================================================================
	t.Run("invalid transitions matrix", func(t *testing.T) {
		invalidCases := []struct {
			name string
			from domain.ConversationState
			to   domain.ConversationState
		}{
			{"Unknown state -> AI_ACTIVE", domain.ConversationState("UNKNOWN"), domain.StateAIActive},
			{"AI_ACTIVE -> Unknown state", domain.StateAIActive, domain.ConversationState("UNKNOWN")},
		}

		for _, tc := range invalidCases {
			t.Run(tc.name, func(t *testing.T) {
				if domain.CanTransition(tc.from, tc.to) {
					t.Errorf("expected transition from %s to %s to be INVALID", tc.from, tc.to)
				}

				conv := &domain.Conversation{
					ID:      "conv-test",
					State:   tc.from,
					Version: 1,
				}
				err := conv.TransitionTo(tc.to)
				if err == nil {
					t.Fatalf("expected error for invalid transition from %s to %s", tc.from, tc.to)
				}
				if !errors.Is(err, domain.ErrInvalidStateTransition) {
					t.Errorf("expected ErrInvalidStateTransition, got: %v", err)
				}
			})
		}
	})

	// =========================================================================
	// TESTE DE GUARDA: AllowsAIProcessing
	// =========================================================================
	t.Run("AllowsAIProcessing guard", func(t *testing.T) {
		aiConv := &domain.Conversation{State: domain.StateAIActive}
		if !aiConv.AllowsAIProcessing() {
			t.Errorf("AI_ACTIVE must allow AI processing")
		}

		humanConv := &domain.Conversation{State: domain.StateHumanActive}
		if humanConv.AllowsAIProcessing() {
			t.Errorf("HUMAN_ACTIVE must NOT allow AI processing (protect human operator)")
		}

		closedConv := &domain.Conversation{State: domain.StateClosed}
		if closedConv.AllowsAIProcessing() {
			t.Errorf("CLOSED must NOT allow AI processing")
		}
	})
}

func TestConversationPolicyResolver_Unit(t *testing.T) {
	ctx := context.Background()
	resolver := domain.NewDefaultConversationPolicyResolver()

	t.Run("resolve default policy", func(t *testing.T) {
		policy, err := resolver.ResolvePolicy(ctx, "tenant-1", "whatsapp")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !policy.AllowAIProcessing {
			t.Errorf("expected AllowAIProcessing to be true")
		}
		if policy.InactivityTimeout != 24*time.Hour {
			t.Errorf("expected 24h inactivity timeout, got %v", policy.InactivityTimeout)
		}
	})

	t.Run("CanProcessWithAI respects state", func(t *testing.T) {
		canAI, err := resolver.CanProcessWithAI(ctx, &domain.Conversation{State: domain.StateAIActive})
		if err != nil || !canAI {
			t.Errorf("expected true for AI_ACTIVE, got %v (err: %v)", canAI, err)
		}

		canHuman, err := resolver.CanProcessWithAI(ctx, &domain.Conversation{State: domain.StateHumanActive})
		if err != nil || canHuman {
			t.Errorf("expected false for HUMAN_ACTIVE, got %v (err: %v)", canHuman, err)
		}

		canNil, err := resolver.CanProcessWithAI(ctx, nil)
		if err != nil || canNil {
			t.Errorf("expected false for nil conversation, got %v", canNil)
		}
	})
}
