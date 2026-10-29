package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConversationFSM_TransitionsAndAuditHistory(t *testing.T) {
	ctx := context.Background()
	pool := setupTestDB(t)
	defer pool.Close()

	repo := postgres.NewRepository(pool)
	tenantID := "tenant-fsm-test"

	// 1. Cria conversa (inicia em AI_ACTIVE, version 1)
	conv, err := repo.GetOrCreateConversation(ctx, tenantID, "ext-fsm-"+uuid.New().String())
	require.NoError(t, err)
	assert.Equal(t, domain.StateAIActive, conv.State)
	assert.Equal(t, int64(1), conv.Version)

	// 2. Transição válida: AI_ACTIVE -> HUMAN_ACTIVE (handoff por operador)
	trans1, err := repo.TransitionConversationState(
		ctx, tenantID, conv.ID, domain.StateHumanActive, 1, "Customer requested human agent", "operator-ana",
	)
	require.NoError(t, err)
	assert.Equal(t, domain.StateHumanActive, trans1.State)
	assert.Equal(t, int64(2), trans1.Version)

	// 3. Conflito de versão otimista: Tentar transição fornecendo versão 1 (obsoleta)
	_, err = repo.TransitionConversationState(
		ctx, tenantID, conv.ID, domain.StateClosed, 1, "Attempting transition with stale version", "system",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrStateVersionMismatch)

	// 4. Transição válida: HUMAN_ACTIVE -> CLOSED (resolvido por operador)
	trans2, err := repo.TransitionConversationState(
		ctx, tenantID, conv.ID, domain.StateClosed, 2, "Issue resolved by operator", "operator-ana",
	)
	require.NoError(t, err)
	assert.Equal(t, domain.StateClosed, trans2.State)
	assert.Equal(t, int64(3), trans2.Version)

	// 5. Transição válida de reabertura: CLOSED -> AI_ACTIVE (cliente mandou mensagem nova)
	trans3, err := repo.TransitionConversationState(
		ctx, tenantID, conv.ID, domain.StateAIActive, 3, "New message received after closure", "client",
	)
	require.NoError(t, err)
	assert.Equal(t, domain.StateAIActive, trans3.State)
	assert.Equal(t, int64(4), trans3.Version)

	// 6. Transição inválida: Estado desconhecido
	_, err = repo.TransitionConversationState(
		ctx, tenantID, conv.ID, domain.ConversationState("INVALID_STATE"), 4, "Illegal transition", "hacker",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidConversationState)

	// 7. Auditoria Completa: Inspeciona conversation_state_history
	history, err := repo.GetConversationStateHistory(ctx, tenantID, conv.ID)
	require.NoError(t, err)
	require.Len(t, history, 3, "must record exactly 3 state transitions in audit history")

	assert.Equal(t, domain.StateAIActive, history[0].FromState)
	assert.Equal(t, domain.StateHumanActive, history[0].ToState)
	assert.Equal(t, int64(1), history[0].FromVersion)
	assert.Equal(t, int64(2), history[0].ToVersion)
	assert.Equal(t, "operator-ana", history[0].ChangedBy)

	assert.Equal(t, domain.StateHumanActive, history[1].FromState)
	assert.Equal(t, domain.StateClosed, history[1].ToState)
	assert.Equal(t, int64(2), history[1].FromVersion)
	assert.Equal(t, int64(3), history[1].ToVersion)

	assert.Equal(t, domain.StateClosed, history[2].FromState)
	assert.Equal(t, domain.StateAIActive, history[2].ToState)
	assert.Equal(t, int64(3), history[2].FromVersion)
	assert.Equal(t, int64(4), history[2].ToVersion)

	// 8. Confirma emissão de eventos conversation.state.changed na Outbox
	outboxEvents, err := repo.FetchPendingOutboxEvents(ctx, 100)
	require.NoError(t, err)

	stateChangedCount := 0
	for _, ob := range outboxEvents {
		if ob.ConversationID == conv.ID && ob.EventType == domain.EventTypeConversationStateChanged {
			stateChangedCount++
		}
	}
	assert.Equal(t, 3, stateChangedCount, "must emit conversation.state.changed for each transition")
}

func TestConversationFSM_ConcurrentTransitionRace(t *testing.T) {
	ctx := context.Background()
	pool := setupTestDB(t)
	defer pool.Close()

	repo := postgres.NewRepository(pool)
	tenantID := "tenant-fsm-race"

	conv, err := repo.GetOrCreateConversation(ctx, tenantID, "ext-fsm-race-"+uuid.New().String())
	require.NoError(t, err)

	// Duas operações concorrentes tentando mudar o estado a partir da versão 1:
	// Goroutine A: tenta passar para HUMAN_ACTIVE
	// Goroutine B: tenta passar para CLOSED
	concurrency := 2
	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0
	conflictCount := 0

	targets := []domain.ConversationState{domain.StateHumanActive, domain.StateClosed}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		target := targets[i]
		go func(tState domain.ConversationState, workerNum int) {
			defer wg.Done()
			_, err := repo.TransitionConversationState(
				ctx, tenantID, conv.ID, tState, 1, fmt.Sprintf("race attempt from worker %d", workerNum), "worker",
			)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successCount++
			} else {
				conflictCount++
			}
		}(target, i)
	}

	wg.Wait()

	// Critério de aceite: Exatamente UMA vence e a outra falha por conflito de versão otimista
	assert.Equal(t, 1, successCount, "exactly one transition must succeed")
	assert.Equal(t, 1, conflictCount, "the conflicting transition must be rejected by optimistic concurrency")
}
