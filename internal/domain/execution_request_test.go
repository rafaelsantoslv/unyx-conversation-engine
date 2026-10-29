package domain_test

import (
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/stretchr/testify/assert"
)

func TestExecutionRequest_Validate(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name    string
		req     domain.ExecutionRequest
		wantErr error
	}{
		{
			name: "valid request",
			req: domain.ExecutionRequest{
				ID:                "req-1",
				TenantID:          "tenant-1",
				ConversationID:    "conv-1",
				GenerationID:      1,
				WatermarkSequence: 3,
				MessageIDs:        []string{"m-1", "m-2", "m-3"},
				Status:            domain.ExecutionRequestStatusPending,
				CreatedAt:         now,
				UpdatedAt:         now,
			},
			wantErr: nil,
		},
		{
			name: "empty id",
			req: domain.ExecutionRequest{
				ID:                "",
				TenantID:          "tenant-1",
				ConversationID:    "conv-1",
				WatermarkSequence: 3,
				MessageIDs:        []string{"m-1"},
				Status:            domain.ExecutionRequestStatusPending,
			},
			wantErr: domain.ErrEmptyExecutionRequestID,
		},
		{
			name: "empty tenant id",
			req: domain.ExecutionRequest{
				ID:                "req-1",
				TenantID:          "",
				ConversationID:    "conv-1",
				WatermarkSequence: 3,
				MessageIDs:        []string{"m-1"},
				Status:            domain.ExecutionRequestStatusPending,
			},
			wantErr: domain.ErrEmptyTenantID,
		},
		{
			name: "zero watermark",
			req: domain.ExecutionRequest{
				ID:                "req-1",
				TenantID:          "tenant-1",
				ConversationID:    "conv-1",
				WatermarkSequence: 0,
				MessageIDs:        []string{"m-1"},
				Status:            domain.ExecutionRequestStatusPending,
			},
			wantErr: domain.ErrZeroWatermarkSequence,
		},
		{
			name: "empty message ids",
			req: domain.ExecutionRequest{
				ID:                "req-1",
				TenantID:          "tenant-1",
				ConversationID:    "conv-1",
				WatermarkSequence: 3,
				MessageIDs:        []string{},
				Status:            domain.ExecutionRequestStatusPending,
			},
			wantErr: domain.ErrEmptyMessageIDs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExecutionRequest_IncludesMessage(t *testing.T) {
	req := domain.ExecutionRequest{
		ID:                "req-1",
		TenantID:          "tenant-1",
		ConversationID:    "conv-1",
		WatermarkSequence: 3,
		MessageIDs:        []string{"m-1", "m-2", "m-3"},
		Status:            domain.ExecutionRequestStatusPending,
	}

	// Mensagens dentro do snapshot
	assert.True(t, req.IncludesMessage(1, "m-1"))
	assert.True(t, req.IncludesMessage(2, "m-2"))
	assert.True(t, req.IncludesMessage(3, "m-3"))

	// Mensagem posterior ao watermark (seq 4) - deve ser estritamente ignorada
	assert.False(t, req.IncludesMessage(4, "m-4"))

	// Mensagem com sequence válida mas ID que não pertence ao snapshot
	assert.False(t, req.IncludesMessage(2, "m-other"))
}
