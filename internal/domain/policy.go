package domain

import (
	"context"
	"time"
)

// ConversationPolicy define as regras de parametrização de atendimento por tenant, canal ou agente (INV-09).
type ConversationPolicy struct {
	TenantID                  string        `json:"tenant_id"`
	Channel                   string        `json:"channel,omitempty"`
	DebounceEnabled           bool          `json:"debounce_enabled"`
	DebounceInactivitySeconds int           `json:"debounce_inactivity_seconds"`
	MaxWaitSeconds            int           `json:"max_wait_seconds"`
	AllowAIProcessing         bool          `json:"allow_ai_processing"`
	HandoffKeywordPrefix      string        `json:"handoff_keyword_prefix,omitempty"`
	InactivityTimeout         time.Duration `json:"inactivity_timeout"`
	MaxDailySessions          int           `json:"max_daily_sessions,omitempty"`
	BusinessHoursOnly         bool          `json:"business_hours_only,omitempty"`
}

// DefaultPolicy retorna a política segura de fábrica caso o tenant não possua configuração customizada.
func DefaultPolicy(tenantID string) ConversationPolicy {
	return ConversationPolicy{
		TenantID:                  tenantID,
		DebounceEnabled:           true,
		DebounceInactivitySeconds: 3,
		MaxWaitSeconds:            15,
		AllowAIProcessing:         true,
		InactivityTimeout:         24 * time.Hour,
	}
}

// ConversationPolicyResolver desacopla o motor de regras de negócio específicas de canal ou tenant.
type ConversationPolicyResolver interface {
	ResolvePolicy(ctx context.Context, tenantID, channel string) (*ConversationPolicy, error)
	CanProcessWithAI(ctx context.Context, conv *Conversation) (bool, error)
}

// DefaultConversationPolicyResolver é a implementação padrão e segura de resolução de políticas.
type DefaultConversationPolicyResolver struct{}

func NewDefaultConversationPolicyResolver() *DefaultConversationPolicyResolver {
	return &DefaultConversationPolicyResolver{}
}

func (r *DefaultConversationPolicyResolver) ResolvePolicy(ctx context.Context, tenantID, channel string) (*ConversationPolicy, error) {
	return &ConversationPolicy{
		TenantID:          tenantID,
		Channel:           channel,
		AllowAIProcessing: true,
		InactivityTimeout: 24 * time.Hour,
	}, nil
}

// CanProcessWithAI valida se o estado da conversa e as políticas permitem a invocação da IA.
func (r *DefaultConversationPolicyResolver) CanProcessWithAI(ctx context.Context, conv *Conversation) (bool, error) {
	if conv == nil {
		return false, nil
	}
	return conv.AllowsAIProcessing(), nil
}
