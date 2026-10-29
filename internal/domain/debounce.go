package domain

import (
	"errors"
	"time"
)

type DebounceWindowStatus string

const (
	DebounceStatusOpen      DebounceWindowStatus = "OPEN"
	DebounceStatusReady     DebounceWindowStatus = "READY"
	DebounceStatusProcessed DebounceWindowStatus = "PROCESSED"
)

// DebounceConfig define os parâmetros da janela de inatividade e teto máximo.
type DebounceConfig struct {
	InactivityWindow time.Duration `json:"inactivity_window"` // Ex: 3s
	MaxWaitTimeout   time.Duration `json:"max_wait_timeout"`   // Ex: 15s (Teto Máximo / Starvation prevention)
}

// DefaultDebounceConfig retorna a configuração padrão de debounce.
func DefaultDebounceConfig() DebounceConfig {
	return DebounceConfig{
		InactivityWindow: 3 * time.Second,
		MaxWaitTimeout:   15 * time.Second,
	}
}

// DebounceWindow representa a janela durável de agrupamento de mensagens de uma conversa.
type DebounceWindow struct {
	ID              string               `json:"id"`
	TenantID        string               `json:"tenant_id"`
	ConversationID  string               `json:"conversation_id"`
	WindowStartedAt time.Time            `json:"window_started_at"`
	DebounceUntil   time.Time            `json:"debounce_until"`
	MaxWaitUntil    time.Time            `json:"max_wait_until"`
	MessageCount    int                  `json:"message_count"`
	Status          DebounceWindowStatus `json:"status"`
	ProcessedAt     *time.Time           `json:"processed_at,omitempty"`
	CreatedAt       time.Time            `json:"created_at"`
	UpdatedAt       time.Time            `json:"updated_at"`
}

var (
	ErrDebounceWindowClosed = errors.New("debounce window is already processed")
)

// CalculateWindowTimes calcula os limites de tempo da janela para abertura ou extensão.
func CalculateWindowTimes(now time.Time, cfg DebounceConfig, existing *DebounceWindow) (startedAt, debounceUntil, maxWaitUntil time.Time) {
	if cfg.InactivityWindow <= 0 {
		// Debounce desabilitado: pronto imediatamente
		return now, now, now
	}

	if existing == nil || existing.Status == DebounceStatusProcessed {
		// Nova janela
		startedAt = now
		debounceUntil = now.Add(cfg.InactivityWindow)
		maxWaitUntil = now.Add(cfg.MaxWaitTimeout)
		return startedAt, debounceUntil, maxWaitUntil
	}

	// Janela existente: estende debounce_until respeitando o max_wait_until (INV-10)
	startedAt = existing.WindowStartedAt
	maxWaitUntil = existing.MaxWaitUntil
	newDebounceUntil := now.Add(cfg.InactivityWindow)

	if newDebounceUntil.After(maxWaitUntil) {
		debounceUntil = maxWaitUntil
	} else {
		debounceUntil = newDebounceUntil
	}

	return startedAt, debounceUntil, maxWaitUntil
}

// IsReady verifica se a janela de debounce expirou e está pronta para consumo pela IA.
func (w *DebounceWindow) IsReady(now time.Time) bool {
	if w.Status == DebounceStatusProcessed {
		return false
	}
	return !now.Before(w.DebounceUntil) || !now.Before(w.MaxWaitUntil)
}
