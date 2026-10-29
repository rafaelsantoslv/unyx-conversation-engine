package domain_test

import (
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
)

func TestDebounce_Calculation(t *testing.T) {
	cfg := domain.DebounceConfig{
		InactivityWindow: 3 * time.Second,
		MaxWaitTimeout:   10 * time.Second,
	}

	t.Run("first message opens window with inactivity and max wait", func(t *testing.T) {
		now := time.Now().UTC()
		started, debounceUntil, maxWait := domain.CalculateWindowTimes(now, cfg, nil)

		if !started.Equal(now) {
			t.Errorf("expected started at %v, got %v", now, started)
		}
		expectedDebounce := now.Add(3 * time.Second)
		if !debounceUntil.Equal(expectedDebounce) {
			t.Errorf("expected debounce until %v, got %v", expectedDebounce, debounceUntil)
		}
		expectedMaxWait := now.Add(10 * time.Second)
		if !maxWait.Equal(expectedMaxWait) {
			t.Errorf("expected max wait %v, got %v", expectedMaxWait, maxWait)
		}
	})

	t.Run("sliding window extends within max wait boundary", func(t *testing.T) {
		t0 := time.Now().UTC()
		window := &domain.DebounceWindow{
			WindowStartedAt: t0,
			DebounceUntil:   t0.Add(3 * time.Second),
			MaxWaitUntil:    t0.Add(10 * time.Second),
			Status:          domain.DebounceStatusOpen,
		}

		// Mensagem 2 chega aos 2 segundos
		t1 := t0.Add(2 * time.Second)
		started, debounceUntil, maxWait := domain.CalculateWindowTimes(t1, cfg, window)

		if !started.Equal(t0) {
			t.Errorf("expected original started time %v, got %v", t0, started)
		}
		// Deve estender para t1 + 3s = t0 + 5s
		expectedDebounce := t1.Add(3 * time.Second)
		if !debounceUntil.Equal(expectedDebounce) {
			t.Errorf("expected extended debounce until %v, got %v", expectedDebounce, debounceUntil)
		}
		if !maxWait.Equal(window.MaxWaitUntil) {
			t.Errorf("expected original max wait %v, got %v", window.MaxWaitUntil, maxWait)
		}
	})

	t.Run("max wait cap prevents infinite extension (starvation prevention INV-10)", func(t *testing.T) {
		t0 := time.Now().UTC()
		window := &domain.DebounceWindow{
			WindowStartedAt: t0,
			DebounceUntil:   t0.Add(8 * time.Second),
			MaxWaitUntil:    t0.Add(10 * time.Second),
			Status:          domain.DebounceStatusOpen,
		}

		// Mensagem chega aos 9 segundos (9s + 3s = 12s, o que ultrapassaria o teto de 10s)
		t2 := t0.Add(9 * time.Second)
		_, debounceUntil, maxWait := domain.CalculateWindowTimes(t2, cfg, window)

		// O debounceUntil deve ser limitado pelo maxWait (10s)
		if !debounceUntil.Equal(window.MaxWaitUntil) {
			t.Errorf("expected debounceUntil capped at max wait %v, got %v", window.MaxWaitUntil, debounceUntil)
		}
		if !maxWait.Equal(window.MaxWaitUntil) {
			t.Errorf("expected max wait %v, got %v", window.MaxWaitUntil, maxWait)
		}
	})

	t.Run("disabled debounce immediately marks ready", func(t *testing.T) {
		now := time.Now().UTC()
		zeroCfg := domain.DebounceConfig{InactivityWindow: 0, MaxWaitTimeout: 0}
		started, debounceUntil, maxWait := domain.CalculateWindowTimes(now, zeroCfg, nil)

		if !started.Equal(now) || !debounceUntil.Equal(now) || !maxWait.Equal(now) {
			t.Errorf("disabled debounce should be immediately ready at %v", now)
		}
	})
}
