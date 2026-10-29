package retry

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"net"
	"strings"
	"time"
)

// ErrorType classifica o erro ocorrido.
type ErrorType string

const (
	ErrorTypeTransient ErrorType = "TRANSIENT"
	ErrorTypePermanent ErrorType = "PERMANENT"
)

// IsRetryable classifica se um erro é transitório (elegível a retry) ou permanente (INV-05, INV-06).
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// 1. Context canceled pelo próprio chamador/shutdown -> não é transitório no sentido de loop
	if errors.Is(err, context.Canceled) {
		return false
	}

	// 2. Timeout ou DeadlineExceeded é clássico transitório
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// 3. Erros de rede temporários
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
	}

	errMsg := strings.ToLower(err.Error())

	// 4. Códigos HTTP e status comuns de instabilidade de LLMs / microserviços
	transientPatterns := []string{
		"503", "service unavailable",
		"502", "bad gateway",
		"504", "gateway timeout",
		"429", "rate limit", "too many requests",
		"timeout", "deadline exceeded",
		"connection reset", "connection refused",
		"temporary", "transient",
	}

	for _, p := range transientPatterns {
		if strings.Contains(errMsg, p) {
			return true
		}
	}

	// 5. Erros permanentes que nunca devem sofrer retry automático:
	// 400 Bad Request, 401 Unauthorized, 403 Forbidden, 404 Not Found, 422 Unprocessable, schema inválido, etc.
	return false
}

// ClassifyError retorna ErrorTypeTransient ou ErrorTypePermanent.
func ClassifyError(err error) ErrorType {
	if IsRetryable(err) {
		return ErrorTypeTransient
	}
	return ErrorTypePermanent
}

// CalculateBackoff calcula backoff exponencial com Full Jitter:
// T_sleep = random_between(0, min(maxBackoff, baseBackoff * 2^attempt))
func CalculateBackoff(attempt int, baseBackoff, maxBackoff time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if baseBackoff <= 0 {
		baseBackoff = 50 * time.Millisecond
	}
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}

	// Calcula base * 2^(attempt-1) com proteção contra overflow
	shift := attempt - 1
	if shift > 30 {
		shift = 30
	}
	multiplier := int64(1) << shift

	tempMax := baseBackoff * time.Duration(multiplier)
	if tempMax > maxBackoff || tempMax <= 0 {
		tempMax = maxBackoff
	}

	// Full Jitter aleatório no intervalo [0, tempMax]
	jitterNs := secureRandomInt64(tempMax.Nanoseconds() + 1)
	return time.Duration(jitterNs)
}

func secureRandomInt64(maxVal int64) int64 {
	if maxVal <= 1 {
		return 0
	}
	n, err := rand.Int(rand.Reader, big.NewInt(maxVal))
	if err != nil {
		// Fallback se rand criptográfico falhar por algum motivo extremo
		return maxVal / 2
	}
	return n.Int64()
}
