package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/config"
	transporthttp "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/handler"
)

func main() {
	cfg, err := config.Load(8080)
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	mux := http.NewServeMux()

	// Endpoints operacionais / diagnóstico
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","service":"api"}`))
	})

	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"alive","service":"api"}`))
	})

	// Ingestion API (POST /v1/messages)
	// No momento o service é nil (retornará 202 Accepted gerando os IDs);
	// nas próximas etapas conectaremos a implementação concreta de persistência/eventos.
	msgHandler := handler.NewMessageHandler(nil)
	mux.HandleFunc("/v1/messages", msgHandler.HandleIngest)

	// Middlewares globais: Request ID tracing e Structured Logging
	handlerWithMiddlewares := transporthttp.RequestIDMiddleware(
		transporthttp.LoggingMiddleware(mux),
	)

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           handlerWithMiddlewares,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("api server starting on :%d", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("api server listen error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down api server gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("api server graceful shutdown failed: %v", err)
	}
	log.Println("api server stopped")
}
