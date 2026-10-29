package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	transkafka "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/kafka"
	"github.com/segmentio/kafka-go"
)

// MessagePipelineHandler define o contrato para o pipeline de execução da mensagem.
type MessagePipelineHandler interface {
	HandleMessageReceived(ctx context.Context, evt domain.Event[domain.MessageReceivedPayload]) error
}

// ResultsRepository define a interface necessária para registrar o ciclo de vida da execução, ordenação e idempotência.
type ResultsRepository interface {
	GetResult(ctx context.Context, tenantID, conversationID, messageID string) (*domain.ProcessingResult, error)
	IsPrecedingSequenceCompleted(ctx context.Context, conversationID string, targetSeq int64) (bool, error)
	AcquireExecutionLockWithSeq(ctx context.Context, tenantID, conversationID, messageID string, sequenceNumber int64, leaseDuration time.Duration) (*domain.ProcessingResult, error)
	AcquireExecutionLockWithSeqAndFencing(ctx context.Context, tenantID, conversationID, messageID string, sequenceNumber int64, leaseDuration time.Duration, fencingToken *int64) (*domain.ProcessingResult, error)
	AcquireExecutionLock(ctx context.Context, tenantID, conversationID, messageID string, leaseDuration time.Duration) (*domain.ProcessingResult, error)
	RecordExecutionStart(ctx context.Context, tenantID, conversationID, messageID string) (*domain.ProcessingResult, error)
	RecordExecutionSuccess(ctx context.Context, tenantID, conversationID, messageID string, output interface{}, executionTime time.Duration) error
	RecordExecutionSuccessWithFencing(ctx context.Context, tenantID, conversationID, messageID string, output interface{}, executionTime time.Duration, fencingToken int64) error
	RecordExecutionFailure(ctx context.Context, tenantID, conversationID, messageID string, failureErr error, executionTime time.Duration) error
	RecordExecutionFailureWithFencing(ctx context.Context, tenantID, conversationID, messageID string, failureErr error, executionTime time.Duration, fencingToken int64) error
	ScheduleRetry(ctx context.Context, tenantID, conversationID, messageID string, failureErr error, baseBackoff, maxBackoff time.Duration) (bool, time.Time, error)
}

// ConversationCoordinator define o contrato para coordenação distribuída entre múltiplos workers (INV-02, INV-04, INV-08).
type ConversationCoordinator interface {
	AcquireExecution(ctx context.Context, tenantID, conversationID, workerID string, leaseDuration time.Duration) (*domain.ConversationExecution, error)
	RenewLease(ctx context.Context, executionID string, fencingToken int64, extension time.Duration) error
	ReleaseExecution(ctx context.Context, executionID string, finalStatus domain.ExecutionStatus) error
	ReleaseExecutionWithFencing(ctx context.Context, executionID string, fencingToken int64, finalStatus domain.ExecutionStatus) error
}

// DefaultMessagePipelineHandler é a implementação do pipeline que orquestra coordenação distribuída,
// idempotência durável, ordenação sequencial estrita por conversa (INV-02) e paralelismo entre conversas (INV-03).
type DefaultMessagePipelineHandler struct {
	runtime           domain.AgentRuntime
	resultsRepo       ResultsRepository
	coordinator       ConversationCoordinator
	workerID          string
	leaseTime         time.Duration
	heartbeatInterval time.Duration
	baseBackoff       time.Duration
	maxBackoff        time.Duration
}

// NewDefaultMessagePipelineHandler cria um novo handler integrando o AgentRuntime, ResultsRepository e opcionalmente o Coordinator.
func NewDefaultMessagePipelineHandler(runtime domain.AgentRuntime, resultsRepo ResultsRepository, opts ...PipelineOption) *DefaultMessagePipelineHandler {
	h := &DefaultMessagePipelineHandler{
		runtime:           runtime,
		resultsRepo:       resultsRepo,
		workerID:          "worker-default",
		leaseTime:         30 * time.Second,
		heartbeatInterval: 10 * time.Second,
		baseBackoff:       500 * time.Millisecond,
		maxBackoff:        30 * time.Second,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// PipelineOption configura o DefaultMessagePipelineHandler.
type PipelineOption func(*DefaultMessagePipelineHandler)

func WithRetrySettings(baseBackoff, maxBackoff time.Duration) PipelineOption {
	return func(h *DefaultMessagePipelineHandler) {
		if baseBackoff > 0 {
			h.baseBackoff = baseBackoff
		}
		if maxBackoff > 0 {
			h.maxBackoff = maxBackoff
		}
	}
}

func WithCoordinator(coordinator ConversationCoordinator, workerID string) PipelineOption {
	return func(h *DefaultMessagePipelineHandler) {
		h.coordinator = coordinator
		if workerID != "" {
			h.workerID = workerID
		}
	}
}

func WithLeaseSettings(leaseTime, heartbeatInterval time.Duration) PipelineOption {
	return func(h *DefaultMessagePipelineHandler) {
		if leaseTime > 0 {
			h.leaseTime = leaseTime
		}
		if heartbeatInterval > 0 {
			h.heartbeatInterval = heartbeatInterval
		}
	}
}

func (h *DefaultMessagePipelineHandler) HandleMessageReceived(ctx context.Context, evt domain.Event[domain.MessageReceivedPayload]) error {
	log.Printf("[pipeline] message event received: event_id=%s, tenant=%s, conv=%s, msg_id=%s, seq=%d, role=%s",
		evt.EventID, evt.TenantID, evt.ConversationID, evt.Payload.MessageID, evt.Payload.SequenceNumber, evt.Payload.Role)

	tenantID := evt.TenantID
	convID := evt.ConversationID
	msgID := evt.Payload.MessageID
	seqNumber := evt.Payload.SequenceNumber

	// 1. Controle de Idempotência e Concorrência Atômica via PostgreSQL
	if h.resultsRepo != nil {
		// Checagem prévia rápida de idempotência
		existing, err := h.resultsRepo.GetResult(ctx, tenantID, convID, msgID)
		if err != nil {
			log.Printf("[pipeline] warning: failed to check existing processing result: %v", err)
			return fmt.Errorf("failed to check existing result: %w", err)
		}

		if existing != nil && existing.Status == domain.ExecutionStatusSucceeded {
			log.Printf("[pipeline] idempotency: message %s (conv=%s, seq=%d) already processed with status SUCCEEDED. Skipping execution.",
				msgID, convID, seqNumber)
			return nil
		}

		// 2. Garantia de Ordenação Sequencial Estrita (INV-02):
		// Se seqNumber > 1, a mensagem N só pode ser processada após a mensagem N-1 estar SUCCEEDED.
		if seqNumber > 1 {
			maxWait := 10 * time.Second
			pollInterval := 25 * time.Millisecond
			deadline := time.Now().Add(maxWait)

			precedingDone := false
			for time.Now().Before(deadline) && ctx.Err() == nil {
				done, checkErr := h.resultsRepo.IsPrecedingSequenceCompleted(ctx, convID, seqNumber)
				if checkErr != nil {
					log.Printf("[pipeline] warning: checking preceding sequence %d failed: %v", seqNumber-1, checkErr)
				}
				if done {
					precedingDone = true
					break
				}
				time.Sleep(pollInterval)
			}

			if !precedingDone {
				log.Printf("[pipeline] sequence gap detected for conv=%s, msg=%s: preceding sequence %d has not completed yet",
					convID, msgID, seqNumber-1)
				return fmt.Errorf("preceding sequence %d not completed for conversation %s", seqNumber-1, convID)
			}
		}

		// Tentativa atômica de adquirir o lock de execução (RUNNING) associado ao sequence_number
		lockRes, err := h.resultsRepo.AcquireExecutionLockWithSeq(ctx, tenantID, convID, msgID, seqNumber, h.leaseTime)
		if err != nil {
			log.Printf("[pipeline] idempotency: message %s currently locked or running by another worker: %v. Skipping.", msgID, err)
			return nil
		}

		// Se já estava concluído (SUCCEEDED)
		if lockRes == nil {
			log.Printf("[pipeline] idempotency: message %s already completed (SUCCEEDED). Skipping execution.", msgID)
			return nil
		}
	}

	// 3. Coordenação Distribuída Atômica de Conversa (INV-02, INV-04, INV-08)
	var activeExec *domain.ConversationExecution
	if h.coordinator != nil {
		exec, err := h.coordinator.AcquireExecution(ctx, tenantID, convID, h.workerID, h.leaseTime)
		if err != nil {
			if errors.Is(err, domain.ErrConversationBusy) {
				log.Printf("[pipeline] conversation %s busy (running by another worker). Skipping concurrent execution.", convID)
				return nil
			}
			return fmt.Errorf("failed to acquire distributed conversation execution: %w", err)
		}
		activeExec = exec
	}

	// Contexto cancelável cooperativamente para execução do runtime e heartbeat
	execCtx, cancelExec := context.WithCancel(ctx)
	defer cancelExec()

	// Inicia heartbeat em background para renovação periódica do lease e detecção de fencing obsoleto (INV-04)
	if h.coordinator != nil && activeExec != nil {
		hbTicker := time.NewTicker(h.heartbeatInterval)
		defer hbTicker.Stop()

		go func() {
			for {
				select {
				case <-execCtx.Done():
					return
				case <-hbTicker.C:
					renewErr := h.coordinator.RenewLease(execCtx, activeExec.ID, activeExec.FencingToken, h.leaseTime)
					if renewErr != nil {
						if errors.Is(renewErr, domain.ErrFencingTokenStale) {
							log.Printf("[pipeline] zombie worker detected! Fencing token %d is stale for conv %s. Cancelling context cooperatively (INV-04).",
								activeExec.FencingToken, convID)
							cancelExec()
							return
						}
						log.Printf("[pipeline] warning: failed to renew lease for conv %s: %v", convID, renewErr)
					}
				}
			}
		}()
	}

	// Libera a concessão distribuída ao finalizar (validando fencing_token se disponível)
	defer func() {
		if h.coordinator != nil && activeExec != nil {
			_ = h.coordinator.ReleaseExecutionWithFencing(context.Background(), activeExec.ID, activeExec.FencingToken, domain.ExecutionStatusSucceeded)
		}
	}()

	start := time.Now()

	// 4. Execução do Runtime (IA / Mock)
	if h.runtime != nil {
		execInput := domain.ExecutionInput{
			TenantID:       tenantID,
			ConversationID: convID,
			MessageID:      msgID,
			Content:        evt.Payload.Content,
			Metadata: map[string]string{
				"channel":         evt.Payload.Channel,
				"role":            string(evt.Payload.Role),
				"event_id":        evt.EventID,
				"sequence_number": fmt.Sprintf("%d", seqNumber),
			},
		}

		out, err := h.runtime.Execute(execCtx, execInput)
		execDuration := time.Since(start)

		if err != nil {
			log.Printf("[pipeline] agent runtime failed for message %s: %v", msgID, err)
			if h.resultsRepo != nil {
				// Avalia retry durável com backoff exponencial e jitter (INV-05, INV-06)
				isRetryable, nextRetry, schedErr := h.resultsRepo.ScheduleRetry(
					context.Background(),
					tenantID,
					convID,
					msgID,
					err,
					h.baseBackoff,
					h.maxBackoff,
				)
				if schedErr != nil {
					log.Printf("[pipeline] warning: failed to schedule retry: %v", schedErr)
				} else if isRetryable {
					log.Printf("[pipeline] transient failure scheduled for retry: conv=%s, msg=%s, next_retry_at=%v",
						convID, msgID, nextRetry)
				} else {
					log.Printf("[pipeline] permanent failure recorded (no retry): conv=%s, msg=%s, err=%v",
						convID, msgID, err)
				}
			}
			return fmt.Errorf("agent runtime execution error: %w", err)
		}

		log.Printf("[pipeline] agent runtime responded successfully for seq %d: tokens=%d, reply_preview=%.40s...",
			seqNumber, out.TokensUsed, out.ResponseContent)

		// 5. Salva resultado de sucesso de forma durável no PostgreSQL validando Fencing Token (INV-04)
		if h.resultsRepo != nil {
			var token int64
			if activeExec != nil {
				token = activeExec.FencingToken
			}
			if err := h.resultsRepo.RecordExecutionSuccessWithFencing(context.Background(), tenantID, convID, msgID, out, execDuration, token); err != nil {
				if errors.Is(err, domain.ErrFencingTokenStale) {
					log.Printf("[pipeline] write rejected by fencing token %d: zombie worker write prevented (INV-04)", token)
					return domain.ErrFencingTokenStale
				}
				log.Printf("[pipeline] error saving execution success to db: %v", err)
				return fmt.Errorf("failed to record execution success in database: %w", err)
			}
		}
	}

	return nil
}

// ConversationWorkerService orquestra o consumo de eventos, desserialização, validação, execução e commit manual.
type ConversationWorkerService struct {
	consumer transkafka.Consumer
	handler  MessagePipelineHandler
}

// NewConversationWorkerService inicializa o serviço de processamento do worker.
func NewConversationWorkerService(consumer transkafka.Consumer, handler MessagePipelineHandler) *ConversationWorkerService {
	if handler == nil {
		handler = &DefaultMessagePipelineHandler{}
	}
	return &ConversationWorkerService{
		consumer: consumer,
		handler:  handler,
	}
}

// ProcessNextMessage busca a próxima mensagem no Kafka, valida e processa com commit manual.
func (w *ConversationWorkerService) ProcessNextMessage(ctx context.Context) error {
	msg, err := w.consumer.FetchMessage(ctx)
	if err != nil {
		return err
	}

	if err := w.handleMessage(ctx, msg); err != nil {
		return err
	}

	return nil
}

func (w *ConversationWorkerService) handleMessage(ctx context.Context, msg kafka.Message) error {
	var evt domain.Event[domain.MessageReceivedPayload]
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		log.Printf("ERROR: corrupted json payload in message (topic=%s, partition=%d, offset=%d, key=%s): %v. Payload: %s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), err, string(msg.Value))

		if commitErr := w.consumer.CommitMessages(ctx, msg); commitErr != nil {
			log.Printf("failed to commit corrupted message: %v", commitErr)
			return commitErr
		}
		return nil
	}

	if err := evt.Validate(); err != nil {
		log.Printf("WARN: schema validation failed for event_id=%s, skipping: %v", evt.EventID, err)
		if commitErr := w.consumer.CommitMessages(ctx, msg); commitErr != nil {
			log.Printf("failed to commit invalid message: %v", commitErr)
			return commitErr
		}
		return nil
	}

	// Processa a mensagem pelo pipeline (idempotência, ordenação sequencial estrita e coordenação distribuída)
	if err := w.handler.HandleMessageReceived(ctx, evt); err != nil {
		log.Printf("pipeline handling failed for event_id=%s: %v", evt.EventID, err)
		return err
	}

	// Efetua commit manual do offset APÓS sucesso na persistência do resultado no PostgreSQL (INV-07)
	if err := w.consumer.CommitMessages(ctx, msg); err != nil {
		log.Printf("failed to commit message offset for event_id=%s: %v", evt.EventID, err)
		return err
	}

	return nil
}

// Run executa o loop contínuo de consumo do worker até o cancelamento do context.
func (w *ConversationWorkerService) Run(ctx context.Context) error {
	log.Println("conversation worker loop started, listening for messages...")
	for {
		select {
		case <-ctx.Done():
			log.Println("conversation worker received stop signal, draining active loop...")
			return ctx.Err()
		default:
			err := w.ProcessNextMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
					return nil
				}
				if errors.Is(err, io.EOF) {
					return nil
				}
				log.Printf("worker process message error: %v", err)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

// Start é um alias conveniente para Run.
func (w *ConversationWorkerService) Start(ctx context.Context) error {
	return w.Run(ctx)
}
