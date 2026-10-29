package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
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
	RecordExecutionSuccessWithApplicability(ctx context.Context, tenantID, conversationID, messageID string, output interface{}, executionTime time.Duration, fencingToken int64, expectedVersion int64) (*domain.ApplicabilityCheckResult, error)
	RecordExecutionFailure(ctx context.Context, tenantID, conversationID, messageID string, failureErr error, executionTime time.Duration) error
	RecordExecutionFailureWithFencing(ctx context.Context, tenantID, conversationID, messageID string, failureErr error, executionTime time.Duration, fencingToken int64) error
	ScheduleRetry(ctx context.Context, tenantID, conversationID, messageID string, failureErr error, baseBackoff, maxBackoff time.Duration) (bool, time.Time, error)
	QuarantineEvent(ctx context.Context, qEvent domain.QuarantinedEvent) error
}

// DebounceRepository define as operações duráveis de debounce no pipeline.
type DebounceRepository interface {
	RegisterOrExtendWindow(ctx context.Context, tenantID, conversationID string, cfg domain.DebounceConfig) (*domain.DebounceWindow, error)
	GetActiveWindow(ctx context.Context, conversationID string) (*domain.DebounceWindow, error)
	MarkWindowProcessed(ctx context.Context, conversationID string) error
}

// ExecutionSchedulerRepository define o contrato para persistência e agendamento durável no PostgreSQL.
type ExecutionSchedulerRepository interface {
	ScheduleExecution(ctx context.Context, tenantID, conversationID string, scheduledFor time.Time) (*domain.ConversationSchedule, error)
	CancelSchedule(ctx context.Context, conversationID string) error
}

// ExecutionRequestSnapshotRepository define o contrato para carregar e atualizar snapshots lógicos de ExecutionRequest (INV-01, INV-04).
type ExecutionRequestSnapshotRepository interface {
	GetExecutionRequest(ctx context.Context, id string) (*domain.ExecutionRequest, error)
	FetchMessagesForRequest(ctx context.Context, req *domain.ExecutionRequest) ([]*domain.Message, error)
	UpdateStatus(ctx context.Context, id string, status domain.ExecutionRequestStatus) error
}

// TenantPolicyResolver define a resolução desacoplada de configurações e políticas por tenant (INV-09).
type TenantPolicyResolver interface {
	ResolvePolicy(ctx context.Context, tenantID, channel string) (*domain.ConversationPolicy, error)
}

// ConversationCoordinator define o contrato para coordenação distribuída entre múltiplos workers (INV-02, INV-04, INV-08).
type ConversationCoordinator interface {
	AcquireExecution(ctx context.Context, tenantID, conversationID, workerID string, leaseDuration time.Duration) (*domain.ConversationExecution, error)
	RenewLease(ctx context.Context, executionID string, fencingToken int64, extension time.Duration) error
	ReleaseExecution(ctx context.Context, executionID string, finalStatus domain.ExecutionStatus) error
	ReleaseExecutionWithFencing(ctx context.Context, executionID string, fencingToken int64, finalStatus domain.ExecutionStatus) error
	ReleaseExecutionAndSchedulePending(ctx context.Context, executionID string, fencingToken int64, tenantID, conversationID string, watermarkSequence int64, finalStatus domain.ExecutionStatus) (*domain.HasPendingMessagesResult, error)
}

// DefaultMessagePipelineHandler é a implementação do pipeline que orquestra coordenação distribuída,
// idempotência durável, ordenação sequencial estrita por conversa (INV-02) e paralelismo entre conversas (INV-03).
type DefaultMessagePipelineHandler struct {
	runtime           domain.AgentRuntime
	resultsRepo       ResultsRepository
	coordinator       ConversationCoordinator
	debounceRepo      DebounceRepository
	scheduleRepo      ExecutionSchedulerRepository
	execReqRepo       ExecutionRequestSnapshotRepository
	policyResolver    TenantPolicyResolver
	debounceCfg       domain.DebounceConfig
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
		debounceCfg:       domain.DebounceConfig{InactivityWindow: 0, MaxWaitTimeout: 0}, // Padrão desabilitado para retrocompatibilidade
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// PipelineOption configura o DefaultMessagePipelineHandler.
type PipelineOption func(*DefaultMessagePipelineHandler)

func WithDebounce(repo DebounceRepository, cfg domain.DebounceConfig) PipelineOption {
	return func(h *DefaultMessagePipelineHandler) {
		h.debounceRepo = repo
		h.debounceCfg = cfg
	}
}

func WithScheduler(repo ExecutionSchedulerRepository) PipelineOption {
	return func(h *DefaultMessagePipelineHandler) {
		h.scheduleRepo = repo
	}
}

func WithExecutionRequestRepository(repo ExecutionRequestSnapshotRepository) PipelineOption {
	return func(h *DefaultMessagePipelineHandler) {
		h.execReqRepo = repo
	}
}

func WithPolicyResolver(resolver TenantPolicyResolver) PipelineOption {
	return func(h *DefaultMessagePipelineHandler) {
		h.policyResolver = resolver
	}
}

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

	// 1. Resolução Dinâmica de Políticas por Tenant (INV-09)
	effectiveDebounceCfg := h.debounceCfg
	if h.policyResolver != nil {
		if pol, polErr := h.policyResolver.ResolvePolicy(ctx, tenantID, evt.Payload.Channel); polErr == nil && pol != nil {
			if !pol.AllowAIProcessing {
				log.Printf("[pipeline] tenant policy disallows AI processing for tenant %s. Skipping execution.", tenantID)
				return nil
			}
			if pol.DebounceEnabled {
				effectiveDebounceCfg = domain.DebounceConfig{
					InactivityWindow: time.Duration(pol.DebounceInactivitySeconds) * time.Second,
					MaxWaitTimeout:   time.Duration(pol.MaxWaitSeconds) * time.Second,
				}
			} else {
				effectiveDebounceCfg = domain.DebounceConfig{InactivityWindow: 0, MaxWaitTimeout: 0}
			}
		}
	}

	// 2. Controle de Idempotência e Concorrência Atômica via PostgreSQL
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

		// Garantia de Ordenação Sequencial Estrita (INV-02):
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

	// 3. Gestão Durável de Debounce (INV-01, INV-02, INV-10)
	if h.debounceRepo != nil && effectiveDebounceCfg.InactivityWindow > 0 {
		window, err := h.debounceRepo.RegisterOrExtendWindow(ctx, tenantID, convID, effectiveDebounceCfg)
		if err != nil {
			log.Printf("[pipeline] warning: failed to register/extend debounce window: %v", err)
		} else if window != nil {
			now := time.Now().UTC()

			// Se tiver repositório de agendamento durável configurado, registra/atualiza o agendamento no PostgreSQL (INV-03, INV-05)
			if h.scheduleRepo != nil {
				_, schedErr := h.scheduleRepo.ScheduleExecution(ctx, tenantID, convID, window.DebounceUntil)
				if schedErr != nil {
					log.Printf("[pipeline] warning: failed to persist durable schedule for conversation %s: %v", convID, schedErr)
				}
			}

			if !window.IsReady(now) {
				// Aguarda a janela de inatividade fechar ou teto máximo ser atingido (Sliding Window)
				waitDuration := time.Until(window.DebounceUntil)
				if waitDuration > 0 {
					log.Printf("[pipeline] debounce: message %s waiting %v for silence window (msg_count=%d)",
						msgID, waitDuration, window.MessageCount)
					select {
					case <-time.After(waitDuration):
					case <-ctx.Done():
						return ctx.Err()
					}
				}

				// Revalida se uma nova mensagem estendeu a janela enquanto esperávamos
				activeWin, checkErr := h.debounceRepo.GetActiveWindow(ctx, convID)
				if checkErr == nil && activeWin != nil && activeWin.Status == domain.DebounceStatusOpen && !activeWin.IsReady(time.Now().UTC()) {
					log.Printf("[pipeline] debounce: window was extended by another incoming message (msg_count=%d). Deferring execution.",
						activeWin.MessageCount)
					return nil
				}
			}

			// Marca janela como processada para o lote atual
			_ = h.debounceRepo.MarkWindowProcessed(ctx, convID)
		}
	}

	// 4. Coordenação Distribuída Atômica de Conversa (INV-02, INV-04, INV-08)
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

	var finalExecutionStatus = domain.ExecutionStatusSucceeded
	var watermark int64 = seqNumber

	// Libera a concessão distribuída ao finalizar (validando fencing_token se disponível)
	// e agenda automaticamente ciclo subsequente caso novas mensagens in-flight tenham chegado (INV-01, INV-02, INV-10)
	defer func() {
		if h.coordinator != nil && activeExec != nil {
			pendingRes, relErr := h.coordinator.ReleaseExecutionAndSchedulePending(
				context.Background(),
				activeExec.ID,
				activeExec.FencingToken,
				tenantID,
				convID,
				watermark,
				finalExecutionStatus,
			)
			if relErr != nil {
				// Fallback para liberação simples caso ocorra erro no agendamento
				_ = h.coordinator.ReleaseExecutionWithFencing(context.Background(), activeExec.ID, activeExec.FencingToken, finalExecutionStatus)
			} else if pendingRes != nil && pendingRes.HasPending {
				log.Printf("[pipeline] in-flight messages detected during execution for conv %s (count=%d, max_seq=%d). Subsequence execution auto-scheduled! (INV-02, INV-10)",
					convID, pendingRes.PendingCount, pendingRes.MaxSequenceNumber)
			}
		}
	}()

	start := time.Now()

	// 4. Execução do Runtime (IA / Mock)
	if h.runtime != nil {
		var snapshotMsgs []*domain.Message
		execReqID := ""

		if h.execReqRepo != nil {
			// Se o evento carrega um execution_request_id nos metadados ou se busca o snapshot ativo
			// (ex: enviado pelo evento conversation.execution.ready)
			if reqID, ok := evt.Payload.ExternalMessageID, false; ok {
				_ = reqID
			}
		}

		execInput := domain.ExecutionInput{
			ExecutionRequestID: execReqID,
			TenantID:           tenantID,
			ConversationID:     convID,
			MessageID:          msgID,
			WatermarkSequence:  watermark,
			SnapshotMessages:   snapshotMsgs,
			Content:            evt.Payload.Content,
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
			finalExecutionStatus = domain.ExecutionStatusFailed
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

					// Isola na Quarentena (Dead Letter) para erros permanentes ou retries estourados (INV-06)
					evtBytes, _ := json.Marshal(evt)
					attempt := 1
					if res, _ := h.resultsRepo.GetResult(context.Background(), tenantID, convID, msgID); res != nil {
						attempt = res.AttemptCount
					}
					qErr := h.resultsRepo.QuarantineEvent(context.Background(), domain.QuarantinedEvent{
						TenantID:       tenantID,
						ConversationID: convID,
						EventID:        evt.EventID,
						Payload:        evtBytes,
						ErrorReason:    err.Error(),
						AttemptCount:   attempt,
						QuarantinedAt:  time.Now().UTC(),
					})
					if qErr != nil {
						log.Printf("[pipeline] warning: failed to isolate permanent failure into quarantine: %v", qErr)
					} else {
						log.Printf("[pipeline] event %s successfully quarantined (dead letter isolation)", evt.EventID)
					}
				}
			}
			return fmt.Errorf("agent runtime execution error: %w", err)
		}

		log.Printf("[pipeline] agent runtime responded successfully for seq %d: tokens=%d, reply_preview=%.40s...",
			seqNumber, out.TokensUsed, out.ResponseContent)

		// 5. Salva resultado de forma durável validando Fencing Token (INV-04) e Validação Atômica de Aplicabilidade (INV-06, INV-07)
		if h.resultsRepo != nil {
			var token int64
			if activeExec != nil {
				token = activeExec.FencingToken
			}
			appResult, err := h.resultsRepo.RecordExecutionSuccessWithApplicability(context.Background(), tenantID, convID, msgID, out, execDuration, token, 0)
			if err != nil {
				if errors.Is(err, domain.ErrFencingTokenStale) {
					log.Printf("[pipeline] write rejected by fencing token %d: zombie worker write prevented (INV-04)", token)
					return domain.ErrFencingTokenStale
				}
				log.Printf("[pipeline] error saving execution success to db: %v", err)
				return fmt.Errorf("failed to record execution success in database: %w", err)
			}

			if appResult != nil && !appResult.IsApplicable {
				log.Printf("[pipeline] WARN: result superseded and discarded (INV-06, INV-07)! Reason: %s (msg=%s, conv=%s, final_status=%s)",
					appResult.Reason, msgID, convID, appResult.FinalStatus)
			}
		}
	}

	return nil
}

// ConversationWorkerService orquestra o consumo de eventos, desserialização, validação, execução e commit manual.
type ConversationWorkerService struct {
	consumer    transkafka.Consumer
	handler     MessagePipelineHandler
	resultsRepo ResultsRepository
}

// WorkerServiceOption configura o ConversationWorkerService.
type WorkerServiceOption func(*ConversationWorkerService)

// WithWorkerResultsRepo injeta o ResultsRepository para quarentena de poison pills no worker.
func WithWorkerResultsRepo(repo ResultsRepository) WorkerServiceOption {
	return func(w *ConversationWorkerService) {
		w.resultsRepo = repo
	}
}

// NewConversationWorkerService inicializa o serviço de processamento do worker.
func NewConversationWorkerService(consumer transkafka.Consumer, handler MessagePipelineHandler, opts ...WorkerServiceOption) *ConversationWorkerService {
	if handler == nil {
		handler = &DefaultMessagePipelineHandler{}
	}
	w := &ConversationWorkerService{
		consumer: consumer,
		handler:  handler,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
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

		// Isola Poison Pill de JSON corrompido em quarantine_events antes de commitar
		if w.resultsRepo != nil {
			tenantID := "unknown"
			keyParts := strings.Split(string(msg.Key), ":")
			if len(keyParts) > 0 && keyParts[0] != "" {
				tenantID = keyParts[0]
			}
			convID := "unknown"
			if len(keyParts) > 1 && keyParts[1] != "" {
				convID = keyParts[1]
			}

			qErr := w.resultsRepo.QuarantineEvent(ctx, domain.QuarantinedEvent{
				TenantID:       tenantID,
				ConversationID: convID,
				EventID:        fmt.Sprintf("poison-offset-%d-%d", msg.Partition, msg.Offset),
				Payload:        msg.Value,
				ErrorReason:    fmt.Sprintf("corrupted json payload: %v", err),
				AttemptCount:   1,
				QuarantinedAt:  time.Now().UTC(),
			})
			if qErr != nil {
				log.Printf("failed to isolate corrupted json into quarantine: %v", qErr)
			}
		}

		if commitErr := w.consumer.CommitMessages(ctx, msg); commitErr != nil {
			log.Printf("failed to commit corrupted message: %v", commitErr)
			return commitErr
		}
		return nil
	}

	if err := evt.Validate(); err != nil {
		log.Printf("WARN: schema validation failed for event_id=%s, skipping: %v", evt.EventID, err)

		// Isola Poison Pill de validação de schema em quarantine_events antes de commitar
		if w.resultsRepo != nil {
			qErr := w.resultsRepo.QuarantineEvent(ctx, domain.QuarantinedEvent{
				TenantID:       evt.TenantID,
				ConversationID: evt.ConversationID,
				EventID:        evt.EventID,
				Payload:        msg.Value,
				ErrorReason:    fmt.Sprintf("schema validation failed: %v", err),
				AttemptCount:   1,
				QuarantinedAt:  time.Now().UTC(),
			})
			if qErr != nil {
				log.Printf("failed to isolate invalid schema event into quarantine: %v", qErr)
			}
		}

		if commitErr := w.consumer.CommitMessages(ctx, msg); commitErr != nil {
			log.Printf("failed to commit invalid message: %v", commitErr)
			return commitErr
		}
		return nil
	}

	// Processa a mensagem pelo pipeline (idempotência, ordenação sequencial estrita e coordenação distribuída)
	if err := w.handler.HandleMessageReceived(ctx, evt); err != nil {
		log.Printf("pipeline handling failed for event_id=%s: %v", evt.EventID, err)

		// Se o erro foi permanente ou os retries se esgotaram (status = FAILED),
		// o evento foi isolado na quarentena e podemos commitar o offset para destravar a partição!
		if w.resultsRepo != nil {
			res, getErr := w.resultsRepo.GetResult(ctx, evt.TenantID, evt.ConversationID, evt.Payload.MessageID)
			if getErr == nil && res != nil && res.Status == domain.ExecutionStatusFailed {
				log.Printf("[worker] event %s failed permanently and quarantined. Committing offset to unblock partition (INV-06).", evt.EventID)
				if commitErr := w.consumer.CommitMessages(ctx, msg); commitErr != nil {
					log.Printf("failed to commit quarantined message offset: %v", commitErr)
					return commitErr
				}
				return nil
			}
		}

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
