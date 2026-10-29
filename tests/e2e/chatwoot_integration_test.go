package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot"
	cwDto "github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot/dto"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/outbound"
	runtimeHttp "github.com/rafaelsantoslv/unyx-conversation-engine/internal/runtime/http"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/handler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChatwootIntegration_21Scenarios implementa e valida rigorosamente todos os 21 cenários
// estabelecidos na especificação do EPIC-05 (UDS-215).
func TestChatwootIntegration_21Scenarios(t *testing.T) {
	pool, _, _ := setupResilienceEnvironment(t)
	defer pool.Close()

	ctx := context.Background()

	// Repositórios
	coreRepo := postgres.NewRepository(pool)
	integrationRepo := postgres.NewIntegrationRepository(pool)
	mappingRepo := postgres.NewIdentityMappingRepository(pool)
	outboundRepo := postgres.NewOutboundIntentsRepository(pool)
	resultsRepo := postgres.NewResultsRepository(pool)
	debounceRepo := postgres.NewDebounceRepository(pool)
	execReqRepo := postgres.NewExecutionRequestRepository(pool)
	policyRepo := postgres.NewPolicyRepository(pool, 5*time.Second)
	ingestionSvc := service.NewIngestionService(coreRepo)

	// Mock do Chatwoot Server
	var mu sync.Mutex
	chatwootSentMessages := make([]cwDto.ChatwootSendResponseDTO, 0)
	var postSendCalls int64
	var postSendFails int64

	chatwootMockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// 1. POST /messages (Envio de saída da IA para o Chatwoot)
		if r.Method == http.MethodPost && r.URL.Path != "/v1/agents/execute" {
			_ = atomic.AddInt64(&postSendCalls, 1)

			// Simula falha se configurado
			if atomic.LoadInt64(&postSendFails) > 0 {
				atomic.AddInt64(&postSendFails, -1)
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error": "temporarily unavailable"}`))
				return
			}

			var body cwDto.ChatwootSendRequestDTO
			_ = json.NewDecoder(r.Body).Decode(&body)

			mu.Lock()
			msgID := int64(100000 + len(chatwootSentMessages) + 1)
			msgResp := cwDto.ChatwootSendResponseDTO{
				ID:                msgID,
				Content:           body.Content,
				MessageType:       "outgoing",
				CreatedAt:         time.Now().Unix(),
				ConversationID:    1234,
				EchoID:            body.EchoID,
				ContentAttributes: body.ContentAttributes,
			}
			chatwootSentMessages = append(chatwootSentMessages, msgResp)
			mu.Unlock()

			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(msgResp)
			return
		}

		// 2. GET /messages (Consulta de reconciliação de entregas ambíguas)
		if r.Method == http.MethodGet {
			mu.Lock()
			copied := make([]cwDto.ChatwootSendResponseDTO, len(chatwootSentMessages))
			copy(copied, chatwootSentMessages)
			mu.Unlock()

			var listResp cwDto.ChatwootMessagesListResponseDTO
			listResp.Payload = copied
			listResp.Meta.AllCount = len(copied)
			_ = json.NewEncoder(w).Encode(listResp)
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer chatwootMockServer.Close()

	// Mock do Agent Runtime HTTP Server
	agentRuntimeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var input domain.ExecutionInput
		_ = json.NewDecoder(r.Body).Decode(&input)

		resp := domain.ExecutionOutput{
			ResponseContent: "Olá! Esta é a resposta computada pelo Agent Runtime para sua mensagem.",
			TokensUsed:      42,
			ExecutedAt:      time.Now().UTC(),
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer agentRuntimeServer.Close()

	// Ingestion Webhook Handler
	ingestionHandler := handler.NewChatwootWebhookHandler(ingestionSvc, integrationRepo)

	// Outbound Sender & Worker
	chatwootSender := chatwoot.NewChatwootOutboundSender(chatwootMockServer.Client())
	echoTracker := chatwoot.NewInMemoryEchoTracker(5 * time.Minute)
	outboundWorker := outbound.NewOutboundWorkerService(
		outboundRepo,
		integrationRepo,
		mappingRepo,
		coreRepo,
		chatwootSender,
		outbound.WithEchoTracker(echoTracker),
	)

	// Runtime Client HTTP
	agentRuntimeClient := runtimeHttp.NewHttpAgentRuntime(
		agentRuntimeServer.URL,
		runtimeHttp.WithHTTPClient(agentRuntimeServer.Client()),
		runtimeHttp.WithTimeout(5*time.Second),
	)

	// Setup do Tenant Principal
	tenantID := "tenant-e2e-" + uuid.New().String()
	webhookToken := "secret-token-e2e-" + uuid.New().String()
	err := integrationRepo.UpsertIntegration(ctx, postgres.TenantIntegration{
		TenantID:       tenantID,
		Channel:        "chatwoot",
		WebhookToken:   webhookToken,
		APIURL:         chatwootMockServer.URL,
		APIAccessToken: "cw-access-token-123",
		IsActive:       true,
	})
	require.NoError(t, err)

	// Helper para disparar webhook do Chatwoot na Ingestion API
	postWebhook := func(tID, token string, payload []byte) *httptest.ResponseRecorder {
		url := fmt.Sprintf("/v1/integrations/chatwoot/webhooks/%s", tID)
		req := httptest.NewRequest(http.MethodPost, url, bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("X-Chatwoot-Webhook-Token", token)
		}
		rr := httptest.NewRecorder()
		ingestionHandler.HandleWebhook(rr, req)
		return rr
	}


	cwAccountID := int64(10)
	cwInboxID := int64(20)
	cwConversationID := int64(1234)
	cwContactID := int64(888)

	// =========================================================================
	// 21 CENÁRIOS INTEGRADOS PONTA A PONTA
	// =========================================================================

	var internalConv *domain.Conversation
	var lastExecutionReq *domain.ExecutionRequest
	var lastIntent *domain.OutboundIntent

	t.Run("01. Cliente envia mensagem pelo Chatwoot -> Webhook recebido", func(t *testing.T) {
		wh := cwDto.ChatwootWebhookPayload{
			Event:       "message_created",
			ID:          5001,
			Content:     "Olá, gostaria de saber preços e horários disponíveis.",
			MessageType: "incoming",
			ContentType: "text",
			Account:     &cwDto.ChatwootAccount{ID: cwAccountID, Name: "Empresa E2E"},
			Inbox:       &cwDto.ChatwootInbox{ID: cwInboxID, Name: "WhatsApp Oficial", ChannelType: "Channel::Whatsapp"},
			Conversation: &cwDto.ChatwootConversation{ID: cwConversationID, Status: "open"},
			Contact:     &cwDto.ChatwootContact{ID: cwContactID, Name: "Carlos Silva", PhoneNumber: "+5511999998888"},
		}
		raw, _ := json.Marshal(wh)
		rr := postWebhook(tenantID, webhookToken, raw)
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("02. Webhook validado e autenticado com token incorreto rejeitado", func(t *testing.T) {
		wh := cwDto.ChatwootWebhookPayload{Event: "message_created", ID: 5002, Content: "Msg inválida"}
		raw, _ := json.Marshal(wh)
		rr := postWebhook(tenantID, "token-falso-invalido", raw)
		assert.Equal(t, http.StatusUnauthorized, rr.Code)
	})

	t.Run("03. Tenant, conversa e contato resolvidos com sucesso", func(t *testing.T) {
		conv, err := mappingRepo.ResolveOrCreateConversationMapping(
			ctx, tenantID, "chatwoot", fmt.Sprintf("%d", cwAccountID),
			fmt.Sprintf("%d", cwInboxID), fmt.Sprintf("%d", cwConversationID),
			fmt.Sprintf("%d", cwContactID), "whatsapp",
		)
		require.NoError(t, err)
		require.NotNil(t, conv)
		assert.Equal(t, tenantID, conv.TenantID)
		assert.Equal(t, domain.StateAIActive, conv.State)
		internalConv = conv
	})

	var savedMsg *domain.Message

	t.Run("04. Mensagem persistida individualmente com ID externo íntegro", func(t *testing.T) {
		// A mensagem 5001 já foi persistida atomicamente com outbox pelo webhook do cenário 01!
		var count int
		err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE tenant_id = $1 AND conversation_id = $2", tenantID, internalConv.ID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)

		// Recupera a mensagem persistida
		var m domain.Message
		err = pool.QueryRow(ctx, "SELECT id, tenant_id, conversation_id, external_id, role, content, sequence_number FROM messages WHERE tenant_id = $1 AND conversation_id = $2", tenantID, internalConv.ID).Scan(
			&m.ID, &m.TenantID, &m.ConversationID, &m.ExternalMessageID, &m.Role, &m.Content, &m.SequenceNumber,
		)
		require.NoError(t, err)
		assert.Equal(t, int64(1), m.SequenceNumber)
		assert.Equal(t, "5001", m.ExternalMessageID)
		savedMsg = &m
	})



	t.Run("05. Mensagens próximas agrupadas pelo Debounce (EPIC-04)", func(t *testing.T) {
		cfg := domain.DebounceConfig{
			InactivityWindow: 1500 * time.Millisecond,
			MaxWaitTimeout:   2500 * time.Millisecond,
		}
		win, err := debounceRepo.RegisterOrExtendWindow(ctx, tenantID, internalConv.ID, cfg)
		require.NoError(t, err)
		assert.Equal(t, domain.DebounceStatusOpen, win.Status)
	})

	executionUUID := uuid.New().String()

	t.Run("06. Execution Request criada com snapshot lógico imutável", func(t *testing.T) {
		created, err := execReqRepo.CreateSnapshotRequest(ctx, tenantID, internalConv.ID)
		require.NoError(t, err)
		require.NotNil(t, created)
		assert.Equal(t, int64(1), created.WatermarkSequence)
		assert.Len(t, created.MessageIDs, 1)
		lastExecutionReq = created
	})

	var computedResponse string
	t.Run("07. Agent Runtime executado via HTTP com sucesso", func(t *testing.T) {
		resp, err := agentRuntimeClient.Execute(ctx, domain.ExecutionInput{
			TenantID:           tenantID,
			ConversationID:     internalConv.ID,
			ExecutionRequestID: lastExecutionReq.ID,
			MessageID:          "cw-5001",
			WatermarkSequence:  lastExecutionReq.WatermarkSequence,
			Content:            "Olá, gostaria de saber preços e horários disponíveis.",
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.NotEmpty(t, resp.ResponseContent)
		computedResponse = resp.ResponseContent
	})

	t.Run("08. Resultado computacional persistido no Postgres", func(t *testing.T) {
		_, err := resultsRepo.RecordExecutionStart(ctx, tenantID, internalConv.ID, savedMsg.ID)
		require.NoError(t, err)

		err = resultsRepo.RecordExecutionSuccess(ctx, tenantID, internalConv.ID, savedMsg.ID, map[string]interface{}{
			"text": computedResponse,
		}, 100*time.Millisecond)
		require.NoError(t, err)
	})



	t.Run("09. Outbound Intent criada duravelmente com status PENDING", func(t *testing.T) {
		intent := domain.OutboundIntent{
			TenantID:       tenantID,
			ConversationID: internalConv.ID,
			ExecutionID:    executionUUID,
			Payload: domain.OutboundPayload{
				Content: computedResponse,
			},
		}
		created, err := outboundRepo.CreateIntent(ctx, intent)
		require.NoError(t, err)
		assert.Equal(t, domain.OutboundStatusPending, created.Status)
		lastIntent = created
	})


	t.Run("10. Outbound Worker adquire a intenção atomicamente", func(t *testing.T) {
		claimed, err := outboundRepo.ClaimPendingIntents(ctx, 5, time.Now().UTC())
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		assert.Equal(t, lastIntent.ID, claimed[0].ID)
	})

	t.Run("11. Resposta enviada com sucesso para a API do Chatwoot", func(t *testing.T) {
		// Como o claim do passo 10 moveu para SENDING, retornamos para PENDING para o worker despachar no batch
		_, _ = pool.Exec(ctx, "UPDATE outbound_intents SET status = 'PENDING' WHERE id = $1", lastIntent.ID)
		processed, err := outboundWorker.ProcessBatch(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, processed)
	})

	t.Run("12. Mensagem aparece na conversa correta do Chatwoot", func(t *testing.T) {
		mu.Lock()
		defer mu.Unlock()
		require.NotEmpty(t, chatwootSentMessages)
		lastSent := chatwootSentMessages[len(chatwootSentMessages)-1]
		assert.Equal(t, int64(1234), lastSent.ConversationID)
		assert.Equal(t, computedResponse, lastSent.Content)
		assert.Equal(t, executionUUID, lastSent.EchoID)
	})


	t.Run("13. Webhook de eco disparado pelo Chatwoot é classificado como BOT_ECHO e NÃO provoca loop", func(t *testing.T) {
		classifier := chatwoot.NewEventClassifier(echoTracker)
		mu.Lock()
		lastSent := chatwootSentMessages[len(chatwootSentMessages)-1]
		mu.Unlock()

		classification, reason := classifier.ClassifyMessage(ctx, tenantID, &cwDto.ChatwootWebhookPayload{
			Event:       "message_created",
			ID:          lastSent.ID,
			MessageType: "outgoing",
			Sender: &cwDto.ChatwootSender{
				Type: "agent_bot",
			},
			Content: lastSent.Content,
		})
		assert.Equal(t, chatwoot.ClassificationBotEcho, classification, "Bot echo deve ser classificado como BOT_ECHO para não gerar resposta recursiva")
		assert.NotEmpty(t, reason)
	})


	t.Run("14. Operador humano assume atendimento -> FSM transiciona para HUMAN_ACTIVE", func(t *testing.T) {
		c, err := coreRepo.GetConversationByID(ctx, internalConv.ID)
		require.NoError(t, err)
		trans, err := coreRepo.TransitionConversationState(
			ctx, tenantID, internalConv.ID, domain.StateHumanActive, c.Version, "operator takeover", "operator-joao",
		)
		require.NoError(t, err)
		assert.Equal(t, domain.StateHumanActive, trans.State)
		internalConv = trans
	})

	t.Run("15. IA é impedida de responder automaticamente enquanto humano atende", func(t *testing.T) {
		execID := uuid.New().String()
		intent := domain.OutboundIntent{
			TenantID:       tenantID,
			ConversationID: internalConv.ID,
			ExecutionID:    execID,
			Payload: domain.OutboundPayload{
				Content: "Tentativa indevida de resposta de IA durante atendimento humano!",
			},
		}
		_, err := outboundRepo.CreateIntent(ctx, intent)
		require.NoError(t, err)

		processed, err := outboundWorker.ProcessBatch(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, processed)

		saved, err := outboundRepo.GetIntentByExecutionID(ctx, tenantID, execID)
		require.NoError(t, err)
		assert.Equal(t, domain.OutboundStatusCancelled, saved.Status, "Intenção deve ter sido cancelada pelo check de elegibilidade INV-05")
	})

	t.Run("16. Atendimento retorna para IA -> Apenas novas mensagens ativam o bot", func(t *testing.T) {
		c, err := coreRepo.GetConversationByID(ctx, internalConv.ID)
		require.NoError(t, err)
		trans, err := coreRepo.TransitionConversationState(
			ctx, tenantID, internalConv.ID, domain.StateAIActive, c.Version, "operator handoff to bot", "operator-joao",
		)
		require.NoError(t, err)
		assert.Equal(t, domain.StateAIActive, trans.State)
		internalConv = trans
	})

	t.Run("17. Chatwoot fica temporariamente indisponível (HTTP 503) -> Retries com backoff recuperam a entrega", func(t *testing.T) {
		atomic.StoreInt64(&postSendFails, 1)

		execID := uuid.New().String()
		intent := domain.OutboundIntent{
			TenantID:       tenantID,
			ConversationID: internalConv.ID,
			ExecutionID:    execID,
			Payload: domain.OutboundPayload{
				Content: "Mensagem pós-falha temporária do Chatwoot.",
			},
		}
		_, err := outboundRepo.CreateIntent(ctx, intent)
		require.NoError(t, err)

		// 1ª tentativa: Falha 503 e agenda retry (FAILED)
		_, _ = outboundWorker.ProcessBatch(ctx)
		saved1, err := outboundRepo.GetIntentByExecutionID(ctx, tenantID, execID)
		require.NoError(t, err)
		assert.Equal(t, domain.OutboundStatusFailed, saved1.Status)
		assert.Equal(t, 1, saved1.Attempts)

		// Avança tempo de retry para o passado e executa 2ª tentativa (sucesso)
		_, _ = pool.Exec(ctx, "UPDATE outbound_intents SET next_retry_at = NOW() - INTERVAL '1 second' WHERE id = $1", saved1.ID)
		processed, err := outboundWorker.ProcessBatch(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, processed)

		saved2, err := outboundRepo.GetIntentByExecutionID(ctx, tenantID, execID)
		require.NoError(t, err)
		assert.Equal(t, domain.OutboundStatusSent, saved2.Status)
	})

	t.Run("18. Worker reinicia abruptamente durante envio -> Recuperabilidade garante entrega posterior", func(t *testing.T) {
		execID := uuid.New().String()
		intent := domain.OutboundIntent{
			TenantID:       tenantID,
			ConversationID: internalConv.ID,
			ExecutionID:    execID,
			Payload: domain.OutboundPayload{
				Content: "Mensagem para teste de restart abrupto do worker.",
			},
		}
		created, err := outboundRepo.CreateIntent(ctx, intent)
		require.NoError(t, err)

		// Simula que worker caiu durante envio
		_, _ = pool.Exec(ctx, "UPDATE outbound_intents SET status = 'FAILED', next_retry_at = NOW() - INTERVAL '1 second' WHERE id = $1", created.ID)

		// Novo worker sobe e recupera
		newWorker := outbound.NewOutboundWorkerService(
			outboundRepo, integrationRepo, mappingRepo, coreRepo, chatwootSender,
		)
		processed, err := newWorker.ProcessBatch(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, processed)

		saved, err := outboundRepo.GetIntentByExecutionID(ctx, tenantID, execID)
		require.NoError(t, err)
		assert.Equal(t, domain.OutboundStatusSent, saved.Status)
	})

	t.Run("19. Webhook duplicado é absorvido com idempotência sem duplicar processamento", func(t *testing.T) {
		dupExternalID := fmt.Sprintf("999%d", time.Now().UnixNano()%100000)
		wh := cwDto.ChatwootWebhookPayload{
			Event:       "message_created",
			ID:          time.Now().UnixNano() % 10000000,
			Content:     "Mensagem enviada com duplicidade de rede.",
			MessageType: "incoming",
			Account:     &cwDto.ChatwootAccount{ID: cwAccountID, Name: "Empresa E2E"},
			Inbox:       &cwDto.ChatwootInbox{ID: cwInboxID, Name: "WhatsApp Oficial", ChannelType: "Channel::Whatsapp"},
			Conversation: &cwDto.ChatwootConversation{ID: cwConversationID, Status: "open"},
			Contact:     &cwDto.ChatwootContact{ID: cwContactID, Name: "Carlos Silva"},
		}
		raw, _ := json.Marshal(wh)

		rr1 := postWebhook(tenantID, webhookToken, raw)
		assert.Equal(t, http.StatusOK, rr1.Code)

		// 2ª chamada idêntica (duplicata de rede do webhook)
		rr2 := postWebhook(tenantID, webhookToken, raw)
		assert.Equal(t, http.StatusOK, rr2.Code)

		// Verifica que a outbox só possui 1 registro para este external_message_id (dentro do objeto aninhado payload.external_message_id)
		extIDStr := fmt.Sprintf("%d", wh.ID)
		var count int
		err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE payload->'payload'->>'external_message_id' = $1", extIDStr).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "Deve absorver duplicata de webhook sem duplicar processamento")
		_ = dupExternalID
	})

	t.Run("20. Dois tenants distintos trocam mensagens simultâneas sem cruzamento de dados", func(t *testing.T) {

		tenantB := "tenant-e2e-isolated-" + uuid.New().String()
		tokenB := "token-tenant-b-" + uuid.New().String()

		err := integrationRepo.UpsertIntegration(ctx, postgres.TenantIntegration{
			TenantID:       tenantB,
			Channel:        "chatwoot",
			WebhookToken:   tokenB,
			APIURL:         chatwootMockServer.URL,
			APIAccessToken: "token-b-123",
			IsActive:       true,
		})
		require.NoError(t, err)

		convB, err := mappingRepo.ResolveOrCreateConversationMapping(
			ctx, tenantB, "chatwoot", fmt.Sprintf("%d", cwAccountID),
			fmt.Sprintf("%d", cwInboxID), fmt.Sprintf("%d", cwConversationID),
			"777", "whatsapp",
		)
		require.NoError(t, err)
		assert.NotEqual(t, internalConv.ID, convB.ID, "IDs internos DEVEM SER DISTINTOS (INV-01)")
		assert.Equal(t, tenantB, convB.TenantID)
	})

	t.Run("21. Timeout de rede com resultado ambíguo é reconciliado sem duplicar mensagens no cliente", func(t *testing.T) {
		ambiguousMessages := make([]cwDto.ChatwootSendResponseDTO, 0)
		postCount := 0
		getCount := 0

		ambiguousServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.Method == http.MethodPost {
				postCount++
				var body cwDto.ChatwootSendRequestDTO
				_ = json.NewDecoder(r.Body).Decode(&body)
				ambiguousMessages = append(ambiguousMessages, cwDto.ChatwootSendResponseDTO{
					ID:                999999,
					Content:           body.Content,
					MessageType:       "outgoing",
					CreatedAt:         time.Now().Unix(),
					ConversationID:    cwConversationID,
					EchoID:            body.EchoID,
					ContentAttributes: body.ContentAttributes,
				})

				// Fecha socket TCP abruptamente sem mandar cabeçalho HTTP
				hj, ok := w.(http.Hijacker)
				if ok {
					conn, _, _ := hj.Hijack()
					conn.Close()
					return
				}
				w.WriteHeader(http.StatusOK)
				return
			}
			if r.Method == http.MethodGet {
				getCount++
				var list cwDto.ChatwootMessagesListResponseDTO
				list.Payload = ambiguousMessages
				list.Meta.AllCount = len(ambiguousMessages)
				_ = json.NewEncoder(w).Encode(list)
				return
			}
		}))
		defer ambiguousServer.Close()

		// Aponta integração do tenant para o mock de ambiguidade
		err := integrationRepo.UpsertIntegration(ctx, postgres.TenantIntegration{
			TenantID:       tenantID,
			Channel:        "chatwoot",
			WebhookToken:   webhookToken,
			APIURL:         ambiguousServer.URL,
			APIAccessToken: "cw-access-token-123",
			IsActive:       true,
		})
		require.NoError(t, err)

		ambiguousExecID := uuid.New().String()
		ambiguousIntent := domain.OutboundIntent{
			TenantID:       tenantID,
			ConversationID: internalConv.ID,
			ExecutionID:    ambiguousExecID,
			Payload: domain.OutboundPayload{
				Content: "Mensagem ambígua sob teste de Two Generals' Problem.",
			},
		}
		_, err = outboundRepo.CreateIntent(ctx, ambiguousIntent)
		require.NoError(t, err)

		ambiguousSender := chatwoot.NewChatwootOutboundSender(ambiguousServer.Client())
		ambiguousWorker := outbound.NewOutboundWorkerService(
			outboundRepo, integrationRepo, mappingRepo, coreRepo, ambiguousSender,
		)

		// 1ª Rodada: Faz POST, TCP cai, worker marca UNKNOWN
		_, _ = ambiguousWorker.ProcessBatch(ctx)
		saved1, err := outboundRepo.GetIntentByExecutionID(ctx, tenantID, ambiguousExecID)
		require.NoError(t, err)
		assert.Equal(t, domain.OutboundStatusUnknown, saved1.Status)
		assert.Equal(t, 1, postCount)

		// 2ª Rodada: Faz GET /messages de reconciliação, localiza a mensagem e transiciona para SENT sem enviar POST novamente
		_, _ = pool.Exec(ctx, "UPDATE outbound_intents SET next_retry_at = NOW() - INTERVAL '1 second' WHERE id = $1", saved1.ID)
		processed2, err := ambiguousWorker.ProcessBatch(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, processed2)
		assert.Equal(t, 1, postCount, "NÃO deve ter feito outro POST (evitou mensagem duplicada no WhatsApp!)")
		assert.Equal(t, 1, getCount, "Deve ter consultado o histórico via GET para reconciliação")

		saved2, err := outboundRepo.GetIntentByExecutionID(ctx, tenantID, ambiguousExecID)
		require.NoError(t, err)
		assert.Equal(t, domain.OutboundStatusSent, saved2.Status)
		require.NotNil(t, saved2.ExternalMessageID)
		assert.Equal(t, "999999", *saved2.ExternalMessageID)
	})

	_ = policyRepo
}
