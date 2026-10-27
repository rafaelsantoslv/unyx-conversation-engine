package domain

import (
	"errors"
	"strings"
	"time"
)

const (
	EventTypeMessageReceived = "message.received"
	EventVersionV1 = "v1"
)

type Event[T any] struct {                                                                          
	EventID        string    `json:"event_id"`                                                         
	TenantID       string    `json:"tenant_id"`                                                        
	ConversationID string    `json:"conversation_id"`                                                  
	EventType      string    `json:"event_type"`                                                       
	Version        string    `json:"version"`                                                          
	OccurredAt     time.Time `json:"occurred_at"`                                                      
	Payload        T         `json:"payload"`                                                          
}                                       
type MessageReceivedPayload struct {                                                                
	MessageID         string      `json:"message_id"`                                                  
	ExternalMessageID string      `json:"external_message_id"`                                         
	Channel           string      `json:"channel"`                                                     
	Role              MessageRole `json:"role"`                                                        
	Content           string      `json:"content"`                                                     
	SequenceNumber    int64       `json:"sequence_number"`                                             
}  

var (                                                                                               
	ErrEmptyEventID   = errors.New("event id cannot be empty")                                         
	ErrEmptyEventType = errors.New("event type cannot be empty")                                       
	ErrEmptyVersion   = errors.New("event version cannot be empty")                                    
	ErrZeroOccurredAt = errors.New("occurred_at must be set")                                          
)  

func (e *Event[T]) Validate() error {                                                               
	if strings.TrimSpace(e.EventID) == "" {                                                            
		return ErrEmptyEventID                                                                             
	}                                                                                                  
	if strings.TrimSpace(e.TenantID) == "" {                                                           
		return ErrEmptyTenantID                                                                            
	}                                                                                                  
	if strings.TrimSpace(e.ConversationID) == "" {                                                     
		return ErrEmptyConversationRef                                                                     
	}                                                                                                  
	if strings.TrimSpace(e.EventType) == "" {                                                          
		return ErrEmptyEventType                                                                           
	}                                                                                                  
	if strings.TrimSpace(e.Version) == "" {                                                            
		return ErrEmptyVersion                                                                             
	}                                                                                                  
	if e.OccurredAt.IsZero() {                                                                         
		return ErrZeroOccurredAt                                                                           
	}                                                                                                  
	return nil                                                                                         
}

func (p *MessageReceivedPayload) Validate() error {                                                 
	if strings.TrimSpace(p.MessageID) == "" {                                                          
		return ErrEmptyMessageID                                                                           
	}                                                                                                  
	if strings.TrimSpace(p.ExternalMessageID) == "" {                                                  
		return ErrEmptyExternalMessageID                                                                   
	}                                                                                                  
	if strings.TrimSpace(p.Channel) == "" {                                                            
		return ErrEmptyChannel                                                                             
	}                                                                                                  
	if !p.Role.IsValid() {                                                                             
		return ErrInvalidMessageRole                                                                       
	}                                                                                                  
	if strings.TrimSpace(p.Content) == "" {                                                            
		return ErrEmptyMessageContent                                                                      
	}                                                                                                  
	return nil                                                                                         
}