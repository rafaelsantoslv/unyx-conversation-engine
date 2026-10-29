package http

import (
	"crypto/rand"                                                                                      
    "encoding/hex"                                                                                     
    "log/slog"                                                                                         
    "net/http"                                                                                         
    "time"      
)

func generateID() string {                                                                          
	bytes := make([]byte, 16)                                                                          
	_, _ = rand.Read(bytes)                                                                            
	return hex.EncodeToString(bytes)                                                                   
}

type responseRecorder struct {                                                                      
	http.ResponseWriter                                                                                
	statusCode int                                                                                     
}   

func (rec *responseRecorder) WriteHeader(code int) {                                                
	rec.statusCode = code                                                                              
	rec.ResponseWriter.WriteHeader(code)                                                               
}

func RequestIDMiddleware(next http.Handler) http.Handler {                                          
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {                             
		reqID := r.Header.Get("X-Request-ID")                                                              
		if reqID == "" {                                                                                   
			reqID = generateID()                                                                               
		}                                                                                                  
																									
		ctx := WithRequestID(r.Context(), reqID)                                                           
		w.Header().Set("X-Request-ID", reqID)                                                              
																									
		next.ServeHTTP(w, r.WithContext(ctx))                                                              
	})                                                                                                 
}  

func LoggingMiddleware(next http.Handler) http.Handler {                                            
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {                             
		start := time.Now()                                                                                
		rec := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}                             
																									
		next.ServeHTTP(rec, r)                                                                             
																									
		duration := time.Since(start)                                                                      
		reqID := RequestIDFromContext(r.Context())                                                         
																									
		slog.InfoContext(r.Context(), "http request completed",                                                                
			"request_id", reqID,                                                                               
			"method", r.Method,                                                                                
			"path", r.URL.Path,                                                                                
			"status", rec.statusCode,                                                                          
			"duration_ms", duration.Milliseconds(),                                                            
		)                                                                                                  
	})                                                                                                 
}                                      