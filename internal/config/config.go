package config

import (
	"os"
	"strconv"
	"strings"
)

// Config holds typed configuration loaded from environment variables.
type Config struct {
	Port         int
	DatabaseURL  string
	KafkaBrokers []string
}

// Load reads configuration from environment variables with sensible defaults.
//
// Recognized environment variables:
// - PORT: port number for HTTP server (default: 8080 or fallback parameter)
// - DATABASE_URL: PostgreSQL connection string (default: postgres://postgres:postgres@localhost:5432/unyx_conversation?sslmode=disable)
// - KAFKA_BROKERS: Comma-separated list of Kafka broker addresses (default: localhost:9092)
func Load(defaultPort ...int) (*Config, error) {
	fallbackPort := 8080
	if len(defaultPort) > 0 && defaultPort[0] > 0 {
		fallbackPort = defaultPort[0]
	}

	port := fallbackPort
	if val := os.Getenv("PORT"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			port = parsed
		}
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/unyx_conversation?sslmode=disable"
	}

	kafkaBrokersRaw := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokersRaw == "" {
		kafkaBrokersRaw = "localhost:9092"
	}

	var brokers []string
	for _, b := range strings.Split(kafkaBrokersRaw, ",") {
		trimmed := strings.TrimSpace(b)
		if trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}

	return &Config{
		Port:         port,
		DatabaseURL:  databaseURL,
		KafkaBrokers: brokers,
	}, nil
}
