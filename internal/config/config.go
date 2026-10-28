package config

import (
	"os"
	"strconv"
	"strings"
)

// Config armazena configuracoes tipadas carregadas de variaveis de ambiente.
type Config struct {
	Port         int
	DatabaseURL  string
	KafkaBrokers []string
}

// Load carrega as configuracoes das variaveis de ambiente com valores padrao.
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
