package config

import (
	"os"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	os.Unsetenv("PORT")
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("KAFKA_BROKERS")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Port != 8080 {
		t.Errorf("expected port 8080, got %d", cfg.Port)
	}
	if cfg.DatabaseURL != "postgres://postgres:postgres@localhost:5432/unyx_conversation?sslmode=disable" {
		t.Errorf("expected default database url, got %s", cfg.DatabaseURL)
	}
	if len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:9092" {
		t.Errorf("expected default broker localhost:9092, got %v", cfg.KafkaBrokers)
	}
}

func TestLoadCustomEnv(t *testing.T) {
	t.Setenv("PORT", "9000")
	t.Setenv("DATABASE_URL", "postgres://custom:secret@db:5432/custom_db")
	t.Setenv("KAFKA_BROKERS", "broker1:9092, broker2:9092")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Port != 9000 {
		t.Errorf("expected port 9000, got %d", cfg.Port)
	}
	if cfg.DatabaseURL != "postgres://custom:secret@db:5432/custom_db" {
		t.Errorf("expected custom database url, got %s", cfg.DatabaseURL)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "broker1:9092" || cfg.KafkaBrokers[1] != "broker2:9092" {
		t.Errorf("expected custom brokers, got %v", cfg.KafkaBrokers)
	}
}
