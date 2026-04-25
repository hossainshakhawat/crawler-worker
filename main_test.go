package main

import (
	"os"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestLoadConfig_EnvOverride(t *testing.T) {
	viper.Reset()
	t.Setenv("WORKER_KAFKA_BROKER", "test-kafka:9092")
	t.Setenv("WORKER_REDIS_ADDR", "test-redis:6379")
	t.Setenv("WORKER_WORKERS", "16")
	defer viper.Reset()

	cfg := loadConfig()

	if cfg.kafkaBroker != "test-kafka:9092" {
		t.Errorf("kafkaBroker: got %q, want %q", cfg.kafkaBroker, "test-kafka:9092")
	}
	if cfg.redisAddr != "test-redis:6379" {
		t.Errorf("redisAddr: got %q, want %q", cfg.redisAddr, "test-redis:6379")
	}
	if cfg.numWorkers != 16 {
		t.Errorf("numWorkers: got %d, want 16", cfg.numWorkers)
	}
}

func TestLoadConfig_TimeoutParsing(t *testing.T) {
	viper.Reset()
	t.Setenv("WORKER_TIMEOUT", "30s")
	t.Setenv("WORKER_CRAWL_DELAY", "500ms")
	defer viper.Reset()

	cfg := loadConfig()

	if cfg.timeout != 30*time.Second {
		t.Errorf("timeout: got %v, want 30s", cfg.timeout)
	}
	if cfg.crawlDelay != 500*time.Millisecond {
		t.Errorf("crawlDelay: got %v, want 500ms", cfg.crawlDelay)
	}
}

func TestLoadConfig_ConfigFile(t *testing.T) {
	viper.Reset()
	defer viper.Reset()
	// Unset any env overrides so we read purely from config.yml
	for _, key := range []string{
		"WORKER_KAFKA_BROKER", "WORKER_REDIS_ADDR", "WORKER_WORKERS",
		"WORKER_TIMEOUT", "WORKER_MAX_BODY", "WORKER_CRAWL_DELAY", "WORKER_AGENT",
	} {
		os.Unsetenv(key)
	}

	cfg := loadConfig()

	// config.yml in the working dir should provide these defaults
	if cfg.kafkaBroker == "" {
		t.Error("kafkaBroker should not be empty when read from config.yml")
	}
	if cfg.numWorkers <= 0 {
		t.Errorf("numWorkers should be > 0, got %d", cfg.numWorkers)
	}
	if cfg.timeout <= 0 {
		t.Errorf("timeout should be > 0, got %v", cfg.timeout)
	}
}
