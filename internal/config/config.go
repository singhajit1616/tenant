package config

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"time"
)

type ControlPlane struct {
	HTTPAddr    string
	DatabaseURL string
	RabbitMQURL string
}

// LoadControlPlane reads HTTP, PostgreSQL, and RabbitMQ settings from the environment and
// rejects a missing database URL.
func LoadControlPlane() (ControlPlane, error) {
	cfg := ControlPlane{
		HTTPAddr:    getenv("HTTP_ADDR", ":8080"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		RabbitMQURL: getenv("RABBITMQ_URL", "amqp://tenant:tenant@127.0.0.1:5672/"),
	}
	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}

type Worker struct {
	RabbitMQURL string
	FailRate    float64
	MinDelay    time.Duration
	MaxDelay    time.Duration
}

// LoadWorker reads broker, failure-rate, and delay settings and rejects invalid
// probabilities or inconsistent delay bounds.
func LoadWorker() (Worker, error) {
	failRate, err := strconv.ParseFloat(getenv("FAIL_RATE", "0"), 64)
	if err != nil || math.IsNaN(failRate) || math.IsInf(failRate, 0) || failRate < 0 || failRate > 1 {
		return Worker{}, fmt.Errorf("FAIL_RATE must be between 0 and 1")
	}
	minMS, err := strconv.Atoi(getenv("MIN_DELAY_MS", "300"))
	if err != nil || minMS < 0 {
		return Worker{}, fmt.Errorf("MIN_DELAY_MS must be >= 0")
	}
	maxMS, err := strconv.Atoi(getenv("MAX_DELAY_MS", "800"))
	if err != nil || maxMS < minMS {
		return Worker{}, fmt.Errorf("MAX_DELAY_MS must be >= MIN_DELAY_MS")
	}
	return Worker{
		RabbitMQURL: getenv("RABBITMQ_URL", "amqp://tenant:tenant@127.0.0.1:5672/"),
		FailRate:    failRate,
		MinDelay:    time.Duration(minMS) * time.Millisecond,
		MaxDelay:    time.Duration(maxMS) * time.Millisecond,
	}, nil
}

// getenv returns a non-empty environment value or the supplied default.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
