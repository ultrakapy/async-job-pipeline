package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port        string
	WorkerCount int
	QueueCap    int
}

func Load() *Config {
	return &Config{
		Port:        getEnv("PORT", "8080"),
		WorkerCount: getIntEnv("WORKER_COUNT", 4),
		QueueCap:    getIntEnv("QUEUE_CAPACITY", 1000),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getIntEnv(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
