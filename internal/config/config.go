package config

import (
	"os"
	"strings"
)

// Config holds all environment variables in a single, type-safe struct.
type Config struct {
	Port           string
	AllowedOrigins []string
}

// NewConfig initializes the configuration provider.
// This isolates os.Getenv away from your network/server logic.
func NewConfig() *Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "80"
	}

	var origins []string
	if rawOrigins := os.Getenv("ALLOWED_ORIGINS"); rawOrigins != "" {
		origins = strings.Split(rawOrigins, ",")
	} else {
		origins = []string{"http://localhost:5173", "http://localhost:4173"}
		if prodFrontend := os.Getenv("FRONTEND_URL"); prodFrontend != "" {
			origins = append(origins, prodFrontend)
		}
	}

	return &Config{
		Port:           port,
		AllowedOrigins: origins,
	}
}
