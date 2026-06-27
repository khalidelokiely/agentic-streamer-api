// Copyright 2026 The Agentic Streamer Authors.
// SPDX-License-Identifier: Apache-2.0

// Package main serves as the primary operational entry point for the
// Near-Real-Time (NRT) Agentic Streamer API engine. It orchestrates the
// dependency injection graph via Uber Fx, bootstraps the high-performance
// Hertz HTTP frame architecture, and provisions global middleware policies.
//
// Operational Architecture Overview:
//
//	[HTTP/SSE Client] <---> [Hertz Router Layer] <---> [Broker Coordinator] <---> [AgentDaemon State]
//	                                                                                     │
//	                                                                             [MemTable Engine]
//
// Data Flow Sequence:
//  1. Client establishes long-polling or Server-Sent Events (SSE) connections.
//  2. The Broker provisions an isolated event loop bucket and a dedicated thread-safe channel for the worker.
//  3. Downstream mutation payloads cross into the input command bus, route through processing goroutines,
//     and write mutations out to the concurrent MemTable tracking engine.
package main

import (
	"context"
	"nrt-agentic-streamer-api/internal/config"
	"nrt-agentic-streamer-api/internal/platform"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/hertz-contrib/cors"
	"go.uber.org/fx"
)

// TODO(khalidelokiely): Extract HTTP route definitions and CORS initialization
// into a dedicated internal/network or internal/handler routing module to
// keep main.go strictly limited to dependency graph wiring.

func main() {
	appGraph := fx.New(
		// Provide foundational system platform configurations, storage components,
		// and background state coordinators.
		platform.Module,
		config.Module,

		// Inject and configure the Hertz HTTP server framework within the active
		// application infrastructure lifecycle.
		fx.Provide(func(lc fx.Lifecycle, config *config.Config) *server.Hertz {
			// Resolve target deployment port binding.
			h := server.Default(server.WithHostPorts(":" + config.Port))

			// Configure Cross-Origin Resource Sharing (CORS) security context policies.
			h.Use(cors.New(cors.Config{
				AllowOrigins:     config.AllowedOrigins,
				AllowMethods:     []string{"GET", "POST", "DELETE", "OPTIONS"},
				AllowHeaders:     []string{"Origin", "Content-Type"},
				ExposeHeaders:    []string{"Content-Type"},
				AllowCredentials: false,
				MaxAge:           12 * time.Hour,
			}))

			// Health check endpoint used by orchestration engine probes (e.g., Railway, Kubernetes)
			// to determine operational readiness and liveness.
			h.GET("/health", func(ctx context.Context, c *app.RequestContext) {
				c.String(200, "OK")
			})

			// Bind engine lifecycle execution policies directly to the Uber Fx kernel context.
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					// NOTE: h.Spin() blocks the execution path while listening for TCP packets.
					// Running inside a dedicated goroutine prevents initialization timeouts during application startup.
					go h.Spin()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					// Gracefully drain connections, terminate SSE stream handles, and close listening sockets.
					return h.Shutdown(ctx)
				},
			})

			return h
		}),

		// Invoke the initialization path to force instantiation of the primary
		// server loop inside the dependency tree.
		fx.Invoke(func(*server.Hertz) {}),
	)

	// Block the main thread, start background daemons, and wait for interrupt signals.
	appGraph.Run()
}
