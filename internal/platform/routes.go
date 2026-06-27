// Copyright 2026 The Agentic Streamer Authors.
// SPDX-License-Identifier: Apache-2.0

package platform

import (
	"github.com/cloudwego/hertz/pkg/app/server"
)

// RegisterRoutes registers endpoints into the Hertz HTTP multiplexer router engine.
func RegisterRoutes(s *server.Hertz, h *Handler) {
	rg := s.RouterGroup.Group("/v1/agents")
	{
		// --- METADATA AND INSPECTION --- //
		rg.GET("", h.GetAvailableAgents)
		rg.GET("/:agentId/runs", h.GetAvailableAgentRuns)
		rg.GET("/:agentId/runs/:runId/events", h.GetAvailableAgentRunEvents)

		// --- STREAMING & SUBSCRIPTIONS --- //
		rg.GET("/sse", h.SSE)
		rg.POST("/watch", h.WatchAgentsRequest)
		rg.POST("/unwatch", h.UnwatchAgentRequest) // Shifted to POST to accept batch JSON bodies safely

		// --- SYSTEM DIAGNOSTICS --- //
		rg.GET("/watchers", h.GetCurrentWatchersAndClients)
	}
}
