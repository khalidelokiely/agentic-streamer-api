package platform

import (
	"github.com/cloudwego/hertz/pkg/app/server"
)

func RegisterRoutes(s *server.Hertz, h *Handler) {

	rg := s.RouterGroup.Group("/v1/agents")
	{
		// --- METADATA AND INSPECTION --- //
		rg.GET("", h.GetAvailableAgents)
		rg.GET("/:agentId/runs", h.GetAvailableAgentRuns)
		rg.GET("/:agentId/runs/:runId/events", h.GetAvailableAgentRunEvents)

		// --- STREAMING --- //
		rg.GET("/sse", h.SSE)
		rg.POST("/watch", h.WatchAgentsRequest)
		rg.DELETE("/watch/:agentId", h.UnwatchAgentRequest)

		// --- SYSTEM DIAG --- //
		rg.GET("/watchers", h.GetCurrentWatchersAndClients)
	}
}
