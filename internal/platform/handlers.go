// Copyright 2026 The Agentic Streamer Authors.
// SPDX-License-Identifier: Apache-2.0

package platform

import (
	"context"
	"log"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/protocol/sse"
	"github.com/oklog/ulid/v2"
)

// Handler bridges incoming HTTP requests from the Hertz web surface to core daemons.
type Handler struct {
	agentDaemon DaemonController
	broker      *Broker
}

// NewHandler constructs a unified HTTP handler controller.
func NewHandler(agentDaemon DaemonController, broker *Broker) *Handler {
	return &Handler{
		agentDaemon: agentDaemon,
		broker:      broker,
	}
}

// GetAvailableAgents handles GET /v1/agents
func (h *Handler) GetAvailableAgents(ctx context.Context, c *app.RequestContext) {
	c.JSON(consts.StatusOK, h.agentDaemon.GetAgents())
}

// GetAvailableAgentRuns handles GET /v1/agents/:agentId/runs
func (h *Handler) GetAvailableAgentRuns(ctx context.Context, c *app.RequestContext) {
	agentID := c.Param("agentId")
	if agentID == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "missing path parameter: agentId"})
		return
	}
	c.JSON(consts.StatusOK, h.agentDaemon.GetAgentRuns(agentID))
}

// GetAvailableAgentRunEvents handles GET /v1/agents/:agentId/runs/:runId/events
func (h *Handler) GetAvailableAgentRunEvents(ctx context.Context, c *app.RequestContext) {
	agentID := c.Param("agentId")
	runID := c.Param("runId")
	if agentID == "" || runID == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "malformed path coordinates"})
		return
	}

	compositeID := AgentRunID(agentID + ":" + runID)
	c.JSON(consts.StatusOK, h.agentDaemon.GetAgentRunEvents(compositeID))
}

// SSE provisions persistent stream hooks for Server-Sent Events configurations.
func (h *Handler) SSE(ctx context.Context, c *app.RequestContext) {
	clientID := c.Query("clientId")
	if clientID == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "missing client id query parameter"})
		return
	}

	w := sse.NewWriter(c)

	// Clearer defer layout: Close the writer when the stream terminates
	defer func() {
		_ = w.Close()
	}()

	// Buffered channel to prevent slow consumers from backing up coordinator loops
	clientChannel := make(chan Event, 100)

	h.broker.AddClient(clientID, clientChannel)

	// Clean up the broker registry automatically when the handler exits
	defer h.broker.RemoveClient(clientID, clientChannel)

	// Send initial handshake to establish connection visibility
	if err := w.WriteEvent("0", "HELLO", []byte("Hello Visitor")); err != nil {
		return // Exit immediately if client dropped during initial setup
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled, tearing down connection for client: %s\n", clientID)
			return

		case event, ok := <-clientChannel:
			if !ok {
				return
			}

			// Allocation-Free Performance Fix: Replaced custom generator with native ulid.Make()
			eventID := ulid.Make().String()
			err := w.WriteEvent(eventID, "SSE_EVENT_AGENT_RUN_WATCH", event.Bytes())
			if err != nil {
				log.Printf("Client connection lost. Terminating stream: %v, Client: %s\n", err, clientID)
				return
			}
		}
	}
}

// WatchAgentsRequest handles POST /v1/agents/watch
func (h *Handler) WatchAgentsRequest(ctx context.Context, c *app.RequestContext) {
	var watchRequest WatchRequest

	if err := c.BindJSON(&watchRequest); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	watchRequest.Ctx = ctx
	h.broker.watchQueue <- watchRequest

	c.JSON(consts.StatusCreated, map[string]interface{}{
		"message": "Agent(s) added to watch list",
		"payload": watchRequest,
	})
}

// UnwatchAgentRequest handles POST /v1/agents/unwatch (Polished Batch Payload Pipeline)
func (h *Handler) UnwatchAgentRequest(ctx context.Context, c *app.RequestContext) {
	var unwatchRequest UnwatchRequest

	// Bind request JSON containing ClientID and AgentRunIDs array directly
	if err := c.BindJSON(&unwatchRequest); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if unwatchRequest.ClientID == "" || len(unwatchRequest.AgentRunIDs) == 0 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "invalid client_id or missing target run ids array"})
		return
	}

	h.broker.Unwatch(unwatchRequest)

	c.JSON(consts.StatusOK, map[string]interface{}{
		"message": "Unwatch request processed",
		"payload": unwatchRequest,
	})
}

// GetCurrentWatchersAndClients handles GET /v1/agents/watchers
func (h *Handler) GetCurrentWatchersAndClients(ctx context.Context, c *app.RequestContext) {
	c.JSON(consts.StatusOK, h.broker.GetCurrentMaps())
}
