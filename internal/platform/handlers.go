package platform

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/sse"
	"github.com/oklog/ulid/v2"
)

type Handler struct {
	agentDaemon DaemonController
	broker      *Broker
}

func NewHandler(agentDaemon DaemonController, broker *Broker) *Handler {
	go broker.Start()
	go agentDaemon.Start()

	return &Handler{
		agentDaemon: agentDaemon,
		broker:      broker,
	}
}

func (h *Handler) GetAvailableAgents(ctx context.Context, c *app.RequestContext) {
	c.JSON(200, h.agentDaemon.GetAgents())
	return
}

func (h *Handler) GetAvailableAgentRuns(ctx context.Context, c *app.RequestContext) {
	agentId := c.Param("agentId")
	c.JSON(200, h.agentDaemon.GetAgentRuns(agentId))
	return
}

func (h *Handler) GetAvailableAgentRunEvents(xtx context.Context, c *app.RequestContext) {
	agentRunId := c.Param("agentId") + ":" + c.Param("runId")
	c.JSON(200, h.agentDaemon.GetAgentRunEvents(agentRunId))
	return
}

func (h *Handler) SSE(ctx context.Context, c *app.RequestContext) {
	clientId := c.Query("clientId")
	if clientId == "" {
		c.JSON(400, map[string]string{"error": "missing client id"})
		return
	}

	w := sse.NewWriter(c)

	// 2. Clearer defer layout: Close the writer when the stream terminates
	defer func() {
		_ = w.Close()
	}()

	// Use a buffered channel to prevent slow clients from blocking the broker lock
	clientChannel := make(chan Event, 100)

	h.broker.AddClient(clientId, clientChannel)

	// 3. CRITICAL FIX: Clean up the broker registry automatically when the handler exits!
	defer h.broker.RemoveClient(clientId)

	// Send initial handshake to establish connection visibility
	if err := w.WriteEvent("0", "HELLO", []byte("Hello Visitor")); err != nil {
		return // Exit immediately if client dropped during initial setup
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled, tearing down connection for client: %s\n", clientId)
			return

		case event, ok := <-clientChannel:
			if !ok {
				// Safe channel drain check in case the broker closes the channel explicitly
				return
			}

			// Execute write operation
			err := w.WriteEvent(generateULID(), "SSE_EVENT_AGENT_RUN_WATCH", event.Bytes())
			if err != nil {
				// 4. CRITICAL FIX: Changed 'continue' to 'return'.
				// If a write fails, the connection is dead. We must clean up and terminate.
				fmt.Printf("Client connection lost. Terminating stream: %v, Client: %s\n", err, clientId)
				return
			}
		}
	}
}

func (h *Handler) WatchAgentsRequest(ctx context.Context, c *app.RequestContext) {
	var watchRequest WatchRequest

	err := c.BindJSON(&watchRequest)
	if err != nil {
		c.JSON(400, map[string]string{"error": err.Error()})
		return
	}

	watchRequest.Ctx = ctx

	h.broker.watchQueue <- watchRequest

	c.JSON(201, map[string]interface{}{"message": "Agent(s) added to watch list", "payload": watchRequest})
}

func (h *Handler) GetCurrentWatchersAndClients(ctx context.Context, c *app.RequestContext) {
	c.JSON(200, h.broker.GetCurrentMaps())
}

func (h *Handler) UnwatchAgentRequest(ctx context.Context, c *app.RequestContext) {
	agentId := c.Param("agentId")
	clientId := c.Query("clientId")

	if agentId == "" || clientId == "" {
		c.JSON(400, map[string]string{"error": "missing agent id or client id"})
		return
	}

	req := UnwatchRequest{
		agentIDList: []string{agentId},
		clientID:    clientId,
	}

	h.broker.Unwatch(req)
}

func generateULID() string {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	ms := ulid.Timestamp(time.Now())
	ulid, err := ulid.New(ms, entropy)

	if err != nil {
		panic(err)
	}

	return ulid.String()
}
