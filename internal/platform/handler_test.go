package platform

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestHandlerGetAvailableAgents tests getting list of agents
func TestHandlerGetAvailableAgents(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore)
	broker := NewBroker(daemon)
	handler := NewHandler(daemon, broker)
	
	// Need to wait for handler to spawn broker and daemon
	time.Sleep(200 * time.Millisecond)
	
	// Create mock request context (simplified - just test the handler method directly)
	// We can't easily test HTTP handlers without importing hertz test utilities
	agents := daemon.GetAgents()
	if agents == nil {
		t.Error("Agents map should not be nil")
	}
}

// TestHandlerSSEClientAddition tests that SSE handler adds clients to broker
func TestHandlerSSEClientAddition(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore)
	broker := NewBroker(daemon)
	handler := NewHandler(daemon, broker)
	
	time.Sleep(200 * time.Millisecond)
	
	// Simulate what the SSE handler does: create a channel and add client
	clientChan := make(chan Event, 100)
	broker.AddClient("sse-client-1", clientChan)
	time.Sleep(100 * time.Millisecond)
	
	snapshot := broker.routingTable.Load().(*RoutingSnapshot)
	if _, exists := snapshot.clients["sse-client-1"]; !exists {
		t.Error("SSE client not added to broker")
	}
}

// TestHandlerWatchRequest tests watch request handling
func TestHandlerWatchRequest(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore)
	broker := NewBroker(daemon)
	handler := NewHandler(daemon, broker)
	
	time.Sleep(200 * time.Millisecond)
	
	// Simulate watch request
	clientChan := make(chan Event, 100)
	broker.AddClient("client-1", clientChan)
	time.Sleep(100 * time.Millisecond)
	
	watchReq := WatchRequest{
		ClientID: "client-1",
		Agents: []TargetAgent{
			{ID: "codepal-v1:*", LatestOnly: false},
		},
		Ctx: context.Background(),
	}
	
	broker.watchQueue <- watchReq
	time.Sleep(100 * time.Millisecond)
	
	snapshot := broker.routingTable.Load().(*RoutingSnapshot)
	if len(snapshot.clients["client-1"].watchList) == 0 {
		t.Error("Watch request not processed")
	}
}

// TestHandlerUnwatchRequest tests unwatch request handling
func TestHandlerUnwatchRequest(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore)
	broker := NewBroker(daemon)
	handler := NewHandler(daemon, broker)
	
	time.Sleep(200 * time.Millisecond)
	
	// Add client
	clientChan := make(chan Event, 100)
	broker.AddClient("client-1", clientChan)
	time.Sleep(100 * time.Millisecond)
	
	// Watch
	watchReq := WatchRequest{
		ClientID: "client-1",
		Agents: []TargetAgent{
			{ID: "codepal-v1:*", LatestOnly: false},
		},
		Ctx: context.Background(),
	}
	broker.watchQueue <- watchReq
	time.Sleep(100 * time.Millisecond)
	
	// Unwatch
	unwatchReq := UnwatchRequest{
		agentIDList: []string{"codepal-v1:*"},
		clientID:    "client-1",
	}
	broker.Unwatch(unwatchReq)
	time.Sleep(100 * time.Millisecond)
	
	snapshot := broker.routingTable.Load().(*RoutingSnapshot)
	if len(snapshot.clients["client-1"].watchList) != 0 {
		t.Error("Unwatch request not processed")
	}
}

// TestHandlerConcurrentClients tests handler behavior with concurrent clients
func TestHandlerConcurrentClients(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore)
	broker := NewBroker(daemon)
	handler := NewHandler(daemon, broker)
	
	time.Sleep(200 * time.Millisecond)
	
	var wg sync.WaitGroup
	numClients := 30
	
	// Add clients concurrently
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			clientID := "concurrent-client-" + string(rune(48+idx%10))
			clientChan := make(chan Event, 100)
			broker.AddClient(clientID, clientChan)
			
			// Immediately watch
			watchReq := WatchRequest{
				ClientID: clientID,
				Agents: []TargetAgent{
					{ID: "codepal-v1:*", LatestOnly: false},
				},
				Ctx: context.Background(),
			}
			broker.watchQueue <- watchReq
		}(i)
	}
	
	wg.Wait()
	time.Sleep(300 * time.Millisecond)
	
	snapshot := broker.routingTable.Load().(*RoutingSnapshot)
	if len(snapshot.clients) == 0 {
		t.Error("Concurrent clients not added")
	}
}

// TestHandlerEventProcessing tests that events flow through handler correctly
func TestHandlerEventProcessing(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore)
	broker := NewBroker(daemon)
	handler := NewHandler(daemon, broker)
	
	time.Sleep(200 * time.Millisecond)
	daemon.Attach(broker)
	
	// Add and watch client
	clientChan := make(chan Event, 100)
	broker.AddClient("event-client", clientChan)
	time.Sleep(100 * time.Millisecond)
	
	watchReq := WatchRequest{
		ClientID: "event-client",
		Agents: []TargetAgent{
			{ID: "codepal-v1:*", LatestOnly: false},
		},
		Ctx: context.Background(),
	}
	broker.watchQueue <- watchReq
	time.Sleep(100 * time.Millisecond)
	
	// Send event through broker (as daemon would)
	event := Event{
		AgentRunID: "codepal-v1:run_uuid_10000",
		NodeName:   "llm_call",
		NodeStatus: "EXECUTING",
		Payload:    "test event",
		Timestamp:  time.Now().UnixMilli(),
	}
	broker.Process(event)
	time.Sleep(300 * time.Millisecond)
	
	// Client should have received event
	select {
	case received := <-clientChan:
		if received.Payload != "test event" {
			t.Errorf("Wrong event received: %s", received.Payload)
		}
	case <-time.After(1 * time.Second):
		t.Error("Event not received by client")
	}
}

// TestHandlerRaceConditionOnConcurrentOperations tests race conditions in handler flow
func TestHandlerRaceConditionOnConcurrentOperations(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore)
	broker := NewBroker(daemon)
	handler := NewHandler(daemon, broker)
	
	time.Sleep(200 * time.Millisecond)
	daemon.Attach(broker)
	
	var wg sync.WaitGroup
	numOperations := 50
	
	// Concurrent client management
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOperations/3; j++ {
				clientID := "race-test-" + string(rune(48+workerID)) + "-" + string(rune(48+j%10))
				clientChan := make(chan Event, 50)
				
				broker.AddClient(clientID, clientChan)
				
				watchReq := WatchRequest{
					ClientID: clientID,
					Agents: []TargetAgent{
						{ID: "codepal-v1:*", LatestOnly: false},
					},
					Ctx: context.Background(),
				}
				broker.watchQueue <- watchReq
				
				time.Sleep(5 * time.Millisecond)
				
				broker.RemoveClient(clientID, clientChan)
			}
		}(i)
	}
	
	// Concurrent event sending
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numOperations; i++ {
			event := Event{
				AgentRunID: "codepal-v1:run_uuid_10000",
				NodeName:   "node",
				NodeStatus: "EXECUTING",
				Payload:    "",
				Timestamp:  time.Now().UnixMilli(),
			}
			broker.Process(event)
			time.Sleep(2 * time.Millisecond)
		}
	}()
	
	// Concurrent reads
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numOperations; i++ {
			_ = broker.GetCurrentMaps()
			_ = daemon.GetAgents()
			time.Sleep(3 * time.Millisecond)
		}
	}()
	
	wg.Wait()
	time.Sleep(100 * time.Millisecond)
	
	// If we got here without panic or data race, test passed
}

// TestHandlerLatestOnlyFlag tests that LatestOnly flag works correctly
func TestHandlerLatestOnlyFlag(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore)
	broker := NewBroker(daemon)
	handler := NewHandler(daemon, broker)
	
	time.Sleep(200 * time.Millisecond)
	
	// Add client
	clientChan := make(chan Event, 100)
	broker.AddClient("latest-client", clientChan)
	time.Sleep(100 * time.Millisecond)
	
	// Watch with LatestOnly=true
	watchReq := WatchRequest{
		ClientID: "latest-client",
		Agents: []TargetAgent{
			{ID: "codepal-v1:*", LatestOnly: true},
		},
		Ctx: context.Background(),
	}
	broker.watchQueue <- watchReq
	time.Sleep(100 * time.Millisecond)
	
	// Watch should be in the watchList even with LatestOnly=true
	snapshot := broker.routingTable.Load().(*RoutingSnapshot)
	if len(snapshot.clients["latest-client"].watchList) == 0 {
		t.Error("Watch not added with LatestOnly=true")
	}
}

// TestHandlerMultipleAgentSubscriptions tests subscribing to multiple agents at once
func TestHandlerMultipleAgentSubscriptions(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore)
	broker := NewBroker(daemon)
	handler := NewHandler(daemon, broker)
	
	time.Sleep(200 * time.Millisecond)
	
	clientChan := make(chan Event, 100)
	broker.AddClient("multi-agent-client", clientChan)
	time.Sleep(100 * time.Millisecond)
	
	// Subscribe to multiple agents
	watchReq := WatchRequest{
		ClientID: "multi-agent-client",
		Agents: []TargetAgent{
			{ID: "codepal-v1:*", LatestOnly: false},
			{ID: "doc-bot:*", LatestOnly: false},
			{ID: "code-reviewer:*", LatestOnly: false},
		},
		Ctx: context.Background(),
	}
	broker.watchQueue <- watchReq
	time.Sleep(100 * time.Millisecond)
	
	snapshot := broker.routingTable.Load().(*RoutingSnapshot)
	if len(snapshot.clients["multi-agent-client"].watchList) != 3 {
		t.Errorf("Expected 3 subscriptions, got %d", len(snapshot.clients["multi-agent-client"].watchList))
	}
}
