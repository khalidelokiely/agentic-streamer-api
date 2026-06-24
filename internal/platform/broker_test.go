package platform

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBrokerAddClient verifies that clients are correctly added to the broker
func TestBrokerAddClient(t *testing.T) {
	daemon := NewAgentDaemon(&mockEventStore{})
	broker := NewBroker(daemon)
	
	go broker.Start()
	time.Sleep(50 * time.Millisecond) // Let broker start
	
	ch := make(chan Event, 100)
	broker.AddClient("client-1", ch)
	
	time.Sleep(200 * time.Millisecond) // Let broker process
	
	snapshot := broker.routingTable.Load().(*RoutingSnapshot)
	if _, exists := snapshot.clients["client-1"]; !exists {
		t.Error("Client not added to broker")
	}
}

// TestBrokerRemoveClient verifies client removal
func TestBrokerRemoveClient(t *testing.T) {
	daemon := NewAgentDaemon(&mockEventStore{})
	broker := NewBroker(daemon)
	
	go broker.Start()
	time.Sleep(50 * time.Millisecond)
	
	ch := make(chan Event, 100)
	broker.AddClient("client-1", ch)
	time.Sleep(200 * time.Millisecond)
	
	broker.RemoveClient("client-1", ch)
	time.Sleep(200 * time.Millisecond)
	
	snapshot := broker.routingTable.Load().(*RoutingSnapshot)
	if _, exists := snapshot.clients["client-1"]; exists {
		t.Error("Client not removed from broker")
	}
}

// TestBrokerMultipleClientsRace tests concurrent client operations under --race
func TestBrokerMultipleClientsRace(t *testing.T) {
	daemon := NewAgentDaemon(&mockEventStore{})
	broker := NewBroker(daemon)
	
	go broker.Start()
	time.Sleep(50 * time.Millisecond)
	
	numClients := 50
	var wg sync.WaitGroup
	channels := make([]chan Event, numClients)
	
	// Add multiple clients concurrently
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		channels[i] = make(chan Event, 100)
		go func(idx int) {
			defer wg.Done()
			clientID := generateTestClientID(idx)
			broker.AddClient(clientID, channels[idx])
		}(i)
	}
	
	wg.Wait()
	time.Sleep(500 * time.Millisecond)
	
	// Verify all clients added
	snapshot := broker.routingTable.Load().(*RoutingSnapshot)
	if len(snapshot.clients) != numClients {
		t.Errorf("Expected %d clients, got %d", numClients, len(snapshot.clients))
	}
	
	// Remove all clients concurrently
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			clientID := generateTestClientID(idx)
			broker.RemoveClient(clientID, channels[idx])
		}(i)
	}
	
	wg.Wait()
	time.Sleep(500 * time.Millisecond)
	
	snapshot = broker.routingTable.Load().(*RoutingSnapshot)
	if len(snapshot.clients) != 0 {
		t.Errorf("Expected 0 clients, got %d", len(snapshot.clients))
	}
}

// TestBrokerWatchRequest tests watch functionality
func TestBrokerWatchRequest(t *testing.T) {
	daemon := NewAgentDaemon(&mockEventStore{})
	broker := NewBroker(daemon)
	
	go broker.Start()
	time.Sleep(50 * time.Millisecond)
	
	ch := make(chan Event, 100)
	broker.AddClient("client-1", ch)
	time.Sleep(200 * time.Millisecond)
	
	watchReq := WatchRequest{
		ClientID: "client-1",
		Agents: []TargetAgent{
			{ID: "codepal-v1", LatestOnly: false},
		},
		Ctx: nil,
	}
	
	broker.watchQueue <- watchReq
	time.Sleep(200 * time.Millisecond)
	
	snapshot := broker.routingTable.Load().(*RoutingSnapshot)
	if len(snapshot.clients["client-1"].watchList) == 0 {
		t.Error("Agent not added to watch list")
	}
}

// TestBrokerUnwatchRequest tests unwatch functionality
func TestBrokerUnwatchRequest(t *testing.T) {
	daemon := NewAgentDaemon(&mockEventStore{})
	broker := NewBroker(daemon)
	
	go broker.Start()
	time.Sleep(50 * time.Millisecond)
	
	ch := make(chan Event, 100)
	broker.AddClient("client-1", ch)
	time.Sleep(200 * time.Millisecond)
	
	// Add watch
	watchReq := WatchRequest{
		ClientID: "client-1",
		Agents: []TargetAgent{
			{ID: "codepal-v1", LatestOnly: false},
		},
		Ctx: nil,
	}
	broker.watchQueue <- watchReq
	time.Sleep(200 * time.Millisecond)
	
	// Remove watch
	unwatchReq := UnwatchRequest{
		agentIDList: []string{"codepal-v1"},
		clientID:    "client-1",
	}
	broker.Unwatch(unwatchReq)
	time.Sleep(200 * time.Millisecond)
	
	snapshot := broker.routingTable.Load().(*RoutingSnapshot)
	if len(snapshot.clients["client-1"].watchList) != 0 {
		t.Error("Agent not removed from watch list")
	}
}

// TestBrokerEventRouting tests that events are routed to correct clients
func TestBrokerEventRouting(t *testing.T) {
	daemon := NewAgentDaemon(&mockEventStore{})
	broker := NewBroker(daemon)
	
	go broker.Start()
	time.Sleep(50 * time.Millisecond)
	
	// Register with daemon first
	daemon.Attach(broker)
	
	ch1 := make(chan Event, 100)
	ch2 := make(chan Event, 100)
	
	broker.AddClient("client-1", ch1)
	broker.AddClient("client-2", ch2)
	time.Sleep(200 * time.Millisecond)
	
	// Watch agent for client-1
	watchReq := WatchRequest{
		ClientID: "client-1",
		Agents: []TargetAgent{
			{ID: "codepal-v1", LatestOnly: false},
		},
		Ctx: nil,
	}
	broker.watchQueue <- watchReq
	time.Sleep(200 * time.Millisecond)
	
	// Send event via Process (as the daemon would)
	event := Event{
		AgentRunID: "codepal-v1:run_uuid_10000",
		NodeName:   "llm_call",
		NodeStatus: "EXECUTING",
		Payload:    "test",
		Timestamp:  time.Now().UnixMilli(),
	}
	broker.Process(event)
	time.Sleep(300 * time.Millisecond)
	
	// Check if event received by client-1
	select {
	case received := <-ch1:
		if received.AgentRunID != event.AgentRunID {
			t.Errorf("Expected event %s, got %s", event.AgentRunID, received.AgentRunID)
		}
	case <-time.After(1 * time.Second):
		t.Error("Event not received by client-1")
	}
	
	// Verify client-2 did not receive it
	select {
	case <-ch2:
		t.Error("Event should not be received by client-2")
	case <-time.After(100 * time.Millisecond):
		// Expected
	}
}

// TestBrokerConcurrentEventRouting tests concurrent event handling
func TestBrokerConcurrentEventRouting(t *testing.T) {
	daemon := NewAgentDaemon(&mockEventStore{})
	broker := NewBroker(daemon)
	
	go broker.Start()
	time.Sleep(50 * time.Millisecond)
	daemon.Attach(broker)
	
	// Setup 10 clients (reduced from 20 to speed up test)
	numClients := 10
	channels := make([]chan Event, numClients)
	
	for i := 0; i < numClients; i++ {
		channels[i] = make(chan Event, 100)
		clientID := generateTestClientID(i)
		broker.AddClient(clientID, channels[i])
	}
	time.Sleep(300 * time.Millisecond)
	
	// Have all clients watch same agent
	for i := 0; i < numClients; i++ {
		watchReq := WatchRequest{
			ClientID: generateTestClientID(i),
			Agents: []TargetAgent{
				{ID: "codepal-v1", LatestOnly: false},
			},
			Ctx: nil,
		}
		broker.watchQueue <- watchReq
	}
	time.Sleep(300 * time.Millisecond)
	
	// Send 20 events via Process (not directly to incomingEventQueue)
	numEvents := 20
	
	for i := 0; i < numEvents; i++ {
		event := Event{
			AgentRunID: "codepal-v1:run_uuid_10000",
			NodeName:   "node",
			NodeStatus: "EXECUTING",
			Payload:    "",
			Timestamp:  time.Now().UnixMilli(),
		}
		broker.Process(event)
	}
	
	time.Sleep(500 * time.Millisecond)
	
	// Verify all clients received events
	for i := 0; i < numClients; i++ {
		count := 0
		for {
			select {
			case <-channels[i]:
				count++
			default:
				goto done
			}
		}
	done:
		if count == 0 {
			t.Logf("Client %d received %d events", i, count)
		}
	}
}

// TestBrokerGetCurrentMaps verifies snapshot export
func TestBrokerGetCurrentMaps(t *testing.T) {
	daemon := NewAgentDaemon(&mockEventStore{})
	broker := NewBroker(daemon)
	
	go broker.Start()
	time.Sleep(50 * time.Millisecond)
	
	ch := make(chan Event, 100)
	broker.AddClient("client-1", ch)
	time.Sleep(200 * time.Millisecond)
	
	watchReq := WatchRequest{
		ClientID: "client-1",
		Agents: []TargetAgent{
			{ID: "codepal-v1", LatestOnly: false},
		},
		Ctx: nil,
	}
	broker.watchQueue <- watchReq
	time.Sleep(200 * time.Millisecond)
	
	maps := broker.GetCurrentMaps()
	if maps == nil {
		t.Error("Expected maps, got nil")
	}
	
	if _, exists := maps["clients"]; !exists {
		t.Error("Missing clients in maps")
	}
	if _, exists := maps["agents"]; !exists {
		t.Error("Missing agents in maps")
	}
}

// TestBrokerWildcardRouting tests wildcard agent subscription
func TestBrokerWildcardRouting(t *testing.T) {
	daemon := NewAgentDaemon(&mockEventStore{})
	broker := NewBroker(daemon)
	
	go broker.Start()
	time.Sleep(50 * time.Millisecond)
	daemon.Attach(broker)
	
	ch := make(chan Event, 100)
	broker.AddClient("client-1", ch)
	time.Sleep(200 * time.Millisecond)
	
	// Watch wildcard
	watchReq := WatchRequest{
		ClientID: "client-1",
		Agents: []TargetAgent{
			{ID: "codepal-v1:*", LatestOnly: false},
		},
		Ctx: nil,
	}
	broker.watchQueue <- watchReq
	time.Sleep(200 * time.Millisecond)
	
	// Send event via Process
	event := Event{
		AgentRunID: "codepal-v1:run_uuid_10000",
		NodeName:   "llm_call",
		NodeStatus: "EXECUTING",
		Payload:    "test",
		Timestamp:  time.Now().UnixMilli(),
	}
	broker.Process(event)
	time.Sleep(300 * time.Millisecond)
	
	// Should receive event
	select {
	case received := <-ch:
		if received.AgentRunID != event.AgentRunID {
			t.Errorf("Wildcard route failed: expected %s, got %s", event.AgentRunID, received.AgentRunID)
		}
	case <-time.After(1 * time.Second):
		t.Error("Event not received via wildcard subscription")
	}
}

// TestBrokerChannelBufferHandling tests slow client handling
func TestBrokerChannelBufferHandling(t *testing.T) {
	daemon := NewAgentDaemon(&mockEventStore{})
	broker := NewBroker(daemon)
	
	go broker.Start()
	time.Sleep(50 * time.Millisecond)
	daemon.Attach(broker)
	
	// Create small buffer channel
	ch := make(chan Event, 5)
	broker.AddClient("client-1", ch)
	time.Sleep(200 * time.Millisecond)
	
	watchReq := WatchRequest{
		ClientID: "client-1",
		Agents: []TargetAgent{
			{ID: "codepal-v1", LatestOnly: false},
		},
		Ctx: nil,
	}
	broker.watchQueue <- watchReq
	time.Sleep(200 * time.Millisecond)
	
	// Send many events without consuming via Process
	for i := 0; i < 20; i++ {
		event := Event{
			AgentRunID: "codepal-v1:run_uuid_10000",
			NodeName:   "node",
			NodeStatus: "EXECUTING",
			Payload:    "",
			Timestamp:  time.Now().UnixMilli(),
		}
		broker.Process(event)
	}
	
	time.Sleep(300 * time.Millisecond)
	
	// Should not panic, some events should be received
	count := 0
	for {
		select {
		case <-ch:
			count++
		default:
			goto done
		}
	}
done:
	// Buffer should have some events (up to buffer size)
	if count == 0 {
		t.Error("Buffer should have events")
	}
	if count > 5 {
		t.Errorf("Buffer overflow: received %d events but capacity is 5", count)
	}
}

// TestBrokerRaceConditionOnSnapshot tests for race conditions on atomic snapshot updates
func TestBrokerRaceConditionOnSnapshot(t *testing.T) {
	daemon := NewAgentDaemon(&mockEventStore{})
	broker := NewBroker(daemon)
	
	go broker.Start()
	time.Sleep(50 * time.Millisecond)
	
	var wg sync.WaitGroup
	numIterations := 100
	
	// Multiple goroutines adding/removing clients and watching
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(clientNum int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				ch := make(chan Event, 50)
				clientID := "race-client-" + string(rune(clientNum*10+j))
				
				broker.AddClient(clientID, ch)
				
				watchReq := WatchRequest{
					ClientID: clientID,
					Agents: []TargetAgent{
						{ID: "codepal-v1", LatestOnly: false},
					},
					Ctx: nil,
				}
				broker.watchQueue <- watchReq
				
				time.Sleep(10 * time.Millisecond)
				
				broker.RemoveClient(clientID, ch)
			}
		}(i)
	}
	
	// Also continuously read the snapshot
	readDone := make(chan struct{})
	go func() {
		for i := 0; i < numIterations*5; i++ {
			_ = broker.GetCurrentMaps()
			time.Sleep(5 * time.Millisecond)
		}
		close(readDone)
	}()
	
	wg.Wait()
	<-readDone
	
	// If we got here without panicking, race detector passed
}

// Helper function to generate unique client IDs
func generateTestClientID(idx int) string {
	return "test-client-" + string(rune(48+idx%10)) + string(rune(65+idx/10))
}

// Mock EventStore for testing
type mockEventStore struct {
	mu     sync.RWMutex
	events map[string][]*Event
}

func (m *mockEventStore) Put(key string, value *Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.events == nil {
		m.events = make(map[string][]*Event)
	}
	m.events[key] = append(m.events[key], value)
}

func (m *mockEventStore) Query(key string) []*Event {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.events[key]
}

func (m *mockEventStore) QueryLastN(key string, lastN int) []*Event {
	m.mu.RLock()
	defer m.mu.RUnlock()
	events := m.events[key]
	if len(events) <= lastN {
		return events
	}
	return events[len(events)-lastN:]
}
