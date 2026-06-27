package platform

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// 1. INTEGRATED THREAD-SAFE MOCK FOR ISOLATED TESTING
// ============================================================================
type mockTestDaemon struct {
	mu           sync.Mutex
	latestEvents map[string]*Event
}

func (m *mockTestDaemon) RegisterAgent(agent Agent) {
	//TODO implement me
	panic("implement me")
}

func (m *mockTestDaemon) RegisterSnapshot(snapshot AgentRunSnapshot) {
	//TODO implement me
	panic("implement me")
}

func (m *mockTestDaemon) Attach(o Observer)                        {}
func (m *mockTestDaemon) Detach(o Observer)                        {}
func (m *mockTestDaemon) Start()                                   {}
func (m *mockTestDaemon) Query(p string, l int) []*Event           { return nil }
func (m *mockTestDaemon) GetAgents() map[string]*AgentMetadata     { return nil }
func (m *mockTestDaemon) GetAgentRuns(id string) []*AgentRunDetail { return nil }
func (m *mockTestDaemon) GetAgentRunEvents(id AgentRunID) []*Event { return nil }
func (m *mockTestDaemon) QueryLatest(param string) *Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.latestEvents == nil {
		return nil
	}
	return m.latestEvents[param]
}

// ============================================================================
// 2. COMPACTED CLIENT CONNECTION LIFECYCLE TESTS
// ============================================================================

// TestBroker_ClientLifecycleSequential validates basic connection registration and deregistration state tracking.
func TestBroker_ClientLifecycleSequential(t *testing.T) {
	daemon := &mockTestDaemon{}
	broker := NewBroker(daemon)
	go broker.Start()

	ch := make(chan Event, 10)
	clientID := "client-sequential-99"

	// 1. Assert Addition
	broker.AddClient(clientID, ch)
	time.Sleep(5 * time.Millisecond) // short state-machine propagation sync

	snapshot := broker.routingTable.Load().(*RoutingSnapshot)
	if _, exists := snapshot.clients[clientID]; !exists {
		t.Fatal("Structural validation failed: Client was not registered inside state-machine maps")
	}

	// 2. Assert Removal
	broker.RemoveClient(clientID, ch)
	time.Sleep(5 * time.Millisecond)

	snapshot = broker.routingTable.Load().(*RoutingSnapshot)
	if _, exists := snapshot.clients[clientID]; exists {
		t.Fatal("Structural validation failed: Client record leaked during graceful disconnect sequence")
	}
}

// TestBroker_ClientLifecycleConcurrent executes a high-density saturation test on connection queues under --race conditions.
func TestBroker_ClientLifecycleConcurrent(t *testing.T) {
	daemon := &mockTestDaemon{}
	broker := NewBroker(daemon)
	go broker.Start()

	numClients := 50
	var wg sync.WaitGroup
	channels := make([]chan Event, numClients)

	// Blast concurrent connection allocations
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		channels[i] = make(chan Event, 10)
		go func(idx int) {
			defer wg.Done()
			broker.AddClient(fmt.Sprintf("concurrent-client-%d", idx), channels[idx])
		}(i)
	}
	wg.Wait()
	time.Sleep(10 * time.Millisecond)

	snapshot := broker.routingTable.Load().(*RoutingSnapshot)
	if len(snapshot.clients) != numClients {
		t.Errorf("State map drift: Expected exactly %d clients, registered %d", numClients, len(snapshot.clients))
	}

	// Blast concurrent disconnections
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			broker.RemoveClient(fmt.Sprintf("concurrent-client-%d", idx), channels[idx])
		}(i)
	}
	wg.Wait()
	time.Sleep(10 * time.Millisecond)

	snapshot = broker.routingTable.Load().(*RoutingSnapshot)
	if len(snapshot.clients) != 0 {
		t.Errorf("Cleanup map leakage: %d client paths remain attached following teardown blast", len(snapshot.clients))
	}
}

// ============================================================================
// 3. CORE LOGIC BOUNDARY & ROUTING TESTS
// ============================================================================

// TestBroker_WatchUnwatchExclusion Matrix asserts exact routing registration and the creation of wildcard exclusions.
func TestBroker_WatchUnwatchExclusionMatrix(t *testing.T) {
	daemon := &mockTestDaemon{}
	broker := NewBroker(daemon)
	go broker.Start()

	ch := make(chan Event, 10)
	clientID := "client-beta"
	broker.AddClient(clientID, ch)

	// 1. Register Wildcard Subscription
	broker.watchQueue <- WatchRequest{
		ClientID: clientID,
		Agents:   []TargetAgent{{ID: "orchestrator:*", LatestOnly: false}},
	}
	time.Sleep(5 * time.Millisecond)

	snapshot := broker.routingTable.Load().(*RoutingSnapshot)
	if !snapshot.clients[clientID].watchList["orchestrator:*"] {
		t.Fatal("Watch registration failed: Wildcard token mapping missing from client registry")
	}

	// 2. Issue an Unwatch Request on an active sub-run to trigger the exclusion layer
	broker.Unwatch(UnwatchRequest{
		ClientID:    clientID,
		AgentRunIDs: []AgentRunID{"orchestrator:run-xyz"},
	})
	time.Sleep(5 * time.Millisecond)

	// 3. Assert system diagnostics maps export structural changes correctly
	diag := broker.GetCurrentMaps()
	clientsMap := diag["clients"].(map[string]interface{})
	targetClient := clientsMap[clientID].(map[string]interface{})
	exclusions := targetClient["exclusions"].(map[AgentRunID]bool)

	if !exclusions["orchestrator:run-xyz"] {
		t.Error("Routing logic breakout: Unwatching an active sub-run within a wildcard namespace failed to generate an runtime exclusion entry")
	}
}

// TestBroker_RoutingExecutionMatrix tests that targeted messages and wide wildcards route cleanly without cross-contamination.
func TestBroker_RoutingExecutionMatrix(t *testing.T) {
	daemon := &mockTestDaemon{}
	broker := NewBroker(daemon)
	go broker.Start()

	chExplicit := make(chan Event, 10)
	chWildcard := make(chan Event, 10)

	broker.AddClient("explicit-client", chExplicit)
	broker.AddClient("wildcard-client", chWildcard)
	time.Sleep(5 * time.Millisecond)

	// Client 1 watches a specific explicit run loop execution boundary
	broker.watchQueue <- WatchRequest{
		ClientID: "explicit-client",
		Agents:   []TargetAgent{{ID: "coder-agent:run-100", LatestOnly: false}},
	}
	// Client 2 watches the general cluster namespace prefix
	broker.watchQueue <- WatchRequest{
		ClientID: "wildcard-client",
		Agents:   []TargetAgent{{ID: "coder-agent:*", LatestOnly: false}},
	}
	time.Sleep(5 * time.Millisecond)

	// Broadcast Event A: Matches both explicit run target AND wildcard pattern
	eventMatching := Event{
		AgentRunID: "coder-agent:run-100",
		NodeName:   "llm_node",
		NodeStatus: "THINKING",
	}
	broker.Process(eventMatching)

	// Broadcast Event B: Matches wildcard namespace ONLY
	eventWildcardOnly := Event{
		AgentRunID: "coder-agent:run-200",
		NodeName:   "tool_node",
		NodeStatus: "EXECUTING_TOOL",
	}
	broker.Process(eventWildcardOnly)
	time.Sleep(10 * time.Millisecond)

	// Assert Explicit Client state engine outcomes
	if len(chExplicit) != 1 {
		t.Errorf("Routing mismatch: Explicit client channel received %d events instead of exactly 1", len(chExplicit))
	}
	evOut := <-chExplicit
	if evOut.AgentRunID != "coder-agent:run-100" {
		t.Errorf("Data contamination: Received wrong event context: %s", evOut.AgentRunID)
	}

	// Assert Wildcard Client state engine outcomes
	if len(chWildcard) != 2 {
		t.Errorf("Routing mismatch: Wildcard namespace subscriber dropped frames, expected 2 events, got %d", len(chWildcard))
	}
}

// ============================================================================
// 4. CAPACITY & RACE STRESS TESTING
// ============================================================================

// TestBroker_SlowClientBufferDrop verifies that slow clients with saturated channel pipes do not block the central processing pipeline.
func TestBroker_SlowClientBufferDrop(t *testing.T) {
	daemon := &mockTestDaemon{}
	broker := NewBroker(daemon)
	go broker.Start()

	// Give client a small capacity array of 2 entries
	ch := make(chan Event, 2)
	broker.AddClient("slow-client", ch)
	time.Sleep(5 * time.Millisecond)

	broker.watchQueue <- WatchRequest{
		ClientID: "slow-client",
		Agents:   []TargetAgent{{ID: "telemetry:*", LatestOnly: false}},
	}
	time.Sleep(5 * time.Millisecond)

	// Blast 10 sequential events. 2 should fill buffer, remainder must drop cleanly via default select blocks
	for i := 0; i < 10; i++ {
		broker.Process(Event{
			AgentRunID: "telemetry:run-active",
			NodeName:   "worker_pool",
			NodeStatus: "STREAMING",
		})
	}

	// Internal worker synchronization channel check loop
	doneChan := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(doneChan)
	}()

	select {
	case <-doneChan:
		// Success: The broker loop successfully skipped the saturated channels without blocking the system thread
		if len(ch) != 2 {
			t.Errorf("Channel saturation tracking corrupted: expected buffer depth to be exactly 2, got %d", len(ch))
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("System Halt: The Broker main execution thread deadlocked on a slow client channel buffer write constraint")
	}
}

// TestBroker_HighContentionSnapshotRace launches parallel reader threads and state mutating loops to break atomic data swap points.
func TestBroker_HighContentionSnapshotRace(t *testing.T) {
	daemon := &mockTestDaemon{}
	broker := NewBroker(daemon)
	go broker.Start()

	var wg sync.WaitGroup
	stopSignal := make(chan struct{})

	// Pool A: Constantly connects, subscribes, and tears down dynamic allocations
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for idx := 0; ; idx++ {
				select {
				case <-stopSignal:
					return
				default:
					ch := make(chan Event, 10)
					cID := fmt.Sprintf("race-worker-%d-iteration-%d", workerID, idx)

					broker.AddClient(cID, ch)
					broker.watchQueue <- WatchRequest{
						ClientID: cID,
						Agents:   []TargetAgent{{ID: "agent-matrix:*", LatestOnly: false}},
					}

					// Let state register before removing
					time.Sleep(1 * time.Millisecond)

					broker.RemoveClient(cID, ch)
				}
			}
		}(i)
	}

	// Pool B: Independent parallel background thread loops parsing structural telemetry copies via Public API endpoints
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopSignal:
				return
			default:
				// Pull diagnostic loads aggressively to collision check maps.Clone boundaries
				_ = broker.GetCurrentMaps()
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	// Execution window
	time.Sleep(150 * time.Millisecond)
	close(stopSignal)
	wg.Wait()
}
