package platform

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// MOCK EVENT STORE
// ============================================================================
// mockEventStore acts as a thread-safe, in-memory mock replacement for the
// custom Skip-List MemTable. It supports concurrent reads/writes and mimics
// sequential prefix scanning.
type mockEventStore struct {
	mu     sync.RWMutex
	events map[string][]*Event
}

// Put safely appends a new event pointer to the internal slice mapping.
func (m *mockEventStore) Put(key string, value *Event) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.events == nil {
		m.events = make(map[string][]*Event)
	}
	m.events[key] = append(m.events[key], value)
}

// Query simulates a Skip-List prefix scan. It gathers all event keys starting
// with the provided parameter prefix, sorts them lexicographically, and returns a safe copy.
func (m *mockEventStore) Query(key string) []*Event {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Gather keys that match the search boundary prefix
	var matchedKeys []string
	for k := range m.events {
		if strings.HasPrefix(k, key) {
			matchedKeys = append(matchedKeys, k)
		}
	}
	// Sort keys to maintain strict chronological sequencing alignment
	sort.Strings(matchedKeys)

	var result []*Event
	for _, k := range matchedKeys {
		for _, ev := range m.events[k] {
			if ev != nil {
				// Create a shallow copy of the event pointer to prevent mutations
				clonedEv := *ev
				result = append(result, &clonedEv)
			}
		}
	}
	return result
}

// QueryLastN retrieves the last N historical logs for a given key prefix.
func (m *mockEventStore) QueryLastN(key string, lastN int) []*Event {
	events := m.Query(key)
	if len(events) == 0 {
		return nil
	}
	if len(events) <= lastN {
		return events
	}
	return events[len(events)-lastN:]
}

// ============================================================================
// MOCK OBSERVER
// ============================================================================
// mockObserver records broadcasted value payloads from the daemon core loop
// asynchronously. Used for assertion checking inside subscription workflows.
type mockObserver struct {
	mu     sync.RWMutex
	events []Event
}

// Process implements the platform.Observer interface by taking a copy of the Event.
func (m *mockObserver) Process(event Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
}

// GetEvents creates a thread-safe snapshot clone of all tracked events received
// up to this point, neutralizing potential cross-thread read/write collisions.
func (m *mockObserver) GetEvents() []Event {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.events) == 0 {
		return nil
	}

	copied := make([]Event, len(m.events))
	copy(copied, m.events)
	return copied
}

// ============================================================================
// 1. BROKER HIGH-CONTENTION CONCURRENCY & RACE TEST
// ============================================================================
// This test forces the Broker's background coordinator loop to process mutations
// (cloning and replacing map snapshots via atomic.Value) while multiple HTTP thread
// pools aggressively read from the same state surface.
func TestBrokerRace_HighContentionChurn(t *testing.T) {
	mockStore := &mockEventStore{events: make(map[string][]*Event)}
	daemon := NewAgentDaemon(mockStore)
	broker := NewBroker(daemon)

	// Fire up background coordinator state loops
	go daemon.Start()
	go broker.Start()
	time.Sleep(20 * time.Millisecond)

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Pool A: 20 Goroutines slamming Client Connections & Disconnections
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			clientChan := make(chan Event, 10)
			clientID := fmt.Sprintf("client-churn-%d", workerID)

			for {
				select {
				case <-stopChan:
					return
				default:
					broker.AddClient(clientID, clientChan)
					time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)

					broker.RemoveClient(clientID, clientChan)
					time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
				}
			}
		}(i)
	}

	// Pool B: 20 Goroutines writing random real-time Agent Watch & Unwatch matrices
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			clientID := fmt.Sprintf("client-churn-%d", workerID%5)

			for {
				select {
				case <-stopChan:
					return
				default:
					targetAgent := fmt.Sprintf("agent-%d", rand.Intn(10))

					// Send watch request
					broker.watchQueue <- WatchRequest{
						ClientID: clientID,
						Agents:   []TargetAgent{{ID: targetAgent, LatestOnly: true}},
					}
					time.Sleep(1 * time.Millisecond)

					// Send unwatch request
					broker.Unwatch(UnwatchRequest{
						agentIDList: []string{targetAgent},
						clientID:    clientID,
					})
					time.Sleep(1 * time.Millisecond)
				}
			}
		}(i)
	}

	// Pool C: 30 Goroutines blasting incoming telemetry events through the pipeline
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					ev := Event{
						AgentRunID: fmt.Sprintf("agent-%d:run-xyz", rand.Intn(10)),
						NodeName:   "llm_call",
						NodeStatus: "THINKING",
						Timestamp:  time.Now().UnixMilli(),
					}
					broker.incomingEventQueue <- ev
					time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond)
				}
			}
		}(i)
	}

	// Pool D: 10 Goroutines aggressively pulling System Diagnostics (The Map Copy Race vector)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					// This triggers b.routingTable.Load() and deep iterations concurrently with mutations
					_ = broker.GetCurrentMaps()
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	// Execute high-density race testing window
	time.Sleep(1500 * time.Millisecond)
	close(stopChan)
	wg.Wait()
}

// ============================================================================
// 2. DAEMON MUTATION, LOOKUP & REGISTRATION RACE TEST
// ============================================================================
// This test asserts the structural integrity of the AgentDaemon's internal
// RWMutex. It checks for safe concurrent access while processing snapshots from
// channels, attaching/detaching observer interfaces, and performing REST queries.
func TestDaemonRace_StateMutationAndObservation(t *testing.T) {
	mockStore := &mockEventStore{events: make(map[string][]*Event)}
	daemon := NewAgentDaemon(mockStore)

	go daemon.Start()
	time.Sleep(20 * time.Millisecond)

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Seed real metadata frameworks
	for i := 0; i < 5; i++ {
		daemon.(*AgentDaemon).RegisterAgent(Agent{
			ID:       fmt.Sprintf("agent-%d", i),
			Metadata: AgentMetadata{Type: "Orchestrator"},
		})
	}

	// Pool A: 20 Goroutines constantly passing telemetry snapshots from backend layers
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					snap := AgentRunSnapshot{
						AgentID:    fmt.Sprintf("agent-%d", rand.Intn(5)),
						RunID:      fmt.Sprintf("run-%d", rand.Intn(100)),
						NodeID:     "executor",
						NodeStatus: "EXECUTING_TOOL",
					}
					daemon.(*AgentDaemon).RegisterSnapshot(snap)
					time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
				}
			}
		}(i)
	}

	// Pool B: 10 Goroutines continuously attaching/detaching dynamic Observers (SSE tracks)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			obs := &raceDummyObserver{}
			for {
				select {
				case <-stopChan:
					return
				default:
					daemon.Attach(obs)
					time.Sleep(1 * time.Millisecond)
					daemon.Detach(obs)
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	// Pool C: 20 Goroutines executing high-frequency lookups mimicking REST API Controllers
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					targetAgent := fmt.Sprintf("agent-%d", rand.Intn(5))
					targetRun := fmt.Sprintf("run-%d", rand.Intn(100))

					// Hit data reader boundaries protected by RLock
					_ = daemon.GetAgents()
					_ = daemon.GetAgentRuns(targetAgent)
					_ = daemon.GetAgentRunEvents(fmt.Sprintf("%s:%s", targetAgent, targetRun))
					_ = daemon.QueryLatest(fmt.Sprintf("%s:%s", targetAgent, targetRun))

					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	// Run processing stress sequence
	time.Sleep(1500 * time.Millisecond)
	close(stopChan)
	wg.Wait()
}

// ============================================================================
// HELPER STRUCTURES FOR COMPLETE ISOLATION
// ============================================================================

type raceDummyObserver struct{}

func (r *raceDummyObserver) Process(event Event) {}
