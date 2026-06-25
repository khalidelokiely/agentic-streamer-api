package platform

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// INTERNAL UTILITY HELPERS
// ============================================================================

// pollCondition periodically evaluates a predicate function up to a maximum timeout
// boundary, enabling deterministic checking of asynchronous channel ingestion.
func pollCondition(timeout time.Duration, predicate func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return predicate()
}

// ============================================================================
// DAEMON CORE BEHAVIORAL TESTS
// ============================================================================

// TestAgentDaemon_RegisterAgent validates that entities pushed to the coordinator
// are picked up and safely recorded into the active monitoring map.
func TestAgentDaemon_RegisterAgent(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore).(*AgentDaemon)
	go daemon.Start()

	targetID := "test-agent-unique-99"
	agent := Agent{
		ID: targetID,
		Metadata: AgentMetadata{
			Type:        "TestAgent",
			Description: "Deterministic validation target",
			Category:    "Testing",
			NodeIDList:  []string{"node1", "node2"},
		},
	}

	daemon.RegisterAgent(agent)

	// Poll state safely instead of arbitrary high-duration sleeps
	success := pollCondition(200*time.Millisecond, func() bool {
		agents := daemon.GetAgents()
		_, exists := agents[targetID]
		return exists
	})

	if !success {
		t.Fatalf("Timeout: Engine failed to register agent '%s' within processing window", targetID)
	}
}

// TestAgentDaemon_RegisterSnapshot verifies that telemetry snapshots route through
// the staging queue and persist into the internal memory event table correctly.
func TestAgentDaemon_RegisterSnapshot(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore).(*AgentDaemon)
	go daemon.Start()

	agentID := "snapshot-agent"
	runID := "run_uuid_99999"
	daemon.RegisterAgent(Agent{ID: agentID})

	snapshot := AgentRunSnapshot{
		AgentID:    agentID,
		RunID:      runID,
		NodeID:     "llm_call",
		NodeStatus: "EXECUTING",
	}
	daemon.RegisterSnapshot(snapshot)

	searchKey := fmt.Sprintf("%s:%s", agentID, runID)
	success := pollCondition(200*time.Millisecond, func() bool {
		events := daemon.Query(searchKey, 0)
		return len(events) > 0
	})

	if !success {
		t.Fatal("Timeout: Snapshot event trace was dropped or index-matching failed inside memory store")
	}
}

// TestAgentDaemon_QueryLatest asserts that streaming iterative modifications
// return only the absolute latest step sequence point.
func TestAgentDaemon_QueryLatest(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore).(*AgentDaemon)
	go daemon.Start()

	agentID := "query-agent"
	runID := "run_uuid_20000"
	daemon.RegisterAgent(Agent{ID: agentID})

	// Dispatch progressive execution sequences down the serialization channel
	statuses := []string{"THINKING", "EXECUTING_TOOL", "COMPLETE"}
	for _, status := range statuses {
		daemon.RegisterSnapshot(AgentRunSnapshot{
			AgentID:    agentID,
			RunID:      runID,
			NodeID:     "llm_call",
			NodeStatus: status,
		})
	}

	var latest *Event
	searchKey := fmt.Sprintf("%s:%s", agentID, runID)
	success := pollCondition(200*time.Millisecond, func() bool {
		latest = daemon.QueryLatest(searchKey)
		return latest != nil && latest.NodeStatus == "COMPLETE"
	})

	if !success {
		if latest == nil {
			t.Fatal("Timeout: QueryLatest returned nil; state channel ingestion halted")
		}
		t.Errorf("State sequencing inversion: Expected terminal node status 'COMPLETE', got %s", latest.NodeStatus)
	}
}

// TestAgentDaemon_AttachDetachObservers verifies broadcast targeting safety and isolates
// detached consumers using a secondary sentinel observer flush pattern.
func TestAgentDaemon_AttachDetachObservers(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore).(*AgentDaemon)
	go daemon.Start()

	obsPrimary := &mockObserver{}
	daemon.Attach(obsPrimary)

	agentID := "obs-agent"
	daemon.RegisterAgent(Agent{ID: agentID})

	// 1. Broadcast while attached
	daemon.RegisterSnapshot(AgentRunSnapshot{
		AgentID:    agentID,
		RunID:      "run-101",
		NodeID:     "node-1",
		NodeStatus: "THINKING",
	})

	caughtPrimary := pollCondition(200*time.Millisecond, func() bool {
		return len(obsPrimary.GetEvents()) > 0
	})
	if !caughtPrimary {
		t.Fatal("Broadcast Failure: Attached primary observer failed to catch live channel stream")
	}

	baselineCount := len(obsPrimary.GetEvents())

	// 2. Detach primary and hook a flush sentinel to confirm message pipeline progression
	daemon.Detach(obsPrimary)

	obsSentinel := &mockObserver{}
	daemon.Attach(obsSentinel)

	daemon.RegisterSnapshot(AgentRunSnapshot{
		AgentID:    agentID,
		RunID:      "run-101",
		NodeID:     "node-1",
		NodeStatus: "EXECUTING",
	})

	// Wait for sentinel to receive the message; guarantees the processing loop
	// has swept cleanly past the primary observer's detachment point.
	flushComplete := pollCondition(200*time.Millisecond, func() bool {
		return len(obsSentinel.GetEvents()) > 0
	})
	if !flushComplete {
		t.Fatal("Pipeline Lock: Transmission failed to propagate to verification sentinel")
	}

	// 3. Confirm primary interface did not receive post-detachment frames
	if len(obsPrimary.GetEvents()) != baselineCount {
		t.Error("Security Breach: Disconnected observer detected an asynchronous telemetry stream memory leak")
	}
}

// ============================================================================
// 5. STRESS & HISTORICAL WILD-CARD RUN CONFORMANCE TESTS
// ============================================================================

// TestAgentDaemon_ConcurrentWriteStressRace hammers core state mapping systems
// using parallel worker pools under active race detection constraints.
func TestAgentDaemon_ConcurrentWriteStressRace(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore).(*AgentDaemon)
	go daemon.Start()

	var wg sync.WaitGroup
	numWorkers := 10
	opsPerWorker := 15

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(wID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				uID := fmt.Sprintf("race-agent-%d-iter-%d", wID, j)
				daemon.RegisterAgent(Agent{ID: uID})

				daemon.RegisterSnapshot(AgentRunSnapshot{
					AgentID:    uID,
					RunID:      "shared-run-trace",
					NodeID:     "computation-core",
					NodeStatus: "COMPUTING",
				})
			}
		}(i)
	}
	wg.Wait()

	// Maintain parallel read loops against maps to trap memory collisions
	stopSignals := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopSignals:
				return
			default:
				_ = daemon.GetAgents()
				_ = daemon.GetAgentRuns("race-agent-0-iter-0")
			}
		}
	}()

	time.Sleep(40 * time.Millisecond)
	close(stopSignals)
}

// TestAgentDaemon_HistoricalQueries AND trailing wildcard scrubs check lookup behaviors
func TestAgentDaemon_HistoricalQueriesAndWildcardScrubs(t *testing.T) {
	eventStore := &mockEventStore{}
	daemon := NewAgentDaemon(eventStore).(*AgentDaemon)
	go daemon.Start()

	// Allow static seeding methods to write into memory tables
	time.Sleep(10 * time.Millisecond)

	snapshot := AgentRunSnapshot{
		AgentID:    "codepal-v1",
		RunID:      "run_uuid_10000",
		NodeID:     "compiler_check",
		NodeStatus: "COMPLETE",
	}
	daemon.RegisterSnapshot(snapshot)

	// Validate historical data point re-hydration parsing
	var history []*Event
	historyPopulated := pollCondition(200*time.Millisecond, func() bool {
		history = daemon.GetAgentRunEvents("codepal-v1:run_uuid_10000")
		return len(history) > 0
	})
	if !historyPopulated {
		t.Fatal("Data Missing: GetAgentRunEvents failed to compile historical matrix logs")
	}

	// Validate wildcard string parsing filters trailing elements correctly
	var wildcardQuery []*Event
	wildcardCleaned := pollCondition(200*time.Millisecond, func() bool {
		wildcardQuery = daemon.Query("codepal-v1:*", 0)
		return len(wildcardQuery) > 0
	})
	if !wildcardCleaned {
		t.Fatal("Scrub Failure: Trailing wildcard filter expression query returned empty data sets")
	}
}
