// Copyright 2026 The Agentic Streamer Authors.
// SPDX-License-Identifier: Apache-2.0

package platform

import (
	"fmt"
	"maps"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type DaemonSnapshot struct {
	agents            map[string]*AgentMetadata
	runningAgents     map[string]map[string]*AgentRunDetail
	fakerAgentRunList map[string][]string
}

// ============================================================================
// THE COORDINATION COORDINATOR KERNEL
// ============================================================================

type AgentDaemon struct {
	memTable EventStore
	snapshot atomic.Value
	sequence int64 // Controlled exclusively inside the single thread worker loop

	registerRequestQueue  chan Agent
	registerAgentRunQueue chan AgentRunDetail
	runSnapshotQueue      chan AgentRunSnapshot

	observersMu sync.RWMutex
	observers   map[Observer]bool
}

// NewAgentDaemon initializes and returns an un-started instance of the core state manager daemon.
func NewAgentDaemon(eventStore EventStore) DaemonController {
	d := &AgentDaemon{
		memTable:              eventStore,
		observers:             make(map[Observer]bool),
		registerRequestQueue:  make(chan Agent, 100),
		registerAgentRunQueue: make(chan AgentRunDetail, 100),
		runSnapshotQueue:      make(chan AgentRunSnapshot, 100),
	}

	d.snapshot.Store(&DaemonSnapshot{
		agents:            make(map[string]*AgentMetadata),
		runningAgents:     make(map[string]map[string]*AgentRunDetail),
		fakerAgentRunList: make(map[string][]string),
	})

	return d
}

// Start ignites the event coordinator runtime engine loop.
func (a *AgentDaemon) Start() {
	fmt.Println("Starting AgentDaemon state coordinator...")

	// 1. Streamlined Configuration Seeding via unified processing pipelines
	a.seedStaticAgents()

	simulationTicker := time.NewTicker(2 * time.Second)
	defer simulationTicker.Stop()

	// 2. The Lock-Free Single-Writer Processing Engine
	for {
		select {
		case req := <-a.registerRequestQueue:
			a.processAgentRegisterRequest(req)

		case runReq := <-a.registerAgentRunQueue:
			a.processAgentRun(runReq)

		case snapshot := <-a.runSnapshotQueue:
			a.sequence++
			a.processAgentRunSnapshot(snapshot, a.sequence)

		case <-simulationTicker.C:
			a.injectSimulatedSnapshot()
		}
	}
}

// ============================================================================
// MUTATION ENGINES (Strict Copy-On-Write & Guard Gated)
// ============================================================================

func (a *AgentDaemon) processAgentRegisterRequest(req Agent) {
	current := a.snapshot.Load().(*DaemonSnapshot)

	// Guard Gate: Exit if metadata configuration has not evolved
	if existing, exists := current.agents[req.ID]; exists {
		if reflect.DeepEqual(existing, &req.Metadata) {
			return
		}
	}

	nextAgents := maps.Clone(current.agents)
	nextAgents[req.ID] = &req.Metadata

	a.snapshot.Store(&DaemonSnapshot{
		agents:            nextAgents,
		runningAgents:     current.runningAgents,
		fakerAgentRunList: current.fakerAgentRunList,
	})
}

func (a *AgentDaemon) processAgentRun(req AgentRunDetail) {
	parts := strings.Split(req.AgentRunID, ":")
	if len(parts) != 2 {
		return
	}
	agentID, runID := parts[0], parts[1]

	current := a.snapshot.Load().(*DaemonSnapshot)

	// Guard Gate: Prevent redundant re-allocations if the run is already active
	if nested, exists := current.runningAgents[agentID]; exists {
		if _, runExists := nested[runID]; runExists {
			return
		}
	}

	// 1. Evolve the Running Agents Map Tree
	nextRunning := maps.Clone(current.runningAgents)
	if nextRunning[agentID] == nil {
		nextRunning[agentID] = make(map[string]*AgentRunDetail)
	} else {
		nextRunning[agentID] = maps.Clone(nextRunning[agentID])
	}
	nextRunning[agentID][runID] = &req

	// 2. Evolve the Simulator Manifest list concurrently to keep them perfectly in sync
	nextFaker := maps.Clone(current.fakerAgentRunList)
	nextFaker[agentID] = append(append([]string(nil), nextFaker[agentID]...), runID)

	a.snapshot.Store(&DaemonSnapshot{
		agents:            current.agents,
		runningAgents:     nextRunning,
		fakerAgentRunList: nextFaker,
	})
}

func (a *AgentDaemon) processAgentRunSnapshot(req AgentRunSnapshot, sequence int64) {
	current := a.snapshot.Load().(*DaemonSnapshot)

	// FEATURE 1: TOPOLOGY VALIDATION FIREWALL
	agentMeta, agentExists := current.agents[req.AgentID]
	if !agentExists {
		fmt.Printf("[WARNING] Ingestion dropped: Agent '%s' is unregistered.\n", req.AgentID)
		return
	}

	isValidNode := false
	for _, nodeID := range agentMeta.NodeIDList {
		if nodeID == req.NodeID {
			isValidNode = true
			break
		}
	}
	if !isValidNode {
		fmt.Printf("[REJECTED] Ingestion dropped: Node '%s' violates layout bounds for '%s'.\n", req.NodeID, req.AgentID)
		return
	}

	// FEATURE 2: AUTOMATED RUN STATE MACRO LIFECYCLE MUTATOR
	if nested, exists := current.runningAgents[req.AgentID]; exists {
		if currentRun, runExists := nested[req.RunID]; runExists {

			isTerminalState := req.NodeStatus == "COMPLETE" || req.NodeStatus == "FAILED"

			if currentRun.Status != req.NodeStatus && isTerminalState {
				nextRunning := maps.Clone(current.runningAgents)
				nextRunning[req.AgentID] = maps.Clone(nextRunning[req.AgentID])

				updatedRun := *currentRun
				updatedRun.Status = req.NodeStatus
				nextRunning[req.AgentID][req.RunID] = &updatedRun

				a.snapshot.Store(&DaemonSnapshot{
					agents:            current.agents,
					runningAgents:     nextRunning,
					fakerAgentRunList: current.fakerAgentRunList,
				})
				fmt.Printf("[STATE CHANGE] Run %s updated to %s\n", req.RunID, req.NodeStatus)
			}
		}
	}

	// FEATURE 3: PERSISTENCE WRITE-HOOK WITH CHRONO-SORTABLE PREFIX INDEX
	storageKey := fmt.Sprintf("%s:%s:%012d", req.AgentID, req.RunID, sequence)
	event := NewEventFromSnapshot(req)

	a.memTable.Put(storageKey, event)
	a.notify(*event)
}

// ============================================================================
// SEED ENGINE & SIMULATION HEARTBEAT
// ============================================================================

func (a *AgentDaemon) seedStaticAgents() {
	fmt.Println("Seeding static configurations through unified pathways...")

	defaultAgents := []Agent{
		{
			ID: "codepal-v1",
			Metadata: AgentMetadata{
				Type:        "CodingAssistant",
				Description: "Autonomous software engineer generating Go code segments.",
				Category:    "Development",
				NodeIDList:  []string{"router", "planner", "llm_call", "compiler_check", "git_push"},
			},
		},
		{
			ID: "doc-bot",
			Metadata: AgentMetadata{
				Type:        "DocumentationCritic",
				Description: "Validates architecture and documentation accuracy.",
				Category:    "Analysis",
				NodeIDList:  []string{"read_files", "llm_eval", "markdown_generator"},
			},
		},
	}

	for _, agent := range defaultAgents {
		// 1. Route through the standard registration framework
		a.processAgentRegisterRequest(agent)

		// 2. Route the baseline structural execution frame through the unified engine
		runID := "seed_run_10000"
		runDetail := AgentRunDetail{
			AgentRunID:      fmt.Sprintf("%s:%s", agent.ID, runID),
			TaskName:        "SEED_INITIALIZATION",
			TaskDescription: fmt.Sprintf("System generated verification vector for %s", agent.ID),
			Status:          "RUNNING",
			CreatedBy:       "KERNEL_BOOTSTRAP",
			CreatedAt:       time.Now().UnixMilli(),
		}
		a.processAgentRun(runDetail)

		// 3. EVOLVED: Populate your event store and fire live notifications for the seed
		a.sequence++
		initialSnapshot := AgentRunSnapshot{
			AgentID:    agent.ID,
			RunID:      runID,
			NodeID:     agent.Metadata.NodeIDList[0], // Dynamically point to the root node
			NodeStatus: "PENDING",
			Message:    "Bootstrap checkpoint committed successfully.",
		}
		a.processAgentRunSnapshot(initialSnapshot, a.sequence)
	}
}

func (a *AgentDaemon) injectSimulatedSnapshot() {
	current := a.snapshot.Load().(*DaemonSnapshot)
	if len(current.agents) == 0 {
		return
	}

	agentIDs := make([]string, 0, len(current.agents))
	for id := range current.agents {
		agentIDs = append(agentIDs, id)
	}

	randomAgentID := agentIDs[rand.Intn(len(agentIDs))]
	runs := current.fakerAgentRunList[randomAgentID]
	if len(runs) == 0 {
		return
	}
	runID := runs[rand.Intn(len(runs))]

	meta := current.agents[randomAgentID]
	if meta == nil || len(meta.NodeIDList) == 0 {
		return
	}

	randomNode := meta.NodeIDList[rand.Intn(len(meta.NodeIDList))]
	statuses := []string{
		"PENDING",
		"THINKING",
		"CALLING_TOOL",
		"AWAITING_INPUT",
		"RETRYING",
		"COMPLETE",
		"FAILED",
	}
	randomStatus := statuses[rand.Intn(len(statuses))]

	snapshot := AgentRunSnapshot{
		AgentID:    randomAgentID,
		RunID:      runID,
		NodeID:     randomNode,
		NodeStatus: randomStatus,
		Message:    fmt.Sprintf("Simulated trace step executed on node '%s'", randomNode),
	}

	select {
	case a.runSnapshotQueue <- snapshot:
	default:
		// Drop frame cleanly if channel blocks to safeguard high throughput loops
	}
}

// ============================================================================
// THREAD-SAFE PUBLIC READ API SURFACE
// ============================================================================

func (a *AgentDaemon) RegisterAgent(agent Agent) {
	a.registerRequestQueue <- agent
}

func (a *AgentDaemon) RegisterAgentRun(agentRun AgentRunDetail) {
	a.registerAgentRunQueue <- agentRun
}

func (a *AgentDaemon) RegisterSnapshot(snapshot AgentRunSnapshot) {
	a.runSnapshotQueue <- snapshot
}

func (a *AgentDaemon) GetAgents() map[string]*AgentMetadata {
	current := a.snapshot.Load().(*DaemonSnapshot)
	return maps.Clone(current.agents)
}

func (a *AgentDaemon) GetAgentRuns(agentID string) []*AgentRunDetail {
	current := a.snapshot.Load().(*DaemonSnapshot)
	runs, exists := current.runningAgents[agentID]
	if !exists {
		return []*AgentRunDetail{}
	}

	result := make([]*AgentRunDetail, 0, len(runs))
	for _, run := range runs {
		result = append(result, run)
	}
	return result
}

func (a *AgentDaemon) GetAgentRunEvents(agentRunID AgentRunID) []*Event {
	return a.memTable.Query(agentRunID.String())
}

func (a *AgentDaemon) Query(param string, last int) []*Event {
	param = a.cleanWildCard(param)
	if last > 0 {
		return a.memTable.QueryLastN(param, last)
	}
	return a.memTable.Query(param)
}

func (a *AgentDaemon) QueryLatest(param string) *Event {
	param = a.cleanWildCard(param)
	if res := a.memTable.QueryLastN(param, 1); len(res) > 0 {
		return res[0]
	}
	return nil
}

// ============================================================================
// OBSERVER FAN-OUT MECHANICS
// ============================================================================

func (a *AgentDaemon) Attach(observer Observer) {
	a.observersMu.Lock()
	defer a.observersMu.Unlock()
	a.observers[observer] = true
}

func (a *AgentDaemon) Detach(observer Observer) {
	a.observersMu.Lock()
	defer a.observersMu.Unlock()
	delete(a.observers, observer)
}

func (a *AgentDaemon) notify(event Event) {
	a.observersMu.RLock()
	targets := make([]Observer, 0, len(a.observers))
	for observer := range a.observers {
		targets = append(targets, observer)
	}
	a.observersMu.RUnlock()

	for _, observer := range targets {
		observer.Process(event)
	}
}

func (a *AgentDaemon) cleanWildCard(param string) string {
	return strings.TrimSuffix(param, "*")
}
