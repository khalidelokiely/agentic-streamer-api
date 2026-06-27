// Copyright 2026 The Agentic Streamer Authors.
// SPDX-License-Identifier: Apache-2.0

package platform

import (
	"fmt"
	"maps"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DaemonSnapshot protects state registers using an immutable copy-on-write structural layout.
type DaemonSnapshot struct {
	agents            map[string]*AgentMetadata
	runningAgents     map[string]map[string]*AgentRunDetail
	fakerAgentRunList map[string][]string
}

// AgentDaemon implements the central coordination kernel monitoring multi-agent graph topologies.
type AgentDaemon struct {
	// memTable handles high-speed chronological event indexing.
	memTable EventStore
	// snapshot holds the atomic pointer to the active immutable DaemonSnapshot.
	snapshot atomic.Value

	// registerRequestQueue buffers incoming external static registration demands.
	registerRequestQueue chan Agent
	// runSnapshotQueue buffers runtime operational telemetry frames.
	runSnapshotQueue chan AgentRunSnapshot

	// observersMu guards the lifecycle attachment matrix exclusively.
	observersMu sync.RWMutex
	observers   map[Observer]bool
}

// NewAgentDaemon initializes and returns an un-started instance of the core state manager daemon.
func NewAgentDaemon(eventStore EventStore) DaemonController {
	d := &AgentDaemon{
		memTable:             eventStore,
		observers:            make(map[Observer]bool),
		registerRequestQueue: make(chan Agent, 100),
		runSnapshotQueue:     make(chan AgentRunSnapshot, 100),
	}

	// Bootstrap core atomic registry structures with empty maps.
	d.snapshot.Store(&DaemonSnapshot{
		agents:            make(map[string]*AgentMetadata),
		runningAgents:     make(map[string]map[string]*AgentRunDetail),
		fakerAgentRunList: make(map[string][]string),
	})

	return d
}

// Start ignites the event coordinator runtime engine loop.
// This executes indefinitely and must be provisioned inside an isolated runtime thread wrapper.
func (a *AgentDaemon) Start() {
	fmt.Println("Starting AgentDaemon state coordinator...")

	// 1. Seed static configurations directly onto the initial state snapshot.
	a.seedStaticAgents()

	// 2. Initialize simulation heartbeat metrics.
	simulationTicker := time.NewTicker(2 * time.Second)
	defer simulationTicker.Stop()

	var sequence int64

	// 3. The Lock-Free Processing Engine.
	// This loop thread owns mutations; no locks are required to read or alter internal copies.
	for {
		select {
		case req := <-a.registerRequestQueue:
			a.processAgentRegisterRequest(req)

		case snapshot := <-a.runSnapshotQueue:
			sequence++
			a.processAgentRunSnapshot(snapshot, sequence)

		case <-simulationTicker.C:
			a.injectSimulatedSnapshot()
		}
	}
}

// processAgentRegisterRequest inserts structural execution templates safely into registries.
func (a *AgentDaemon) processAgentRegisterRequest(req Agent) {
	current := a.snapshot.Load().(*DaemonSnapshot)

	// Clone top-level configuration elements to support clean mutation separation.
	nextAgents := maps.Clone(current.agents)
	nextRunning := maps.Clone(current.runningAgents)
	nextFaker := maps.Clone(current.fakerAgentRunList)

	nextAgents[req.ID] = &req.Metadata

	a.snapshot.Store(&DaemonSnapshot{
		agents:            nextAgents,
		runningAgents:     nextRunning,
		fakerAgentRunList: nextFaker,
	})
}

// processAgentRunSnapshot parses real-time execution states and commits updates downstream.
func (a *AgentDaemon) processAgentRunSnapshot(req AgentRunSnapshot, sequence int64) {
	// Compute unified, zero-padded sortable string indexing bounds for the skip list storage tier.
	storageKey := fmt.Sprintf("%s:%s:%012d", req.AgentID, req.RunID, sequence)
	event := NewEventFromSnapshot(req)

	a.memTable.Put(storageKey, event)
	a.notify(*event)
}

// injectSimulatedSnapshot targets random nodes to generate simulated framework activities.
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
	statuses := []string{"THINKING", "EXECUTING_TOOL", "COMPLETE"}
	randomStatus := statuses[rand.Intn(len(statuses))]

	snapshot := AgentRunSnapshot{
		AgentID:    randomAgentID,
		RunID:      runID,
		NodeID:     randomNode,
		NodeStatus: randomStatus,
	}

	select {
	case a.runSnapshotQueue <- snapshot:
	default:
		// Queue saturated; drop frame silently to prevent performance degradation.
	}
}

// seedStaticAgents formats baseline configurations on boot.
func (a *AgentDaemon) seedStaticAgents() {
	fmt.Println("Seeding static agent configurations...")

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

	current := a.snapshot.Load().(*DaemonSnapshot)
	nextAgents := maps.Clone(current.agents)
	nextRunning := maps.Clone(current.runningAgents)
	nextFaker := maps.Clone(current.fakerAgentRunList)

	for _, agent := range defaultAgents {
		nextAgents[agent.ID] = &agent.Metadata

		if _, ok := nextRunning[agent.ID]; !ok {
			nextRunning[agent.ID] = make(map[string]*AgentRunDetail)
		}

		runID := "run_uuid_10000"
		nextRunning[agent.ID][runID] = &AgentRunDetail{
			AgentRunID:      fmt.Sprintf("%s:%s", agent.ID, runID),
			TaskName:        "SEED_RUN",
			TaskDescription: fmt.Sprintf("Seed Agent Run for Agent %v", agent.ID),
			CreatedBy:       "SYSTEM_GENERATE",
			CreatedAt:       time.Now().UnixMilli(),
		}

		nextFaker[agent.ID] = append(nextFaker[agent.ID], runID)
	}

	a.snapshot.Store(&DaemonSnapshot{
		agents:            nextAgents,
		runningAgents:     nextRunning,
		fakerAgentRunList: nextFaker,
	})
}

// PUBLIC API SURFACE BOUNDARIES (100% Thread-Safe & Lock-Free Read Operations)

func (a *AgentDaemon) RegisterAgent(req Agent) {
	a.registerRequestQueue <- req
}

func (a *AgentDaemon) RegisterSnapshot(req AgentRunSnapshot) {
	a.runSnapshotQueue <- req
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

func (a *AgentDaemon) GetAgents() map[string]*AgentMetadata {
	// LOCK-FREE: Read straight from the current atomic pointer snapshot
	current := a.snapshot.Load().(*DaemonSnapshot)
	return maps.Clone(current.agents)
}

func (a *AgentDaemon) GetAgentRuns(agentID string) []*AgentRunDetail {
	// LOCK-FREE: Non-blocking isolated range collection reads
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

// SUBSCRIPTION MECHANICS

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
	if strings.HasSuffix(param, "*") {
		return strings.TrimSuffix(param, "*")
	}
	return param
}
