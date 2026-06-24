package platform

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type AgentDaemon struct {
	memTable  EventStore
	observers map[Observer]bool
	agents    map[string]*AgentMetadata
	// @TODO: Remove this map as soon as data source is ready, this is only for seed usage to pick a random run
	//        and seed it with sequences of runs.
	fakerAgentRunList    map[string][]string
	runningAgents        map[string]map[string]*AgentRunDetail
	registerRequestQueue chan Agent
	runSnapshotQueue     chan AgentRunSnapshot
	mu                   sync.RWMutex // <-- Add this here to protect the maps
}

func NewAgentDaemon(eventStore EventStore) DaemonController {
	return &AgentDaemon{
		memTable:             eventStore,
		observers:            make(map[Observer]bool),
		agents:               make(map[string]*AgentMetadata),
		fakerAgentRunList:    make(map[string][]string),
		runningAgents:        make(map[string]map[string]*AgentRunDetail),
		registerRequestQueue: make(chan Agent, 100),
		runSnapshotQueue:     make(chan AgentRunSnapshot, 100),
	}
}

func (a *AgentDaemon) Start() {
	fmt.Println("Starting AgentDaemon state coordinator...")

	// 1. Run static seeding before entering the infinite loop
	a.seedStaticAgents()

	// 2. Initialize a ticker to simulate live agent events every 2 seconds
	simulationTicker := time.NewTicker(2 * time.Second)
	defer simulationTicker.Stop()

	// Track a monotonic sequence number to form unique, sortable chronological keys
	var sequence int64

	// 3. The single-threaded coordinator loop
	for {
		select {
		case req := <-a.registerRequestQueue:
			a.processAgentRegisterRequest(req)

		case snapshot := <-a.runSnapshotQueue:
			sequence++
			a.processAgentRunSnapshot(snapshot, sequence)

		case <-simulationTicker.C:
			// Inject a random live snapshot to simulate LangGraph activity
			a.injectSimulatedSnapshot()
		}
	}
}

func (a *AgentDaemon) addNewRun(agentId, runId string, req *AgentRunDetail) {
	if _, ok := a.runningAgents[agentId]; !ok {
		a.runningAgents[agentId] = make(map[string]*AgentRunDetail)
	}

	a.runningAgents[agentId][runId] = req
}

// seedStaticAgents pre-populates your daemon registry with template agent archetypes on boot
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

	for _, agent := range defaultAgents {
		a.processAgentRegisterRequest(agent)
		// Seed an active run tracking instance mapping the Agent ID to a mock active Run ID
		a.runningAgents[agent.ID] = make(map[string]*AgentRunDetail)

		runID := "run_uuid_10000"

		a.addNewRun(agent.ID, runID, &AgentRunDetail{
			AgentRunID:      fmt.Sprintf("%s:%s", agent.ID, runID),
			TaskName:        "SEED_RUN",
			TaskDescription: fmt.Sprintf("Seed Agent Run for Agent %v", agent.ID),
			CreatedBy:       "SYSTEM_GENERATE",
			CreatedAt:       time.Now().UnixMilli(),
		})

		a.fakerAgentRunList[agent.ID] = append(a.fakerAgentRunList[agent.ID], runID)
	}
}

// injectSimulatedSnapshot randomly picks an active agent and fires a snapshot into its own queue
func (a *AgentDaemon) injectSimulatedSnapshot() {
	if len(a.agents) == 0 {
		return
	}

	// Pick a random registered agent ID
	agentIDs := make([]string, 0, len(a.agents))
	for id := range a.agents {
		agentIDs = append(agentIDs, id)
	}

	if len(agentIDs) == 0 {
		return
	}

	randomAgentID := agentIDs[rand.Intn(len(agentIDs))]
	meta := a.agents[randomAgentID]
	runID := a.fakerAgentRunList[randomAgentID][rand.Intn(len(a.fakerAgentRunList[randomAgentID]))]

	// Pick a random sub-node from the agent's defined graph layout
	randomNode := meta.NodeIDList[rand.Intn(len(meta.NodeIDList))]

	statuses := []string{"THINKING", "EXECUTING_TOOL", "COMPLETE"}
	randomStatus := statuses[rand.Intn(len(statuses))]

	snapshot := AgentRunSnapshot{
		RunID:      runID,
		AgentID:    randomAgentID,
		NodeID:     randomNode,
		NodeStatus: randomStatus,
	}

	// Push it back through the public queue thread-safely
	// Non-blocking write to avoid locking up the ticker if the queue is full
	select {
	case a.runSnapshotQueue <- snapshot:
	default:
		fmt.Println("Warning: runSnapshotQueue full, dropping simulated event")
	}
}

func (a *AgentDaemon) processAgentRegisterRequest(req Agent) {
	a.mu.Lock()         // Acquire exclusive write lock
	defer a.mu.Unlock() // Release lock when mutation finishes

	a.agents[req.ID] = &req.Metadata
}

func (a *AgentDaemon) processAgentRunSnapshot(req AgentRunSnapshot, sequence int64) {
	storageKey := fmt.Sprintf("%s:%s:%012d", req.AgentID, req.RunID, sequence)

	event := NewEventFromSnapshot(req)

	a.memTable.Put(storageKey, event)
	a.notify(*event)
}

func (a *AgentDaemon) RegisterAgent(req Agent) {
	a.registerRequestQueue <- req
}

func (a *AgentDaemon) RegisterSnapshot(req AgentRunSnapshot) {
	a.runSnapshotQueue <- req
}

func (a *AgentDaemon) cleanWildCard(param string) string {
	if strings.HasSuffix(param, "*") {
		return strings.TrimSuffix(param, "*")
	}

	return param
}

func (a *AgentDaemon) Query(param string, last int) []*Event {
	param = a.cleanWildCard(param)

	if last > 0 {
		return a.memTable.QueryLastN(param, last)
	}

	// regurgitate history
	return a.memTable.Query(param)
}

func (a *AgentDaemon) QueryLatest(param string) *Event {
	param = a.cleanWildCard(param)

	if res := a.memTable.QueryLastN(param, 1); len(res) > 0 {
		return res[0]
	}
	return nil
}

func (a *AgentDaemon) Attach(observer Observer) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.observers[observer] = true
}

func (a *AgentDaemon) Detach(observer Observer) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.observers, observer)
}

func (a *AgentDaemon) notify(event Event) {
	a.mu.RLock()
	targets := make([]Observer, 0, len(a.observers))

	for observer := range a.observers {
		targets = append(targets, observer)
	}
	a.mu.RUnlock()

	for _, observer := range targets {
		observer.Process(event)
	}
}

func (a *AgentDaemon) GetAgents() map[string]*AgentMetadata {
	a.mu.RLock() // Allow other readers, block writers
	defer a.mu.RUnlock()

	return a.agents
}

func (a *AgentDaemon) GetAgentRuns(agentID string) []*AgentRunDetail {
	result := make([]*AgentRunDetail, 0)

	a.mu.RLock() // Allow other readers, block writers
	defer a.mu.RUnlock()

	for _, run := range a.runningAgents[agentID] {
		result = append(result, run)
	}

	return result
}

func (a *AgentDaemon) GetAgentRunEvents(agentRunID string) []*Event {
	fmt.Println(agentRunID)
	return a.memTable.Query(agentRunID)
}
