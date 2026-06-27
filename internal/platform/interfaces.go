// Copyright 2026 The Agentic Streamer Authors.
// SPDX-License-Identifier: Apache-2.0

package platform

// Observable defines a contract for structural dispatch systems allowing
// automated real-time lifecycle event tracking attachments.
type Observable interface {
	// Attach hooks an engine processor instance into the broadcast array pipeline.
	Attach(observer Observer)
	// Detach cuts the subscription tracking path for a targeted observer node.
	Detach(observer Observer)
}

// Queryable outlines data collection extraction vectors across state logs.
type Queryable interface {
	// Query scans index parameters to return matching chronological arrays.
	Query(param string, last int) []*Event
	// QueryLatest returns the single most recent telemetry packet found for a given target.
	QueryLatest(param string) *Event
}

// DaemonController encapsulates the entire system interface contract required to
// run, register, and query the underlying agent graph state engine.
type DaemonController interface {
	Observable
	Queryable

	// Start boots the state machine coordinator loop. Must block on execution.
	Start()
	// GetAgents returns a thread-safe, isolated map copy of all registered agent templates.
	GetAgents() map[string]*AgentMetadata
	// GetAgentRuns fetches all active runtime instance profiles assigned to a parent Agent.
	GetAgentRuns(agentID string) []*AgentRunDetail
	// GetAgentRunEvents streams cold execution history straight out of storage keys.
	GetAgentRunEvents(agentRunID AgentRunID) []*Event
	// RegisterAgent injects static capability schemas into the engine context.
	RegisterAgent(agent Agent)
	// RegisterSnapshot pumps live incoming status frames directly into processing streams.
	RegisterSnapshot(snapshot AgentRunSnapshot)
}

// Observer specifies the application pipeline contract interface used to intercept
// multiplexed multi-agent telemetry broadcasts.
type Observer interface {
	// Process handles an isolated event packet transmitted by an upstream multiplexer.
	Process(event Event)
}

// EventStore provisions high-speed data persistence contracts for skip-lists,
// memtables, or underlying log arrays.
type EventStore interface {
	// Put writes a record pointer into storage using an ordered sortable index key.
	Put(key string, value *Event)
	// Query retrieves a slice array of historical pointers matched against prefix keys.
	Query(key string) []*Event
	// QueryLastN scans indexing bounds to isolate the top N tail items for a specific stream.
	QueryLastN(key string, lastN int) []*Event
}
