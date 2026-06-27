// Copyright 2026 The Agentic Streamer Authors.
// SPDX-License-Identifier: Apache-2.0

package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// AgentRunID defines a strongly typed domain identifier mapping standard composite
// tracking keys formatted as "<agent_id>:<run_id>" or wildcard arrays like "<agent_id>:*".
type AgentRunID string

// String converts the underlying type definition back to a standard primitive string slice.
func (a AgentRunID) String() string {
	return string(a)
}

// GetWildCardKey processes the internal token structure to infer the general group tracking key.
// Returns an error if the composite formatting structure is broken or missing components.
func (a AgentRunID) GetWildCardKey() (AgentRunID, error) {
	agentID, _, err := a.Parts()
	if err != nil {
		return "", err
	}

	return AgentRunID(agentID + ":*"), nil
}

// Parts validates the structural format and extracts the independent agent and run components.
// Returns an error if the composite formatting structure is broken or missing components.
func (a AgentRunID) Parts() (agentID string, runID string, err error) {
	str := string(a)
	idx := strings.Index(str, ":")

	// Validation check 1: Must contain the delimiter and cannot start/end with it
	if idx <= 0 || idx == len(str)-1 {
		return "", "", errors.New("malformed or unparseable agent run identifier payload configuration")
	}

	agentID = str[:idx]
	runID = str[idx+1:]

	// Validation check 2: Ensure there isn't a accidental second delimiter (e.g. "id:run:extra")
	if strings.Contains(runID, ":") {
		return "", "", errors.New("malformed identifier: multiple delimiters found")
	}

	return agentID, runID, nil
}

// Event models a normalized transition status vector distributed across outbound real-time streams.
type Event struct {
	AgentRunID AgentRunID `json:"agent_run_id"`
	NodeName   string     `json:"node_name"` // e.g., "router", "llm_call", "tool_executor"
	NodeStatus string     `json:"status"`    // e.g., "THINKING", "EXECUTING_TOOL", "COMPLETE"
	Payload    string     `json:"payload"`
	Timestamp  int64      `json:"timestamp"`
}

// NewEventFromSnapshot map converts an inbound un-marshaled raw data telemetry feed vector
// into a production unified broadcast structure.
func NewEventFromSnapshot(snapshot AgentRunSnapshot) *Event {
	return &Event{
		AgentRunID: AgentRunID(fmt.Sprintf("%s:%s", snapshot.AgentID, snapshot.RunID)),
		NodeName:   snapshot.NodeID,
		NodeStatus: snapshot.NodeStatus,
		Payload:    fmt.Sprintf("Node [%s] transitioned to state [%s]", snapshot.NodeID, snapshot.NodeStatus),
		Timestamp:  time.Now().UnixMilli(),
	}
}

// Bytes marshals the active structural payload directly into standard JSON array formats.
func (event *Event) Bytes() []byte {
	b, _ := json.Marshal(event)
	return b
}

// Agent represents static schema configurations defining operational capability layers.
type Agent struct {
	ID       string        `json:"id"`
	Metadata AgentMetadata `json:"metadata"`
}

// AgentMetadata encapsulates behavioral topologies defining step counts and properties.
type AgentMetadata struct {
	Type        string   `json:"type"`
	Description string   `json:"description"`
	Category    string   `json:"category"`
	NodeIDList  []string `json:"node_id_list"`
}

// AgentRunDetail details historical instance identification schemas for cold UI hydration tasks.
type AgentRunDetail struct {
	AgentRunID      string `json:"agent_run_id"`
	TaskName        string `json:"task_name"`
	TaskDescription string `json:"task_description"`
	TaskID          string `json:"task_id,omitempty"`
	CreatedBy       string `json:"created_by"`
	CreatedAt       int64  `json:"created_at"`
}

// AgentRunSnapshot specifies the flat payload boundary mapping structures parsed directly from input fabrics.
type AgentRunSnapshot struct {
	AgentID    string `json:"agent_id"`
	RunID      string `json:"run_id"`
	NodeID     string `json:"node_id"`
	NodeStatus string `json:"node_status"`
}

// WatchRequest packages explicit arrays targeting execution tracking triggers.
type WatchRequest struct {
	ClientID string        `json:"client_id"`
	Agents   []TargetAgent `json:"agents"`
	Ctx      context.Context
}

// TargetAgent coordinates properties for individual observation stream layers.
type TargetAgent struct {
	ID         AgentRunID `json:"id"`
	LatestOnly bool       `json:"latest_only"`
}
