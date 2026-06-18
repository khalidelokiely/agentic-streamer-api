package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Event struct {
	AgentRunID string `json:"agent_run_id"`
	NodeName   string `json:"node_name"` // e.g., "router", "llm_call", "tool_executor"
	NodeStatus string `json:"status"`    // e.g., "THINKING", "EXECUTING_TOOL", "COMPLETE"
	Payload    string `json:"payload"`
	Timestamp  int64  `json:"timestamp"`
}

func NewEventFromSnapshot(snapshot AgentRunSnapshot) *Event {
	return &Event{
		AgentRunID: fmt.Sprintf("%s:%s", snapshot.AgentID, snapshot.RunID),
		NodeName:   snapshot.NodeID,
		NodeStatus: snapshot.NodeStatus,
		Payload:    fmt.Sprintf("Node [%s] transitioned to state [%s]", snapshot.NodeID, snapshot.NodeStatus),
		Timestamp:  time.Now().UnixMilli(),
	}
}

func (event *Event) Bytes() []byte {
	b, _ := json.Marshal(event)
	return b
}

type Agent struct {
	ID       string        `json:"id"`
	Metadata AgentMetadata `json:"metadata"`
}

type AgentMetadata struct {
	Type        string   `json:"type"`
	Description string   `json:"description"`
	Category    string   `json:"category"`
	NodeIDList  []string `json:"node_id_list"`
}

type AgentRunDetail struct {
	AgentRunID string `json:"agent_run_id"` // REASON: the full agent_run_id is tracked here because this will be used to hydrate the UI
	//															 clicking on this run from UI should send <agent_id>:<run_id> to watch
	TaskName        string `json:"task_name"` // Associated Task or brief of the task
	TaskDescription string `json:"task_description"`
	TaskID          string `json:"task_id"` //OPTIONAL: task ID if we're tracking agents by tasks
	CreatedBy       string `json:"created_by"`
	CreatedAt       int64  `json:"created_at"`
}

type AgentRunSnapshot struct {
	// This is the request as per sent to from the agent data source (langchain etc.) it's not concerned except with agent_id and run_id
	// The snapshot source doesn't care about the way we structure the internal memtable id
	AgentID    string `json:"agent_id"`
	RunID      string `json:"run_id"`
	NodeID     string `json:"node_id"`     // I think that's Thinking, Searching etc.
	NodeStatus string `json:"node_status"` // I think that is Completed, In Progress, Stopped etc.
}

type WatchRequest struct {
	ClientID string        `json:"client_id"`
	Agents   []TargetAgent `json:"agents"`
	Ctx      context.Context
}

type TargetAgent struct {
	ID         string `json:"id"`
	LatestOnly bool   `json:"latest_only"`
}
