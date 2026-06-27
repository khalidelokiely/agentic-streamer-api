// Copyright 2026 The Agentic Streamer Authors.
// SPDX-License-Identifier: Apache-2.0

// Package memtable implements a high-performance, concurrent, in-memory
// storage engine optimized for streaming agent run telemetry and event logs.
//
// The core underlying data structure is an augmented bidirectional Skip List
// that supports logarithmic search, insertion, and deletion profiles alongside
// fast, bidirectional constant-time pointer sequencing for suffix range scans.
//
// Concurrency Model:
//   - Multiple readers can execute concurrent queries via shared reader-locks (RLock).
//   - Single writers isolate structural mutations via exclusive locks (Lock).
//   - Elements stored inside the collection are pointer references (*T). External
//     consumers must treat returned pointers as structurally immutable to avoid
//     uncontrolled data race conditions across boundary layers.
package memtable

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
)

// Node represents a single structural element located inside the multi-level Skip List.
type Node[T any] struct {
	// key represents the lexicographically sorted identifier (e.g., "agent_id:run_id:sequence").
	key string
	// value holds the pointer reference to the payload generic model type.
	value *T
	// forward stores pointer chains to downstream nodes partitioned across express tracks.
	// Index represents the height level of the corresponding track layout.
	forward []*Node[T]
	// backward provides a direct structural link to the immediate left neighbor strictly at Level 0.
	// This enables fast reverse linear iteration for suffix-bound data tracking.
	backward *Node[T]
}

// MemTable provides thread-safe, ordered string-to-interface tracking storage.
// It is primarily utilized to maintain timeline snapshot event histories for streaming graphs.
type MemTable[T any] struct {
	// maxLevel establishes the hard ceiling limit for express-lane tracks.
	maxLevel int
	// currentLevel reflects the maximum height layer currently initialized with nodes.
	currentLevel int
	// head serves as the permanent sentinel entrypoint node containing zero values.
	head *Node[T]
	// mu guards structural topologies across concurrent read and write operations.
	mu sync.RWMutex
}

// NewMemTable provisions and returns an initialized instance of an empty MemTable index.
// The maxLevel parameter defines the structural express track limitation; a value of
// 12 to 16 is typically optimal for holding millions of dynamic streaming telemetry keys.
func NewMemTable[T any](maxLevel int) *MemTable[T] {
	return &MemTable[T]{
		maxLevel:     maxLevel,
		currentLevel: 0,
		head:         &Node[T]{key: "", value: nil, forward: make([]*Node[T], maxLevel)},
	}
}

// Query performs a prefix-based forward scan across the ordered dataset collection.
// It returns a slice of generic item pointers matching the specified base key filter layout.
//
// Example:
//   - Querying prefix "agent-1" returns keys like "agent-1:run-1", "agent-1:run-2" ordered chronologically.
func (m *MemTable[T]) Query(key string) []*T {
	prefix := fmt.Sprintf("%s:", key)
	events := make([]*T, 0)

	// OPTIMIZATION: Converted to shared RLock to maximize parallel high-throughput scans.
	m.mu.RLock()
	defer m.mu.RUnlock()

	curr := m.head

	// Step 1: Traverse the express lanes downward to locate the closest left element.
	for i := m.currentLevel; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].key < prefix {
			curr = curr.forward[i]
		}
	}

	// Step 2: Drop into Level 0 down track to identify the prospective starting point match.
	curr = curr.forward[0]

	// Step 3: Stream items forward linearly matching the prefix boundary constraints.
	for curr != nil && strings.HasPrefix(curr.key, prefix) {
		events = append(events, curr.value)
		curr = curr.forward[0]
	}

	return events
}

// QueryLastN scans the dataset inversely to extract the most recent N items recorded
// matching a given prefix key context. This is highly optimized for polling real-time tails.
//
// The search boundary utilizes the max-byte marker character (\xff) to establish an
// absolute ceiling threshold vector for the initial logarithmic jump sequence.
func (m *MemTable[T]) QueryLastN(key string, lastN int) []*T {
	prefix := fmt.Sprintf("%s:", key)
	searchKey := fmt.Sprintf("%s:\xff", key)

	events := make([]*T, 0)

	// OPTIMIZATION: Converted to shared RLock to allow parallel telemetry polling.
	m.mu.RLock()
	defer m.mu.RUnlock()

	curr := m.head

	// Step 1: Logarithmically jump to the highest entry pointing right before the ceiling threshold.
	for i := m.currentLevel; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].key < searchKey {
			curr = curr.forward[i]
		}
	}

	// Step 2: Retreat one node via the backward lane pointer to land directly on the newest record match.
	curr = curr.backward

	// Step 3: Iterate backward linearly through the timeline until capacity or boundaries breach.
	for curr != nil && strings.HasPrefix(curr.key, prefix) {
		events = append(events, curr.value)
		if len(events) == lastN {
			break
		}
		curr = curr.backward
	}

	return events
}

// Put inserts or completely updates a payload element within the synchronized index array.
// If an identical matching key configuration already exists, its internal payload address is swapped out.
func (m *MemTable[T]) Put(key string, value *T) {
	// Pre-allocate tracking array buffer memory outside critical lock region to minimize lock contention intervals.
	update := make([]*Node[T], m.maxLevel)
	newNodeLevel := m.getLevel()
	newNode := &Node[T]{key: key, value: value, forward: make([]*Node[T], newNodeLevel+1), backward: nil}

	m.mu.Lock()
	defer m.mu.Unlock()

	curr := m.head

	// Step 1: Audit and record traversal coordinates where pointers will be sliced and restructured.
	for i := m.currentLevel; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].key < key {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	// Step 2: Handle idempotent overwrite logic if the absolute matching key target already exists.
	if curr.forward[0] != nil && curr.forward[0].key == key {
		curr.forward[0].value = value
		return
	}

	// Step 3: Expand the active express track index ceilings if the randomized level exceeds current bounds.
	if newNodeLevel > m.currentLevel {
		for i := m.currentLevel + 1; i <= newNodeLevel; i++ {
			update[i] = m.head
		}
		m.currentLevel = newNodeLevel
	}

	// Step 4: Execute atomic bidirectional splicing strictly along the Level 0 layout plane.
	newNode.backward = update[0]
	if update[0].forward[0] != nil {
		update[0].forward[0].backward = newNode
	}
	newNode.forward[0] = update[0].forward[0]
	update[0].forward[0] = newNode

	// Step 5: Route structural update pointers across upper levels.
	for i := 1; i <= newNodeLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
}

// getLevel generates a geometric distribution level assignment via a pseudo-random coin-flip execution.
// Returns an integer constrained strictly between 0 and (maxLevel - 1).
func (m *MemTable[T]) getLevel() int {
	level := 0

	// TODO(khalidelokiely): If high-concurrency profiling reveals performance bottlenecks within
	// math/rand's shared global source lock generator, substitute this execution with an optimized
	// thread-local fast pseudo-random generator routine.
	for rand.Float64() > 0.5 && level < m.maxLevel-1 {
		level++
	}

	return level
}
