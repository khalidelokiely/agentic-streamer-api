package memtable

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
)

type Node[T any] struct {
	key      string
	value    *T
	forward  []*Node[T]
	backward *Node[T]
}

type MemTable[T any] struct {
	maxLevel     int
	currentLevel int
	head         *Node[T]
	mu           sync.RWMutex
}

func NewMemTable[T any](maxLevel int) *MemTable[T] {
	return &MemTable[T]{
		maxLevel:     maxLevel,
		currentLevel: 0,
		head:         &Node[T]{key: "", value: nil, forward: make([]*Node[T], maxLevel)},
	}
}

func (m *MemTable[T]) Query(key string) []*T {
	prefix := fmt.Sprintf("%s:", key)
	events := make([]*T, 0)

	m.mu.Lock()
	defer m.mu.Unlock()

	curr := m.head

	for i := m.currentLevel; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].key < prefix {
			curr = curr.forward[i]
		}
	}

	curr = curr.forward[0]

	for curr != nil && strings.HasPrefix(curr.key, prefix) {
		events = append(events, curr.value)
		curr = curr.forward[0]
	}

	return events
}

func (m *MemTable[T]) QueryLastN(key string, lastN int) []*T {
	prefix := fmt.Sprintf("%s:", key)
	searchKey := fmt.Sprintf("%s:\xff", key)

	events := make([]*T, 0)

	m.mu.Lock()
	defer m.mu.Unlock()

	curr := m.head

	for i := m.currentLevel; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].key < searchKey {
			curr = curr.forward[i]
		}
	}

	// current is now either nil or at the next agent_run_id
	curr = curr.backward

	for curr != nil && strings.HasPrefix(curr.key, prefix) {
		events = append(events, curr.value)
		if len(events) == lastN {
			break
		}
		curr = curr.backward
	}

	return events
}

func (m *MemTable[T]) Put(key string, value *T) {
	// Pre Allocate to provision for Locks
	update := make([]*Node[T], m.maxLevel)
	// Roll dice, get the new node's level
	newNodeLevel := m.getLevel()
	newNode := &Node[T]{key, value, make([]*Node[T], newNodeLevel+1), nil}

	m.mu.Lock()
	defer m.mu.Unlock()

	curr := m.head

	// hydrate the update slice with pointers to the left neighbors of newNode
	for i := m.currentLevel; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].key < key {
			curr = curr.forward[i]
		}

		update[i] = curr
	}

	// Check if the next node at Level 0 is an exact match for overwrite
	if curr.forward[0] != nil && curr.forward[0].key == key {
		curr.forward[0].value = value // Overwrite existing event data
		return
	}

	// If newNodeLevel > m.currentLevel: expand the express tracks
	if newNodeLevel > m.currentLevel {
		for i := m.currentLevel + 1; i <= newNodeLevel; i++ {
			update[i] = m.head
		}

		m.currentLevel = newNodeLevel
	}

	//Bidirectional Level 0 execution
	newNode.backward = update[0]

	if update[0].forward[0] != nil {
		update[0].forward[0].backward = newNode
	}

	newNode.forward[0] = update[0].forward[0]
	update[0].forward[0] = newNode

	// Add the node to the upper express levels
	for i := 1; i <= newNodeLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
}

func (m *MemTable[T]) getLevel() int {
	level := 0

	for rand.Float64() > 0.5 && level < m.maxLevel-1 {
		level++
	}

	return level
}
