// Copyright 2026 The Agentic Streamer Authors.
// SPDX-License-Identifier: Apache-2.0

// Package platform provisions core synchronization mechanics, background daemons,
// and routing fabrics to drive real-time agent execution networks.
//
// The broker sub-module acts as a high-throughput event multicast hub and
// dynamic subscription manager. It implements a Copy-On-Write (COW)
// concurrency architecture utilizing atomic snapshots to enable lock-free
// read distributions for active event streaming pipelines.
package platform

import (
	"fmt"
	"log"
	"maps"
	"strings"
	"sync/atomic"
)

// client maintains state contexts and network connection channels for an active
// Server-Sent Events (SSE) stream consumer instance.
type client struct {
	// channel acts as the outbound FIFO queue for multiplexed event packets.
	channel chan Event
	// watchList registers exact unique identifiers currently tracked by this client.
	watchList map[AgentRunID]bool
	// exclusions maintains localized overrides blocking specific sub-stream updates
	// when a client subscribes to a parent wildcard configuration.
	exclusions map[AgentRunID]bool
	// clientID uniquely identifies the remote target session node.
	clientID string
}

// UnwatchRequest models a mutation payload used to strip target subscriptions
// from an active client framework context. It supports batch unwatching.
type UnwatchRequest struct {
	// ClientID specifies the target consumer profile to modify.
	ClientID string `json:"client_id"`
	// AgentRunIDs maps targeted stream IDs to detach.
	AgentRunIDs []AgentRunID `json:"agent_run_ids"`
}

// clientConnectMsg handles synchronization handshakes across thread boundaries.
type clientConnectMsg struct {
	clientID string
	channel  chan Event
}

// RoutingSnapshot defines the immutable structural framework swapped atomically
// inside the Broker's configuration registry.
type RoutingSnapshot struct {
	// clients maps active transaction IDs to internal tracking profiles.
	clients map[string]*client
	// agentRunWatchList provides a reverse index lookup tracing from a given Agent or
	// Run ID out to a multi-map group of subscribing client accounts.
	agentRunWatchList map[AgentRunID]map[string]bool
}

// Broker coordinates event routing and stream subscriptions between underlying state
// daemons and downstream HTTP consumer pools. It processes mutations sequentially
// through channels to avoid multi-thread locking contention.
type Broker struct {
	// routingTable stores an atomic pointer to a read-optimized *RoutingSnapshot configuration.
	routingTable atomic.Value

	// watchQueue processes incoming registration requests for streaming tracking targets.
	watchQueue chan WatchRequest
	// unwatchQueue processes requests to release monitoring hooks.
	unwatchQueue chan UnwatchRequest
	// incomingEventQueue receives telemetry vectors from execution environments.
	incomingEventQueue chan Event
	// removeClientQueue detaches dead connections and purges routing references.
	removeClientQueue chan clientConnectMsg
	// addClientQueue hooks fresh or reconnected client event channels into the matrix.
	addClientQueue chan clientConnectMsg

	// agentDaemon references the underlying master database coordinator system.
	agentDaemon DaemonController
}

// NewBroker constructs and returns an operational Event Multiplexer Broker instance.
func NewBroker(agentDaemon DaemonController) *Broker {
	b := &Broker{
		watchQueue:         make(chan WatchRequest, 100),
		unwatchQueue:       make(chan UnwatchRequest, 100),
		incomingEventQueue: make(chan Event, 100),
		removeClientQueue:  make(chan clientConnectMsg, 100),
		addClientQueue:     make(chan clientConnectMsg, 100),
		agentDaemon:        agentDaemon,
	}

	b.routingTable.Store(&RoutingSnapshot{
		clients:           make(map[string]*client),
		agentRunWatchList: make(map[AgentRunID]map[string]bool),
	})

	return b
}

// Start boots the continuous operational event coordinator sequence loop.
// This method blocks indefinitely and must execute inside a dedicated worker goroutine.
func (b *Broker) Start() {
	fmt.Println("Starting broker coordinator loop...")

	b.agentDaemon.Attach(b)
	defer b.agentDaemon.Detach(b)

	for {
		select {
		case msg := <-b.addClientQueue:
			b.processAddClient(msg.clientID, msg.channel)

		case req := <-b.watchQueue:
			b.processWatchRequest(req)

		case event := <-b.incomingEventQueue:
			b.notifyClientChannels(&event)

		case msg := <-b.removeClientQueue:
			b.processRemoveClientRequest(msg)

		case req := <-b.unwatchQueue:
			b.processUnwatchRequest(req)
		}
	}
}

// Process consumes execution vectors emitted by attached state controllers.
// This implements the Observable pipeline contract interface using non-blocking dispatch configurations.
func (b *Broker) Process(event Event) {
	select {
	case b.incomingEventQueue <- event:
	default:
		log.Println("Warning: Broker incoming queue full, dropping event")
	}
}

// notifyClientChannels broadcasts event payloads out to matched subscribers.
// It reads exclusively from the current atomic pointer snapshot, allowing concurrent
// execution alongside ongoing configuration modifications.
func (b *Broker) notifyClientChannels(events ...*Event) {
	snapshot := b.routingTable.Load().(*RoutingSnapshot)

	for _, event := range events {
		if event == nil {
			continue
		}

		agentRunID := event.AgentRunID

		// Path A: Route to subscribers targeting this unique runtime instance.
		for clientID := range snapshot.agentRunWatchList[agentRunID] {
			client, exists := snapshot.clients[clientID]
			if !exists {
				continue
			}

			if _, excluded := client.exclusions[agentRunID]; excluded {
				continue
			}

			b.sendToClient(clientID, event)
		}

		// Path B: Route to wildcard subscribers (format: "agent_id:*").
		if wildCardKey, err := agentRunID.GetWildCardKey(); err == nil {
			for clientID := range snapshot.agentRunWatchList[wildCardKey] {
				client, exists := snapshot.clients[clientID]
				if !exists {
					continue
				}
				if _, excluded := client.exclusions[agentRunID]; excluded {
					continue
				}
				b.sendToClient(clientID, event)
			}
		}
	}
}

// sendToClient routes a payload pointer safely to an individual client's outbound buffer channel.
func (b *Broker) sendToClient(clientID string, event *Event) {
	snapshot := b.routingTable.Load().(*RoutingSnapshot)

	client, exists := snapshot.clients[clientID]
	if exists && client.channel != nil {
		select {
		case client.channel <- *event:
		default:
			log.Printf("Warning: Client %s buffer full, dropping event\n", clientID)
		}
	}
}

// processAddClient maps a live connection thread reference to a unique client configuration vector.
func (b *Broker) processAddClient(clientID string, ch chan Event) {
	current := b.routingTable.Load().(*RoutingSnapshot)

	nextClients := maps.Clone(current.clients)
	nextWatchList := maps.Clone(current.agentRunWatchList)

	oldClient, exists := nextClients[clientID]

	if !exists {
		nextClients[clientID] = &client{
			channel:    ch,
			watchList:  make(map[AgentRunID]bool),
			exclusions: make(map[AgentRunID]bool),
		}
	} else {
		nextClients[clientID] = &client{
			channel:    ch,
			watchList:  maps.Clone(oldClient.watchList),
			exclusions: maps.Clone(oldClient.exclusions),
		}
	}

	b.routingTable.Store(&RoutingSnapshot{
		clients:           nextClients,
		agentRunWatchList: nextWatchList,
	})

	if exists && len(oldClient.watchList) > 0 {
		go func(clientCh chan Event, watchMap map[AgentRunID]bool) {
			for agentID := range watchMap {
				if event := b.agentDaemon.QueryLatest(agentID.String()); event != nil {
					select {
					case clientCh <- *event:
					default:
						return
					}
				}
			}
		}(ch, oldClient.watchList)
	}
}

// processWatchRequest maps new target stream keys to a client's subscription table.
func (b *Broker) processWatchRequest(req WatchRequest) {
	current := b.routingTable.Load().(*RoutingSnapshot)

	oldClient, clientExists := current.clients[req.ClientID]
	if !clientExists {
		oldClient = &client{
			channel:    nil,
			watchList:  make(map[AgentRunID]bool),
			exclusions: make(map[AgentRunID]bool),
		}
	}

	nextClients := maps.Clone(current.clients)
	nextWatchList := maps.Clone(current.agentRunWatchList)

	clonedClient := &client{
		channel:    oldClient.channel,
		watchList:  maps.Clone(oldClient.watchList),
		exclusions: maps.Clone(oldClient.exclusions),
	}
	nextClients[req.ClientID] = clonedClient

	for _, agent := range req.Agents {
		if nextWatchList[agent.ID] == nil {
			nextWatchList[agent.ID] = make(map[string]bool)
		} else {
			nextWatchList[agent.ID] = maps.Clone(current.agentRunWatchList[agent.ID])
		}

		nextWatchList[agent.ID][req.ClientID] = true
		clonedClient.watchList[agent.ID] = true
	}

	b.routingTable.Store(&RoutingSnapshot{
		clients:           nextClients,
		agentRunWatchList: nextWatchList,
	})

	if clonedClient.channel != nil {
		for _, agent := range req.Agents {
			if agent.LatestOnly {
				if latestEvent := b.agentDaemon.QueryLatest(agent.ID.String()); latestEvent != nil {
					b.sendToClient(req.ClientID, latestEvent)
				}
			}
		}
	}
}

// processRemoveClientRequest tears down tracking nodes for a closed connection context.
func (b *Broker) processRemoveClientRequest(c clientConnectMsg) {
	current := b.routingTable.Load().(*RoutingSnapshot)

	oldClient, exists := current.clients[c.clientID]
	if !exists || c.channel != oldClient.channel {
		return
	}

	nextClients := maps.Clone(current.clients)
	nextWatchList := maps.Clone(current.agentRunWatchList)

	delete(nextClients, c.clientID)

	for agentRunID := range oldClient.watchList {
		newWatchListForAgent := maps.Clone(current.agentRunWatchList[agentRunID])
		delete(newWatchListForAgent, c.clientID)
		if len(newWatchListForAgent) == 0 {
			delete(nextWatchList, agentRunID)
		} else {
			nextWatchList[agentRunID] = newWatchListForAgent
		}
	}

	b.routingTable.Store(&RoutingSnapshot{
		clients:           nextClients,
		agentRunWatchList: nextWatchList,
	})
}

// processUnwatchRequest parses structural targets to detach individual subscription branches.
func (b *Broker) processUnwatchRequest(req UnwatchRequest) {
	current := b.routingTable.Load().(*RoutingSnapshot)

	oldClient, exists := current.clients[req.ClientID]
	if !exists {
		return
	}

	nextClients := maps.Clone(current.clients)
	nextWatchList := maps.Clone(current.agentRunWatchList)

	clonedClient := &client{
		channel:    oldClient.channel,
		watchList:  maps.Clone(oldClient.watchList),
		exclusions: maps.Clone(oldClient.exclusions),
	}

	for _, agent := range req.AgentRunIDs {
		_, watchedExplicitly := clonedClient.watchList[agent]

		if !watchedExplicitly {
			// Leverage native type parsing method instead of raw sliced-string assumptions
			if wildCardKey, err := agent.GetWildCardKey(); err == nil {
				if _, watchedViaWildcard := clonedClient.watchList[wildCardKey]; watchedViaWildcard {
					clonedClient.exclusions[agent] = true
					continue
				}
			}
		}

		// Fixed slicing type compile bugs safely using underlying explicit casts
		if strings.HasSuffix(string(agent), "*") {
			prefix := strings.TrimSuffix(string(agent), "*")
			for exclusionKey := range clonedClient.exclusions {
				if strings.HasPrefix(string(exclusionKey), prefix) {
					delete(clonedClient.exclusions, exclusionKey)
				}
			}
		}

		delete(clonedClient.watchList, agent)

		clientsMap, exists := current.agentRunWatchList[agent]
		if !exists {
			continue
		}

		clonedClientsMap := maps.Clone(clientsMap)
		delete(clonedClientsMap, req.ClientID)

		if len(clonedClientsMap) == 0 {
			delete(nextWatchList, agent)
		} else {
			nextWatchList[agent] = clonedClientsMap
		}
	}

	nextClients[req.ClientID] = clonedClient

	b.routingTable.Store(&RoutingSnapshot{
		clients:           nextClients,
		agentRunWatchList: nextWatchList,
	})
}

// PUBLIC API SURFACE BOUNDARIES (Thread-safe interface entry points)

// AddClient marshals an connection handshake parameter vector onto the state machine.
func (b *Broker) AddClient(clientID string, ch chan Event) {
	b.addClientQueue <- clientConnectMsg{clientID: clientID, channel: ch}
}

// RemoveClient dispatches a request to disconnect a streaming socket loop session.
func (b *Broker) RemoveClient(clientID string, clientChan chan Event) {
	b.removeClientQueue <- clientConnectMsg{clientID: clientID, channel: clientChan}
}

// Unwatch enqueues an asynchronous batch request to drop targeted telemetry tracking parameters.
func (b *Broker) Unwatch(req UnwatchRequest) {
	b.unwatchQueue <- req
}

// GetCurrentMaps exports an isolated debug configuration context snapshot of active routes.
func (b *Broker) GetCurrentMaps() map[string]interface{} {
	snapshot := b.routingTable.Load().(*RoutingSnapshot)

	exportedAgents := maps.Clone(snapshot.agentRunWatchList)
	exportedClients := make(map[string]interface{})

	for id, c := range snapshot.clients {
		exportedClients[id] = map[string]interface{}{
			"watching":   maps.Clone(c.watchList),
			"exclusions": maps.Clone(c.exclusions),
		}
	}

	return map[string]interface{}{
		"agents":  exportedAgents,
		"clients": exportedClients,
	}
}
