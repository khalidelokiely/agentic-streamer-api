package platform

import (
	"fmt"
	"log"
	"maps"
	"strings"
	"sync/atomic"
)

type client struct {
	channel    chan Event
	watchList  map[string]bool // Tracks which agents this specific client is watching
	exclusions map[string]bool
}

type UnwatchRequest struct {
	agentIDList []string
	clientID    string
}

type clientConnectMsg struct {
	clientID string
	channel  chan Event
}

type RoutingSnapshot struct {
	clients        map[string]*client
	agentWatchList map[string]map[string]bool
}

type Broker struct {
	// 1. Shifted to pointers to allow clean, in-place slice mutations
	routingTable       atomic.Value
	watchQueue         chan WatchRequest
	unwatchQueue       chan UnwatchRequest
	incomingEventQueue chan Event
	removeClientQueue  chan string
	addClientQueue     chan clientConnectMsg // New queue to eliminate the HTTP thread race

	agentDaemon DaemonController
}

func NewBroker(agentDaemon DaemonController) *Broker {
	b := &Broker{
		watchQueue:         make(chan WatchRequest, 100),
		unwatchQueue:       make(chan UnwatchRequest, 100),
		incomingEventQueue: make(chan Event, 100),
		removeClientQueue:  make(chan string, 100),
		addClientQueue:     make(chan clientConnectMsg, 100),
		agentDaemon:        agentDaemon,
	}

	b.routingTable.Store(&RoutingSnapshot{
		clients:        make(map[string]*client),
		agentWatchList: make(map[string]map[string]bool),
	})

	return b
}

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
			// FIXED: Pass the address of the loop value safely
			b.notifyClientChannels(&event)

		case clientId := <-b.removeClientQueue:
			b.processRemoveClientRequest(clientId)

		case req := <-b.unwatchQueue:
			b.processUnwatchRequest(req)
		}
	}
}

func (b *Broker) Process(event Event) {
	// Non-blocking write to protect the Daemon thread from a slow broker
	select {
	case b.incomingEventQueue <- event:
	default:
		log.Println("Warning: Broker incoming queue full, dropping event")
	}
}

func (b *Broker) notifyClientChannels(events ...*Event) {
	snapshot := b.routingTable.Load().(*RoutingSnapshot)

	for _, event := range events {
		if event == nil {
			continue
		}

		agentRunID := event.AgentRunID

		// PATH A: Route to explicit single-run target subscribers
		for clientID := range snapshot.agentWatchList[agentRunID] {
			client, exists := snapshot.clients[clientID]
			if !exists {
				continue
			}

			if _, excluded := client.exclusions[agentRunID]; excluded {
				continue
			}

			b.sendToClient(clientID, event)
		}

		// PATH B: Route to wildcard subscribers (agentID:*)
		// Find the delimiter position cleanly without allocating new string slices
		if idx := strings.Index(agentRunID, ":"); idx != -1 {
			agentID := agentRunID[:idx]
			wildcardKey := agentID + ":*" // "codepal-v1:*"

			for clientID := range snapshot.agentWatchList[wildcardKey] {
				if _, exists := snapshot.clients[clientID].exclusions[agentRunID]; exists {
					continue
				}
				b.sendToClient(clientID, event)
			}
		}
	}
}

// Helper abstraction to isolate safety selection
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

func (b *Broker) processAddClient(clientID string, ch chan Event) {
	current := b.routingTable.Load().(*RoutingSnapshot)

	// Clone the maps
	nextClients := maps.Clone(current.clients)
	nextWatchList := maps.Clone(current.agentWatchList)

	// make the mutation on the copy
	nextClients[clientID] = &client{
		channel:    ch,
		watchList:  make(map[string]bool),
		exclusions: make(map[string]bool),
	}

	// Atomically swap
	b.routingTable.Store(&RoutingSnapshot{
		clients:        nextClients,
		agentWatchList: nextWatchList,
	})
}

func (b *Broker) processWatchRequest(req WatchRequest) {
	current := b.routingTable.Load().(*RoutingSnapshot)

	// 1. Check existence using the current snapshot first
	oldClient, clientExists := current.clients[req.ClientID]
	if !clientExists {
		return
	}

	// 2. Start with top-level shallow clones
	nextClients := maps.Clone(current.clients)
	nextWatchList := maps.Clone(current.agentWatchList)

	// 3. DEEP CLONE THE CLIENT: Protect the client struct and its inner maps
	clonedClient := &client{
		channel:    oldClient.channel,
		watchList:  maps.Clone(oldClient.watchList),  // Deep clone the watch map
		exclusions: maps.Clone(oldClient.exclusions), // Deep clone the exclusions map
	}
	// Replace the pointer in our new map so it's fully isolated
	nextClients[req.ClientID] = clonedClient

	for _, agent := range req.Agents {
		// 4. DEEP CLONE THE AGENT WATCH MAP: Protect the nested subscriber maps
		if current.agentWatchList[agent.ID] == nil {
			nextWatchList[agent.ID] = make(map[string]bool)
		} else {
			// Explicitly clone the inner map before writing to it!
			nextWatchList[agent.ID] = maps.Clone(current.agentWatchList[agent.ID])
		}

		// Now these mutations are happening on 100% isolated memory arrays
		nextWatchList[agent.ID][req.ClientID] = true
		clonedClient.watchList[agent.ID] = true
	}

	// 5. Publish the completely isolated new snapshot
	b.routingTable.Store(&RoutingSnapshot{
		clients:        nextClients,
		agentWatchList: nextWatchList,
	})

	// Fetch query logs and pass to notifications
	for _, agent := range req.Agents {
		if agent.LatestOnly {
			latestEvent := b.agentDaemon.QueryLatest(agent.ID)
			if latestEvent != nil {
				b.sendToClient(req.ClientID, latestEvent)
			}
		} else {
			events := b.agentDaemon.Query(agent.ID, -1)

			for _, event := range events {
				b.sendToClient(req.ClientID, event)
			}
		}
	}
}

func (b *Broker) processRemoveClientRequest(clientId string) {
	// Get the current RoutingSnapshot
	current := b.routingTable.Load().(*RoutingSnapshot)

	oldClient, exists := current.clients[clientId]
	if !exists {
		return
	}

	// Create Next Clients and Next WatchList (Shallow)
	nextClients := maps.Clone(current.clients)
	nextWatchList := maps.Clone(current.agentWatchList)

	delete(nextClients, clientId) //remove the client from nextClients

	//With old reference to oldClient, we can go into nextWatchList and modify it. just make sure we copy the map in
	// the key

	for agentRunID, _ := range oldClient.watchList {
		newWatchListForAgent := maps.Clone(current.agentWatchList[agentRunID])
		delete(newWatchListForAgent, clientId)
		if len(newWatchListForAgent) == 0 {
			delete(nextWatchList, agentRunID)
		} else {
			nextWatchList[agentRunID] = newWatchListForAgent
		}
	}

	b.routingTable.Store(&RoutingSnapshot{
		clients:        nextClients,
		agentWatchList: nextWatchList,
	})
}

func (b *Broker) processUnwatchRequest(req UnwatchRequest) {
	current := b.routingTable.Load().(*RoutingSnapshot)

	// Create shallow copy
	nextClients := maps.Clone(current.clients)
	nextWatchList := maps.Clone(current.agentWatchList)

	oldClient, exists := current.clients[req.clientID]

	if !exists {
		return
	}

	clonedClient := &client{
		channel:    oldClient.channel,
		watchList:  maps.Clone(oldClient.watchList),
		exclusions: maps.Clone(oldClient.exclusions),
	}

	for _, agent := range req.agentIDList {
		_, watchedExplicitly := clonedClient.watchList[agent]

		if !watchedExplicitly {
			// If not, explode on :, and append :* And check if client is watching through wildcard
			wildCardKey := strings.Split(agent, ":")[0] + ":*"

			if _, watchedViaWildcard := clonedClient.watchList[wildCardKey]; watchedViaWildcard {
				// ---> YES: Add the entire agent_run_id to the exclusions map
				clonedClient.exclusions[agent] = true
				continue
			}
		}

		if strings.HasSuffix(agent, "*") { // If req is a wild card, discard it and move on.
			prefix := strings.TrimSuffix(agent, "*") // Extracts "codepal-v1:"
			for exclusionKey := range clonedClient.exclusions {
				if strings.HasPrefix(exclusionKey, prefix) {
					delete(clonedClient.exclusions, exclusionKey)
				}
			}
		}

		delete(clonedClient.watchList, agent)

		// ---> NO: blindly delete the - clientID from agent_run_id in the nextWatchList
		// Check if the run is still active and maintaining client list
		clientsMap, exists := current.agentWatchList[agent]
		if !exists {
			continue
		}

		clonedClientsMap := maps.Clone(clientsMap)

		delete(clonedClientsMap, req.clientID)

		if len(clonedClientsMap) == 0 {
			delete(nextWatchList, agent)
		} else {
			nextWatchList[agent] = clonedClientsMap
		}

	}

	nextClients[req.clientID] = clonedClient

	b.routingTable.Store(&RoutingSnapshot{
		clients:        nextClients,
		agentWatchList: nextWatchList,
	})
}

// PUBLIC API SURFACE BOUNDARIES (Called concurrently by HTTP threads)

func (b *Broker) AddClient(clientID string, ch chan Event) {
	b.addClientQueue <- clientConnectMsg{clientID: clientID, channel: ch}
}

func (b *Broker) RemoveClient(clientID string) {
	b.removeClientQueue <- clientID
}

func (b *Broker) Unwatch(request UnwatchRequest) {
	b.unwatchQueue <- request
}

func (b *Broker) GetCurrentMaps() map[string]interface{} {
	snapshot := b.routingTable.Load().(*RoutingSnapshot)

	// Shallow clone the outer structures so external iterations cannot disrupt mutations
	exportedAgents := maps.Clone(snapshot.agentWatchList)
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
