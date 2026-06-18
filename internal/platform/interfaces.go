package platform

type Observable interface {
	Attach(Observer)
	Detach(Observer)
}

type Queryable interface {
	Query(param string, last int) []*Event
	QueryLatest(param string) *Event
}

type DaemonController interface {
	Observable
	Queryable
	Start()
	GetAgents() map[string]*AgentMetadata
	GetAgentRuns(agentId string) []*AgentRunDetail
	GetAgentRunEvents(agentRunId string) []*Event
}

type Observer interface {
	Process(event Event)
}

type EventStore interface {
	Put(key string, value *Event)
	Query(key string) []*Event
	QueryLastN(key string, lastN int) []*Event
}
