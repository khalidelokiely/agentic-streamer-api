# Agentic Streamer API

[![Concurrency & Race Check](https://github.com/khalidelokiely/agentic-streamer-api/actions/workflows/main.yml/badge.svg)](https://github.com/khalidelokiely/agentic-streamer-api/actions/workflows/main.yml)

A high-performance REST API backend for live agentic updates powered by Go and server-sent events (SSE). Built with [Hertz](https://www.cloudwego.io/) and [Uber FX](https://github.com/uber-go/fx), this service enables real-time streaming of agent execution events to multiple clients simultaneously.

## 🚀 Live Demo
![Deployed on Railway](https://img.shields.io/badge/Deployed%20on-Railway-0B0D0E?style=for-the-badge&logo=railway)


**Production URL:** https://agentic-streamer-api-production.up.railway.app/

> **Note:** Currently, the service seeds and generates fake run events for demo purposes. See [Future Enhancements](#-future-enhancements) for the planned push-based event ingestion endpoint.

## Features

- **Real-time SSE Streaming** - Stream agent execution events to multiple clients simultaneously with buffered channels to prevent blocking
- **Agent Monitoring** - Track available agents, their runs, and detailed execution events
- **Watch Management** - Subscribe to specific agents and receive live updates
- **Memory-backed Storage** - In-memory state management with efficient data structures
- **CORS Enabled** - Pre-configured CORS for development and production frontends
- **Health Checks** - Built-in health endpoint for monitoring and deployment verification
- **Efficient Message Routing** - Broker-based message distribution with subscription model

## 📋 Architecture

### Core Components

- **Broker** (`internal/platform/broker.go`) - Central message distribution hub managing client subscriptions and event routing
- **Daemon** (`internal/platform/daemon.go`) - Background controller managing agent lifecycle and event tracking
- **Handler** (`internal/platform/handlers.go`) - HTTP request handlers for all API endpoints
- **MemTable** (`internal/memtable/memtable.go`) - In-memory data structure for tracking node statuses
- **Types** (`internal/platform/types.go`) - Domain models for events, agents, and watch requests

### Tech Stack

- **Language:** Go 1.23
- **HTTP Framework:** [Cloudwego Hertz](https://www.cloudwego.io/) - High-performance async HTTP framework
- **Dependency Injection:** [Uber FX](https://github.com/uber-go/fx) - Application lifecycle management
- **Unique IDs:** [ULID v2](https://github.com/oklog/ulid/v2) - Sortable unique identifiers
- **Deployment:** Railway.app with NixPacks

## 📡 API Endpoints

All endpoints are prefixed with `/v1/agents`

### Metadata & Inspection

- `GET /` - Get list of available agents
- `GET /:agentId/runs` - Get all runs for a specific agent
- `GET /:agentId/runs/:runId/events` - Get events for a specific agent run

### Streaming

- `GET /sse?clientId=<CLIENT_ID>` - Establish SSE connection for live event streaming
- `POST /watch` - Subscribe to agent updates
- `DELETE /watch/:agentId?clientId=<CLIENT_ID>` - Unsubscribe from agent updates

### System Diagnostics

- `GET /watchers` - Get current watchers and connected clients
- `GET /health` - Health check endpoint (used by Railway for deployment validation)

## 🔌 API Examples

### Get Available Agents

```bash
curl "https://agentic-streamer-api-production.up.railway.app/v1/agents"
```

### Connect to Event Stream

```bash
curl -N "https://agentic-streamer-api-production.up.railway.app/v1/agents/sse?clientId=client-123"
```

### Subscribe to Agent Updates

```bash
curl -X POST "https://agentic-streamer-api-production.up.railway.app/v1/agents/watch" \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "client-123",
    "agents": [
      {
        "id": "codepal-v1:run_uuid_10000",
        "latest_only": false
      }
    ]
  }'
```

### Get Runs for a Specific Agent

```bash
curl "https://agentic-streamer-api-production.up.railway.app/v1/agents/codepal-v1/runs"
```

### Get Events for a Specific Agent Run

```bash
curl "https://agentic-streamer-api-production.up.railway.app/v1/agents/codepal-v1/runs/run_uuid_10000/events"
```

### Unsubscribe from Agent

```bash
curl -X DELETE "https://agentic-streamer-api-production.up.railway.app/v1/agents/watch/codepal-v1?clientId=client-123"
```

## 🏗️ Project Structure

```
agentic-streamer-api/
├── cmd/
│   └── api/
│       └── main.go           # Application entry point
├── internal/
│   ├── platform/
│   │   ├── broker.go         # Message broker for event distribution
│   │   ├── daemon.go         # Agent daemon controller
│   │   ├── handlers.go       # HTTP handlers
│   │   ├── interfaces.go     # Interface definitions
│   │   ├── module.go         # FX module configuration
│   │   ├── routes.go         # Route registration
│   │   └── types.go          # Data types and structures
│   └── memtable/
│       └── memtable.go       # In-memory state management
├── go.mod
├── go.sum
├── railway.toml              # Railway.app deployment config
└── README.md
```

## 🛠️ Local Development

### Prerequisites

- Go 1.23+
- Git

### Testing
```bash
#Race tests
go test ./... --race 
```

### Installation

```bash
# Clone the repository
git clone https://github.com/khalidelokiely/agentic-streamer-api.git
cd agentic-streamer-api

# Install dependencies
go mod download

# Build the application
go build -o out ./cmd/api/main.go

# Run locally
./out
```

The API will start on `http://localhost:80` by default. You can override the port using the `PORT` environment variable:

```bash
PORT=8080 ./out
```

### Testing the Local Deployment

```bash
# In one terminal, start the server
PORT=8080 ./out

# In another terminal, subscribe to demo events
curl -X POST "http://localhost:8080/v1/agents/watch" \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "local-client",
    "agents": [
      {
        "id": "codepal-v1:run_uuid_10000",
        "latest_only": false
      }
    ]
  }'

# In yet another terminal, connect to the event stream
curl -N "http://localhost:8080/v1/agents/sse?clientId=local-client"
```

## 🚀 Deployment

This project is configured for deployment on Railway.app using NixPacks.

### Deployment Configuration (`railway.toml`)

```toml
[build]
builder = "nixpacks"
buildCommand = "go build -o out ./cmd/api/main.go"

[deploy]
startCommand = "./out"
healthcheckPath = "/health"
```

### Deploy to Railway

1. Connect your repository to Railway
2. Railway will automatically detect `railway.toml` and build/deploy accordingly
3. The service is live at: https://agentic-streamer-api-production.up.railway.app/

## 📊 Event Data Model

Events streamed through SSE follow this structure:

```json
{
  "agent_run_id": "codepal-v1:run_uuid_10000",
  "node_name": "llm_call",
  "status": "EXECUTING",
  "payload": "Node [llm_call] transitioned to state [EXECUTING]",
  "timestamp": 1687123456789
}
```

## 🔄 Watch Request Model

Subscribe to agent updates with:

```json
{
  "client_id": "client-123",
  "agents": [
    {
      "id": "codepal-v1:run_uuid_10000",
      "latest_only": false
    }
  ]
}
```

- `client_id`: Unique identifier for the client (used to route events)
- `agents`: List of target agents to watch
- `latest_only`: If `true`, only stream events from the latest run; if `false`, stream all events

## 🔐 CORS Configuration

Pre-configured CORS origins:
- `http://localhost:5173` (local development)
- `https://agentic-streamer-ui-react.vercel.app` (production UI)

Allowed methods: GET, POST, DELETE, OPTIONS

## 🐛 Key Implementation Details

### Buffered Channels

Client channels use a buffer size of 100 messages to prevent slow clients from blocking the broker lock:

```go
clientChannel := make(chan Event, 100)
```

### Graceful Connection Cleanup

- Automatic client removal from broker registry when handlers exit
- Proper error handling for write failures with immediate connection termination
- Context-based cancellation for clean shutdown

### ULID Generation

Events are assigned sortable unique identifiers using ULID v2:

```go
entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
ms := ulid.Timestamp(time.Now())
ulid, err := ulid.New(ms, entropy)
```

## 🤝 Integrations

This API is designed to work with:
- **Frontend:** [Agentic Streamer UI](https://agentic-streamer-ui-react.vercel.app) (React)
- **Agent Framework:** LangChain, AutoGen, or custom agent implementations
- **Event Sources:** Currently seeds demo events; push-based ingestion coming soon (see Future Enhancements)

## 📝 License

This project is open source and available on GitHub.

## 🤔 Questions or Issues?

For bugs, feature requests, or questions, please open an issue on [GitHub](https://github.com/khalidelokiely/agentic-streamer-api/issues).

## 🎯 Future Enhancements

- [ ] **Push-based Event Ingestion Endpoint** - Create a dedicated endpoint for external services (LangChain, AutoGen, etc.) to push agent run events into the streamer instead of relying on seeded demo data
- [ ] Improve some lock mechanisms in the AgentDaemon
- [ ] Add Caching for AgentMetadata so handler can use it instead of hitting the AgentDaemon - reducing lock contention
- [ ] Graceful shutdown and persistence of events upon run completion
- [ ] Persistent storage backend (PostgreSQL/MongoDB)
- [ ] Authentication & authorization layer
- [ ] Event filtering and query capabilities
- [ ] Metrics and observability (Prometheus, Grafana)
- [ ] Rate limiting and backpressure handling
- [ ] WebSocket support as alternative to SSE
