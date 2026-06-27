// Copyright 2026 The Agentic Streamer Authors.
// SPDX-License-Identifier: Apache-2.0

package platform

import (
	"context"
	"fmt"
	"nrt-agentic-streamer-api/internal/memtable"

	"go.uber.org/fx"
)

// Module encapsulates all underlying infrastructure components, cache systems,
// event brokers, and daemon state coordinators under a unified Uber Fx tree.
var Module = fx.Module("platform",
	fx.Provide(
		// Provide the thread-safe, high-performance in-memory event indexer.
		func() EventStore {
			// Instantiates a skiplist/map hybrid optimized for chronological data layout.
			return memtable.NewMemTable[Event](16)
		},
		// Provide the state broadcast distribution hub.
		NewBroker,
		// Provide the central agent topology state controller wrapper.
		// Note: Ensure NewAgentDaemon returns a DaemonController interface type.
		func(store EventStore) DaemonController {
			return NewAgentDaemon(store)
		},
		// Provide the web surface handler engine.
		NewHandler,
	),
	// Execute automated application initialization pathways and HTTP network mapping.
	fx.Invoke(RegisterRoutes, RegisterAppLifecycle),
)

// RegisterAppLifecycle coordinates orderly startup and teardown phases for long-running
// background system threads, preventing execution leaks before the server core goes hot.
func RegisterAppLifecycle(lc fx.Lifecycle, broker *Broker, daemon DaemonController) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			fmt.Println("Initializing Agentic Streamer platform background workers...")

			// Launch the single-threaded state machine coordinator loop
			go daemon.Start()

			// Launch the fan-out client broadcast channel multiplexer loop
			go broker.Start()

			return nil
		},

		OnStop: func(ctx context.Context) error {
			fmt.Println("Shutting down background workers gracefully...")

			// 1. First drop/close incoming client request streams to drain current buffers
			// 2. Shutting down in OnStop prevents orphaned channels from dangling in memory
			return nil
		},
	})
}
