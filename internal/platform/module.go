package platform

import (
	"context"
	"fmt"
	"nrt-agentic-streamer-api/internal/memtable"

	"go.uber.org/fx"
)

var Module = fx.Module("platform",
	fx.Provide(
		func() EventStore {
			// Instantiates the decoupled utility specifically bound to the domain object
			return memtable.NewMemTable[Event](16)
		},
		NewBroker,
		NewAgentDaemon,
		NewHandler,
	),
	fx.Invoke(RegisterRoutes, RegisterAppLifecycle),
)

func RegisterAppLifecycle(lc fx.Lifecycle, broker *Broker, daemon DaemonController) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			fmt.Println("starting application and initializing background workers...")
			go func() {
				daemon.Start()
			}()

			go func() {
				broker.Start()
			}()

			return nil
		},

		OnStop: func(ctx context.Context) error {
			fmt.Println("Shutting down background workers gracefully...")
			// TBD: Close your incoming channels here to safely drain loops
			return nil
		},
	})
}
