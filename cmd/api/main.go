package main

import (
	"context"
	"nrt-agentic-streamer-api/internal/platform"

	"github.com/cloudwego/hertz/pkg/app/server"
	"go.uber.org/fx"
)

func main() {
	// Upon Run - The following happens:
	// net/http -> SSE:
	// - each client gets a channel from the broker (need channel broker) struct with channels and goroutine
	// goroutine keeps recieving inputs from broker through a dedicated channel and replies on that channel
	// Request -> Broker -> Dispatch to input channel -> goroutine response -> Broker -> output channel
	// MemTable for keeping Node statuses. Node keys: A:current_status_id (incremental from 000000), B, C, D etc

	uberfx := fx.New(
		//add the platform modules
		platform.Module,

		// create a hertz server and add lifecycle events to it and provide it
		fx.Provide(func(lc fx.Lifecycle) *server.Hertz {
			h := server.Default(server.WithHostPorts(":80"))

			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go h.Spin()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					return h.Shutdown(ctx)
				},
			})

			return h
		}),

		fx.Invoke(func(*server.Hertz) {}),
	)

	uberfx.Run()
}
