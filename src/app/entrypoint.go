package app

import (
	"context"
	"fmt"
	"io"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"
)

type Entrypoint interface {
	io.Closer
	Init(ctx context.Context) error
	Run(ctx context.Context) error
}

func Run(ctx context.Context, e Entrypoint) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := e.Init(ctx); err != nil {
		return fmt.Errorf("entrypoint init error: %w", err)
	}

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return e.Run(ctx)
	})

	// graceful shutdown
	eg.Go(func() error {
		<-ctx.Done()
		fmt.Printf("gracefully shutting down app...\n")

		return e.Close()
	})

	if err := eg.Wait(); err != nil {
		fmt.Printf("app was shut down, reason: %s\n", err.Error())
	}

	return nil
}
