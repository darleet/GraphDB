package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

type Options struct {
	ConfigPath string
}

type RootCommand struct {
	*cobra.Command
	Options Options
}

func Init(name string) *RootCommand {
	cmd := &RootCommand{
		Command: &cobra.Command{
			Use: name,
		},
	}
	cmd.initFlags()

	return cmd
}

func (c *RootCommand) Execute(ctx context.Context) error {
	return c.ExecuteContext(ctx)
}

func (c *RootCommand) MustExecute(ctx context.Context) {
	if err := c.Execute(ctx); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "app failed: %v\n", err)
		os.Exit(1)
	}
}
