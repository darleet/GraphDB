package app

import (
	"context"

	"github.com/Blackdeer1524/GraphDB/src/cli"
)

var rootCmd = cli.Init("server")

func MustExecute(ctx context.Context) {
	initStart()
	rootCmd.MustExecute(ctx)
}
