package app

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/Blackdeer1524/GraphDB/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src"
	"github.com/Blackdeer1524/GraphDB/src/cfg"
	"github.com/Blackdeer1524/GraphDB/src/delivery"
)

const CloseTimeout = 15 * time.Second

type APIEntrypoint struct {
	ConfigPath string

	s   *delivery.Server
	cfg cfg.ServerConfig
}

func (e *APIEntrypoint) Init(ctx context.Context) error {
	config, err := cfg.LoadConfig(e.ConfigPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	e.cfg = config

	var log src.Logger
	if e.cfg.Environment == cfg.EnvDev {
		log = utils.Must(zap.NewDevelopment()).Sugar()
	} else {
		log = utils.Must(zap.NewProduction()).Sugar()
	}

	e.s = delivery.NewServer(log, e.cfg)

	return nil
}

func (e *APIEntrypoint) Run(ctx context.Context) error {
	return e.s.Run()
}

func (e *APIEntrypoint) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), CloseTimeout)
	defer cancel()

	if e.s != nil {
		return e.s.Close(ctx)
	}

	return nil
}
