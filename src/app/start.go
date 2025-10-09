package app

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/Blackdeer1524/GraphDB/src"
	"github.com/Blackdeer1524/GraphDB/src/cfg"
	"github.com/Blackdeer1524/GraphDB/src/delivery"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
)

const CloseTimeout = 15 * time.Second

type APIEntrypoint struct {
	ConfigPath string

	s   *delivery.Server
	log src.Logger
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

	e.log = log

	e.s = delivery.NewServer(log, e.cfg)

	return nil
}

func (e *APIEntrypoint) Run(ctx context.Context) error {
	return e.s.Run()
}

func (e *APIEntrypoint) Close() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), CloseTimeout)
	defer cancel()

	if e.s != nil {
		err = e.s.Close(ctx)
	}

	if e.log != nil {
		if err != nil {
			e.log.Error("failed to close server", zap.Error(err))
		}

		logErr := e.log.Sync()
		if logErr != nil && err != nil {
			err = fmt.Errorf("%w, %w", err, logErr)
		} else if logErr != nil {
			err = logErr
		}
	}

	return
}
