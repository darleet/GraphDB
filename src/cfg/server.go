package cfg

import (
	"errors"
	"fmt"

	"github.com/spf13/viper"
)

type ServerConfig struct {
	Environment Environment `mapstructure:"ENVIRONMENT"`

	ServerHost string `mapstructure:"SERVER_HOST"`
	ServerPort int    `mapstructure:"SERVER_PORT"`
}

func LoadConfig(path string) (ServerConfig, error) {
	viper.AddConfigPath(path)
	viper.SetConfigType("env")
	viper.SetConfigName(".env")
	viper.SetEnvPrefix("GRAPHDB")
	viper.AutomaticEnv()

	viper.SetOptions(viper.ExperimentalBindStruct())

	viper.SetDefault("ENVIRONMENT", DefaultEnv)
	viper.SetDefault("SERVER_HOST", "localhost")
	viper.SetDefault("SERVER_PORT", 8080)

	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("config file not found, using env vars")
	}

	var cfg ServerConfig

	err = viper.Unmarshal(&cfg)
	if err != nil {
		return ServerConfig{}, fmt.Errorf("viper unmarshaling config: %w", err)
	}

	err = cfg.Environment.Validate()
	if err != nil {
		return ServerConfig{}, fmt.Errorf("environment validation: %w", err)
	}

	return cfg, nil
}

const (
	EnvDev  Environment = "dev"
	EnvProd Environment = "prod"

	DefaultEnv = EnvDev
)

type Environment string

func (e Environment) Validate() error {
	if e != EnvDev && e != EnvProd {
		return errors.New("environment must be either dev or prod")
	}

	return nil
}
