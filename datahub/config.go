package datahub

import (
	"errors"

	influxdb_repository "github.com/containers-ai/karina/datahub/pkg/repository/influxdb"
	"github.com/containers-ai/karina/datahub/pkg/repository/prometheus"
	"github.com/containers-ai/karina/pkg/utils/log"
)

const (
	defaultBindAddress = ":50050"
)

type Config struct {
	BindAddress string                      `mapstructure:"bind-address"`
	Prometheus  *prometheus.Config          `mapstructure:"prometheus"`
	InfluxDB    *influxdb_repository.Config `mapstructure:"influxdb"`
	Log         *log.Config                 `mapstructure:"log"`
}

func NewDefaultConfig() Config {

	var (
		defaultlogConfig        = log.NewDefaultConfig()
		defaultPrometheusConfig = prometheus.NewDefaultConfig()
		defaultInfluxDBConfig   = influxdb_repository.NewDefaultConfig()
		config                  = Config{
			BindAddress: defaultBindAddress,
			Prometheus:  &defaultPrometheusConfig,
			InfluxDB:    &defaultInfluxDBConfig,
			Log:         &defaultlogConfig,
		}
	)

	return config
}

func (c *Config) Validate() error {

	var err error

	err = c.Prometheus.Validate()
	if err != nil {
		return errors.New("gRPC config validate failed: " + err.Error())
	}

	return nil
}
