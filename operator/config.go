package operator

import (
	datahub "github.com/containers-ai/karina/operator/datahub"
	"github.com/containers-ai/karina/pkg/utils/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// Config defines configurations
type Config struct {
	MetricsAddress string          `mapstructure:"metrics-addr"`
	Log            *log.Config     `mapstructure:"log"`
	Datahub        *datahub.Config `mapstructure:"datahub"`
	Manager        manager.Manager
}

// NewDefaultConfig returns Config objecdt
func NewDefaultConfig() Config {

	defaultLogConfig := log.NewDefaultConfig()
	defaultDatahubConfig := datahub.NewDefaultConfig()

	c := Config{
		MetricsAddress: ":8080",
		Log:            &defaultLogConfig,
		Datahub:        &defaultDatahubConfig,
	}

	return c
}

func (c Config) Validate() error {
	return nil
}

func (c *Config) SetManager(manager manager.Manager) {

	if c == nil {
		c = &Config{}
	}

	c.Manager = manager
}
