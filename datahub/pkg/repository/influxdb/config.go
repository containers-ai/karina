package influxdb

import (
	"errors"
	"net/url"

	"github.com/containers-ai/karina/pkg"
)

const (
	defaultAddress  = "https://influxdb." + pkg.ProjectCodeName + ".svc.cluster.local:8086"
	defaultUsername = "datahub"
	defaultPassword = "datahub"
)

type Config struct {
	Address  string `mapstructure:"address"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

func NewDefaultConfig() Config {
	var config = Config{
		Address:  defaultAddress,
		Username: defaultUsername,
		Password: defaultPassword,
	}
	return config
}

func (c *Config) Validate() error {
	_, err := url.Parse(c.Address)
	if err != nil {
		return errors.New("InfluxDB config validate failed: " + err.Error())
	}

	return nil
}
