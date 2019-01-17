package datahub

import (
	"errors"
	"net/url"
)

type Config struct {
	Address string `mapstructure:"address"`
}

func NewDefaultConfig() Config {

	c := Config{
		Address: "datahub.federatorai.svc.cluster.local:50050",
	}
	return c
}

func (c *Config) Validate() error {

	var err error

	_, err = url.Parse(c.Address)
	if err != nil {
		return errors.New("datahub config validate failed: " + err.Error())
	}

	return nil
}
