package clusterstatus

import (
	"github.com/containers-ai/karina/datahub/pkg/repository/influxdb"
)

const (
	// Node is node measurement
	Node influxdb.Measurement = "node"
	// Container is container measurement
	Container influxdb.Measurement = "container"
)
