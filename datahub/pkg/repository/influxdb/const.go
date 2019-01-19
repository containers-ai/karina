package influxdb

import (
	"github.com/containers-ai/karina/pkg"
)

const (
	// Time is InfluxDB time tag
	Time string = "time"

	// ClusterStatus is cluster_status database
	ClusterStatus Database = pkg.ProjectCodeName + "_cluster_status"
	// Prediction is prediction database
	Prediction Database = pkg.ProjectCodeName + "_prediction"
	// Recommendation is recommendation database
	Recommendation Database = pkg.ProjectCodeName + "_recommendation"
	// Score is score database
	Score Database = pkg.ProjectCodeName + "_score"
)
