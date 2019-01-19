package clusterstatus

import (
	"strconv"
	"time"

	"github.com/containers-ai/karina/datahub/pkg/utils"
	influxdb_client "github.com/influxdata/influxdb/client/v2"
)

type nodeField = string
type nodeTag = string

const (
	// NodeTime is the time node information is inserted to databse
	NodeTime nodeTag = "time"
	// NodeName is the name of node
	NodeName nodeTag = "name"

	// NodeGroup is node group name
	NodeGroup nodeField = "group"
	// NodeInCluster represents node in cluster
	NodeInCluster nodeField = "in_cluster"
	// NodeIsPredicted represents node in prediction
	NodeIsPredicted nodeField = "is_predicted"
)

var (
	// NodeTags list tags of node measurement
	NodeTags = []nodeTag{NodeTime, NodeName}
	// NodeFields list fields of node measurement
	NodeFields = []nodeField{NodeGroup, NodeInCluster, NodeIsPredicted}
)

// NodeEntity Entity in database
type NodeEntity struct {
	Time time.Time
	Name *string

	Group       *string
	InCluster   *bool
	IsPredicted *bool
}

// NewNodeEntityFromMap Build entity from map
func NewNodeEntityFromMap(data map[string]string) NodeEntity {

	// TODO: log error
	tempTimestamp, _ := utils.ParseTime(data[NodeTime])

	entity := NodeEntity{
		Time: tempTimestamp,
	}

	if name, exist := data[NodeName]; exist {
		entity.Name = &name
	}
	if group, exist := data[NodeGroup]; exist {
		entity.Group = &group
	}

	if isPredicted, exist := data[NodeIsPredicted]; exist {
		value, _ := strconv.ParseBool(isPredicted)
		entity.IsPredicted = &value
	}
	if inCluster, exist := data[NodeInCluster]; exist {
		value, _ := strconv.ParseBool(inCluster)
		entity.InCluster = &value
	}

	return entity
}

func (e NodeEntity) InfluxDBPoint(measurementName string) (*influxdb_client.Point, error) {

	tags := map[string]string{}
	if e.Name != nil {
		tags[NodeName] = *e.Name
	}

	fields := map[string]interface{}{}
	if e.Group != nil {
		fields[NodeGroup] = *e.Group
	}
	if e.IsPredicted != nil {
		fields[NodeIsPredicted] = *e.IsPredicted
	}
	if e.InCluster != nil {
		fields[NodeInCluster] = *e.InCluster
	}

	return influxdb_client.NewPoint(measurementName, tags, fields, e.Time)
}
