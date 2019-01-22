package clusterstatus

import (
	"strconv"
	"time"

	"github.com/containers-ai/karina/datahub/pkg/utils"
	influxdb_client "github.com/influxdata/influxdb/client/v2"
	"github.com/pkg/errors"
)

type containerTag = string
type containerField = string

const (
	// ContainerTime is the time that container information is saved to the measurement
	ContainerTime containerTag = "time"
	// ContainerNamespace is the container namespace
	ContainerNamespace containerTag = "namespace"
	// ContainerPodName is the name of pod that container is running in
	ContainerPodName containerTag = "pod_name"
	// ContainerScalerNamespace is the namespace of Scaler that container belongs to
	ContainerScalerNamespace containerTag = "scaler_namespace"
	// ContainerScalerName is the name of Scaler that container belongs to
	ContainerScalerName containerTag = "scaler_name"
	// ContainerNodeName is the name of node that container is running in
	ContainerNodeName containerTag = "node_name"
	// ContainerName is the container name
	ContainerName containerTag = "name"

	// ContainerResourceRequestCPU is CPU request of the container
	ContainerResourceRequestCPU containerField = "resource_request_cpu"
	// ContainerResourceRequestMemory is memory request of the container
	ContainerResourceRequestMemory containerField = "resource_request_memroy"
	// ContainerResourceLimitCPU is CPU limit of the container
	ContainerResourceLimitCPU containerField = "resource_limit_cpu"
	// ContainerResourceLimitMemory is memory limit of the container
	ContainerResourceLimitMemory containerField = "resource_limit_memory"
	// ContainerIsPredicted is the state that container is predicted or not
	ContainerIsPredicted containerField = "is_predicted"
	// ContainerIsDeleted is the state that container is deleted or not
	ContainerIsDeleted containerField = "is_deleted"
	// ContainerPolicy is the prediction policy of container
	ContainerPolicy containerField = "policy"
	// ContainerPodCreateTime is the creation time of pod
	ContainerPodCreateTime containerField = "pod_create_time"
)

var (
	// ContainerTags is the list of container measurement tags
	ContainerTags = []containerTag{
		ContainerTime, ContainerNamespace, ContainerPodName,
		ContainerScalerNamespace, ContainerScalerName,
		ContainerNodeName, ContainerName,
	}
	// ContainerFields is the list of container measurement fields
	ContainerFields = []containerField{
		ContainerResourceRequestCPU, ContainerResourceRequestMemory,
		ContainerResourceLimitCPU, ContainerResourceLimitMemory,
		ContainerIsPredicted, ContainerIsDeleted, ContainerPolicy, ContainerPodCreateTime,
	}
)

// ContainerEntity Entity in database
type ContainerEntity struct {
	Time                  time.Time
	Namespace             *string
	PodName               *string
	scalerNamespace       *string
	scalerName            *string
	NodeName              *string
	Name                  *string
	ResourceRequestCPU    *float64
	ResourceRequestMemory *int64
	ResourceLimitCPU      *float64
	ResourceLimitMemory   *int64
	IsPredicted           *bool
	IsDeleted             *bool
	Policy                *string
}

// NewContainerEntityFromMap Build entity from map
func NewContainerEntityFromMap(data map[string]string) ContainerEntity {

	// TODO: log error
	tempTimestamp, _ := utils.ParseTime(data[ContainerTime])

	entity := ContainerEntity{
		Time: tempTimestamp,
	}

	if namespace, exist := data[ContainerNamespace]; exist {
		entity.Namespace = &namespace
	}
	if podName, exist := data[ContainerPodName]; exist {
		entity.PodName = &podName
	}
	if scalerNamespace, exist := data[ContainerScalerNamespace]; exist {
		entity.scalerNamespace = &scalerNamespace
	}
	if scalerName, exist := data[ContainerScalerName]; exist {
		entity.scalerName = &scalerName
	}
	if nodeName, exist := data[ContainerNodeName]; exist {
		entity.NodeName = &nodeName
	}
	if name, exist := data[ContainerName]; exist {
		entity.Name = &name
	}
	if resourceRequestCPU, exist := data[ContainerResourceRequestCPU]; exist {
		value, _ := strconv.ParseFloat(resourceRequestCPU, 64)
		entity.ResourceRequestCPU = &value
	}
	if resourceRequestMemory, exist := data[ContainerResourceRequestMemory]; exist {
		value, _ := strconv.ParseInt(resourceRequestMemory, 10, 64)
		entity.ResourceRequestMemory = &value
	}
	if resourceLimitCPU, exist := data[ContainerResourceLimitCPU]; exist {
		value, _ := strconv.ParseFloat(resourceLimitCPU, 64)
		entity.ResourceLimitCPU = &value
	}
	if resourceLimitMemory, exist := data[ContainerResourceLimitMemory]; exist {
		value, _ := strconv.ParseInt(resourceLimitMemory, 10, 64)
		entity.ResourceLimitMemory = &value
	}
	if isPredicted, exist := data[ContainerIsPredicted]; exist {
		value, _ := strconv.ParseBool(isPredicted)
		entity.IsPredicted = &value
	}
	if isDeleted, exist := data[ContainerIsDeleted]; exist {
		value, _ := strconv.ParseBool(isDeleted)
		entity.IsDeleted = &value
	}
	if policy, exist := data[ContainerPolicy]; exist {
		entity.Policy = &policy
	}

	return entity
}

func (e ContainerEntity) InfluxDBPoint(measurementName string) (*influxdb_client.Point, error) {

	tags := map[string]string{}
	if e.Namespace != nil {
		tags[ContainerNamespace] = *e.Namespace
	}
	if e.PodName != nil {
		tags[ContainerPodName] = *e.PodName
	}
	if e.NodeName != nil {
		tags[ContainerNodeName] = *e.NodeName
	}
	if e.Name != nil {
		tags[ContainerName] = *e.Name
	}
	if e.scalerNamespace != nil {
		tags[ContainerScalerNamespace] = *e.scalerNamespace
	}
	if e.scalerName != nil {
		tags[ContainerScalerName] = *e.scalerName
	}

	fields := map[string]interface{}{}
	if e.IsDeleted != nil {
		fields[ContainerIsDeleted] = *e.IsDeleted
	}
	if e.IsPredicted != nil {
		fields[ContainerIsPredicted] = *e.IsPredicted
	}
	if e.Policy != nil {
		fields[ContainerPolicy] = *e.Policy
	}
	if e.ResourceRequestCPU != nil {
		fields[ContainerResourceRequestCPU] = *e.ResourceRequestCPU
	}
	if e.ResourceRequestMemory != nil {
		fields[ContainerResourceRequestMemory] = *e.ResourceRequestMemory
	}
	if e.ResourceLimitCPU != nil {
		fields[ContainerResourceLimitCPU] = *e.ResourceLimitCPU
	}
	if e.ResourceLimitMemory != nil {
		fields[ContainerResourceLimitMemory] = *e.ResourceLimitMemory
	}

	point, err := influxdb_client.NewPoint(measurementName, tags, fields, e.Time)
	if err != nil {
		return nil, errors.Wrapf(err, "new influxdb point from container entity failed: %s", err.Error())
	}

	return point, nil
}
