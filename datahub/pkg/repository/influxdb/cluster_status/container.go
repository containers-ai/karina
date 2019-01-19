package clusterstatus

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	datahub_metric_v1alpha2 "github.com/containers-ai/api/datahub/metric/v1alpha2"
	datahub_resource_metadata_v1alpha2 "github.com/containers-ai/api/datahub/resource/metadata/v1alpha2"
	datahub_resource_v1alpha2 "github.com/containers-ai/api/datahub/resource/v1alpha2"
	cluster_status_entity "github.com/containers-ai/karina/datahub/pkg/entity/influxdb/cluster_status"
	"github.com/containers-ai/karina/datahub/pkg/repository/influxdb"
	"github.com/containers-ai/karina/datahub/pkg/utils"
	"github.com/containers-ai/karina/pkg/utils/log"
	proto_timestmap "github.com/golang/protobuf/ptypes/timestamp"
	influxdb_client "github.com/influxdata/influxdb/client/v2"
)

var (
	containerScope = log.RegisterScope("cluster_status_db_container_measurement", "cluster_status DB container measurement", 0)
)

// ContainerRepository is used to operate node measurement of cluster_status database
type ContainerRepository struct {
	influxDB *influxdb.InfluxDBRepository
}

// IsTag checks the column is tag or not
func (containerRepository *ContainerRepository) IsTag(column string) bool {
	for _, tag := range cluster_status_entity.ContainerTags {
		if column == string(tag) {
			return true
		}
	}
	return false
}

// NewContainerRepository creates the ContainerRepository instance
func NewContainerRepository(influxDBCfg *influxdb.Config) *ContainerRepository {
	return &ContainerRepository{
		influxDB: &influxdb.InfluxDBRepository{
			Address:  influxDBCfg.Address,
			Username: influxDBCfg.Username,
			Password: influxDBCfg.Password,
		},
	}
}

// ListPredictedContainers list predicted containers
func (containerRepository *ContainerRepository) ListPredictedContainers(scalerNS, scalerName string, isPredicted bool) ([]*datahub_resource_v1alpha2.Pod, error) {
	podList := []*datahub_resource_v1alpha2.Pod{}
	// SELECT * FROM container WHERE is_deleted=false AND is_predicted=true GROUP BY namespace,pod_name,karina_scaler_namespace,karina_scaler_name
	whereStr := fmt.Sprintf("WHERE \"%s\"=%t AND \"%s\"=%s", string(cluster_status_entity.ContainerIsPredicted), isPredicted,
		string(cluster_status_entity.ContainerIsDeleted), "false")
	if whereStr != "" {
		if scalerNS != "" && scalerName != "" {
			whereStr = fmt.Sprintf("%s AND \"%s\"='%s' AND \"%s\"='%s'", whereStr,
				string(cluster_status_entity.ContainerScalerNamespace), scalerNS,
				string(cluster_status_entity.ContainerScalerName), scalerName)
		} else if scalerNS != "" && scalerName == "" {
			whereStr = fmt.Sprintf("%s AND \"%s\"='%s'", whereStr,
				string(cluster_status_entity.ContainerScalerNamespace), scalerNS)
		} else if scalerNS == "" && scalerName != "" {
			whereStr = fmt.Sprintf("%s AND \"%s\"='%s'", whereStr,
				string(cluster_status_entity.ContainerScalerName), scalerName)
		}
	}
	cmd := fmt.Sprintf("SELECT * FROM %s %s GROUP BY \"%s\",\"%s\",\"%s\",\"%s\"",
		string(Container), whereStr,
		string(cluster_status_entity.ContainerNamespace), string(cluster_status_entity.ContainerPodName),
		string(cluster_status_entity.ContainerScalerNamespace), string(cluster_status_entity.ContainerScalerName))
	containerScope.Debug("List Predicted Containers CMD: " + cmd)
	if results, err := containerRepository.influxDB.QueryDB(cmd, string(influxdb.ClusterStatus)); err == nil {
		for _, result := range results {
			for _, ser := range result.Series {
				podName := ser.Tags[string(cluster_status_entity.ContainerPodName)]
				contanerNS := ser.Tags[string(cluster_status_entity.ContainerNamespace)]
				scalerNS := ser.Tags[string(cluster_status_entity.ContainerScalerNamespace)]
				scalerName := ser.Tags[string(cluster_status_entity.ContainerScalerName)]
				thePod := &datahub_resource_v1alpha2.Pod{
					NamespacedName: &datahub_resource_metadata_v1alpha2.NamespacedName{
						Name:      podName,
						Namespace: contanerNS,
					},
					Scaler: &datahub_resource_metadata_v1alpha2.NamespacedName{
						Name:      scalerName,
						Namespace: scalerNS,
					},
				}
				if len(ser.Values) > 0 {
					for colIdx, col := range ser.Columns {
						if col == cluster_status_entity.ContainerPodCreateTime && ser.Values[0][colIdx] != nil {
							if createTime, err := ser.Values[0][colIdx].(json.Number).Int64(); err == nil {
								thePod.StartTime = &proto_timestmap.Timestamp{
									Seconds: createTime,
								}
								break
							} else {
								containerScope.Error(err.Error())
							}
						} else if col == cluster_status_entity.ContainerNodeName && ser.Values[0][colIdx] != nil {
							thePod.NodeName = ser.Values[0][colIdx].(string)
						} else if col == cluster_status_entity.ContainerIsPredicted && ser.Values[0][colIdx] != nil {
							thePod.IsPredicted = ser.Values[0][colIdx].(bool)
						}
					}
				}
				podList = append(podList, thePod)
			}
		}
		return podList, nil
	} else {
		return podList, err
	}
}

// CreateContainers add containers information container measurement
func (containerRepository *ContainerRepository) CreateContainers(pods []*datahub_resource_v1alpha2.Pod) error {
	points := []*influxdb_client.Point{}
	for _, pod := range pods {
		podNS := pod.GetNamespacedName().GetNamespace()
		podName := pod.GetNamespacedName().GetName()
		containers := pod.GetContainers()
		isPredictedPod := pod.GetIsPredicted()

		for _, container := range containers {
			tags := map[string]string{
				string(cluster_status_entity.ContainerNamespace): podNS,
				string(cluster_status_entity.ContainerPodName):   podName,
				string(cluster_status_entity.ContainerNodeName):  pod.GetNodeName(),
				string(cluster_status_entity.ContainerName):      container.GetName(),
			}
			fields := map[string]interface{}{
				string(cluster_status_entity.ContainerIsDeleted):     false,
				string(cluster_status_entity.ContainerIsPredicted):   isPredictedPod,
				string(cluster_status_entity.ContainerPolicy):        pod.GetPolicy(),
				string(cluster_status_entity.ContainerPodCreateTime): pod.StartTime.GetSeconds(),
			}
			if isPredictedPod {
				tags[string(cluster_status_entity.ContainerScalerNamespace)] = pod.GetScaler().GetNamespace()
				tags[string(cluster_status_entity.ContainerScalerName)] = pod.GetScaler().GetName()
			}
			for metricKey, metricData := range container.GetLimitResource() {
				if data := metricData.GetData(); len(data) == 1 {

					switch metricKey {
					case int32(datahub_metric_v1alpha2.MetricType_CPU_USAGE_SECONDS_PERCENTAGE):
						if floatVal, err := utils.StringToFloat64(data[0].NumValue); err == nil {
							fields[string(cluster_status_entity.ContainerResourceLimitCPU)] = floatVal
						}
					case int32(datahub_metric_v1alpha2.MetricType_MEMORY_USAGE_BYTES):
						if intVal, err := utils.StringToInt64(data[0].NumValue); err == nil {
							fields[string(cluster_status_entity.ContainerResourceLimitMemory)] = intVal
						}
					}
				}
			}
			for metricKey, metricData := range container.GetRequestResource() {
				if data := metricData.GetData(); len(data) == 1 {

					switch metricKey {
					case int32(datahub_metric_v1alpha2.MetricType_CPU_USAGE_SECONDS_PERCENTAGE):
						if floatVal, err := utils.StringToFloat64(data[0].NumValue); err == nil {
							fields[string(cluster_status_entity.ContainerResourceRequestCPU)] = floatVal
						}
					case int32(datahub_metric_v1alpha2.MetricType_MEMORY_USAGE_BYTES):
						if intVal, err := utils.StringToInt64(data[0].NumValue); err == nil {
							fields[string(cluster_status_entity.ContainerResourceRequestMemory)] = intVal
						}
					}
				}
			}

			if pt, err := influxdb_client.NewPoint(string(Container), tags, fields, influxdb.ZeroTime); err == nil {
				points = append(points, pt)
			} else {
				scope.Error(err.Error())
			}
		}
	}
	containerRepository.influxDB.WritePoints(points, influxdb_client.BatchPointsConfig{
		Database: string(influxdb.ClusterStatus),
	})
	return nil
}

// UpdateContainers updates containers' fields into container measurement
func (containerRepository *ContainerRepository) UpdateContainers(containerEntities []*cluster_status_entity.ContainerEntity) error {

	var (
		err            error
		pointsToUpdate = make([]*influxdb_client.Point, 0)
	)

	if err != nil {
		return errors.New("update containers failed: " + err.Error())
	}
	for _, containerEntity := range containerEntities {
		point, err := (*containerEntity).InfluxDBPoint(string(Container))
		if err != nil {
			return errors.New("update containers failed: " + err.Error())
		}

		pointsToUpdate = append(pointsToUpdate, point)
	}

	containerRepository.influxDB.WritePoints(pointsToUpdate, influxdb_client.BatchPointsConfig{
		Database: string(influxdb.ClusterStatus),
	})
	return nil
}

// DeleteContainers set containers' field is_deleted to true into container measurement
func (containerRepository *ContainerRepository) DeleteContainers(pods []*datahub_resource_v1alpha2.Pod) error {

	var (
		err error

		containersEntityBeforeDelete = make([]*cluster_status_entity.ContainerEntity, 0)

		pointsToDelete = make([]*influxdb_client.Point, 0)
	)

	containersEntityBeforeDelete, err = containerRepository.ListPodsContainers(pods)
	if err != nil {
		return errors.New("delete containers failed: " + err.Error())
	}
	for _, containerEntity := range containersEntityBeforeDelete {
		entity := *containerEntity

		trueValue := true
		entity.IsDeleted = &trueValue
		point, err := entity.InfluxDBPoint(string(Container))
		if err != nil {
			return errors.New("delete containers failed: " + err.Error())
		}

		pointsToDelete = append(pointsToDelete, point)
	}

	containerRepository.influxDB.WritePoints(pointsToDelete, influxdb_client.BatchPointsConfig{
		Database: string(influxdb.ClusterStatus),
	})
	return nil
}

// ListPodsContainers list containers information container measurement
func (containerRepository *ContainerRepository) ListPodsContainers(pods []*datahub_resource_v1alpha2.Pod) ([]*cluster_status_entity.ContainerEntity, error) {

	var (
		cmd                 = ""
		cmdSelectString     = ""
		cmdTagsFilterString = ""
		containerEntities   = make([]*cluster_status_entity.ContainerEntity, 0)
	)

	if len(pods) == 0 {
		return containerEntities, nil
	}

	cmdSelectString = fmt.Sprintf(`select * from "%s" `, Container)
	for _, pod := range pods {

		var (
			namespace = ""
			podName   = ""
		)

		if pod.GetNamespacedName() != nil {
			namespace = pod.GetNamespacedName().GetNamespace()
			podName = pod.GetNamespacedName().GetName()
		}

		cmdTagsFilterString += fmt.Sprintf(`("%s" = '%s' and "%s" = '%s') or `,
			cluster_status_entity.ContainerNamespace, namespace,
			cluster_status_entity.ContainerPodName, podName,
		)
	}
	cmdTagsFilterString = strings.TrimSuffix(cmdTagsFilterString, "or ")

	cmd = fmt.Sprintf("%s where %s", cmdSelectString, cmdTagsFilterString)
	containerScope.Debug("List pod containers CMD: " + cmd)
	results, err := containerRepository.influxDB.QueryDB(cmd, string(influxdb.ClusterStatus))
	if err != nil {
		return containerEntities, errors.New("list containers' entity failed: " + err.Error())
	}

	rows := influxdb.PackMap(results)
	for _, row := range rows {
		for _, data := range row.Data {
			entity := cluster_status_entity.NewContainerEntityFromMap(data)
			containerEntities = append(containerEntities, &entity)
		}
	}

	return containerEntities, nil
}
