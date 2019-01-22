package recommendation

import (
	"encoding/json"
	"fmt"
	"time"

	datahub_metric_v1alpha2 "github.com/containers-ai/api/datahub/metric/v1alpha2"
	datahub_recommendation_v1alpha2 "github.com/containers-ai/api/datahub/recommendation/v1alpha2"
	datahub_resource_metadata_v1alpha2 "github.com/containers-ai/api/datahub/resource/metadata/v1alpha2"
	datahub_v1alpha2 "github.com/containers-ai/api/datahub/v1alpha2"
	recommendation_entity "github.com/containers-ai/karina/datahub/pkg/entity/influxdb/recommendation"
	"github.com/containers-ai/karina/datahub/pkg/repository/influxdb"
	"github.com/containers-ai/karina/datahub/pkg/utils"
	"github.com/containers-ai/karina/pkg/utils/log"
	"github.com/golang/protobuf/ptypes/timestamp"
	influxdb_client "github.com/influxdata/influxdb/client/v2"
	"github.com/pkg/errors"
)

var (
	containerScope = log.RegisterScope("recommendation_db_container_measurement", "recommendation DB container measurement", 0)
)

// ContainerRepository is used to operate node measurement of recommendation database
type ContainerRepository struct {
	influxDB *influxdb.InfluxDBRepository
}

// IsTag checks the column is tag or not
func (containerRepository *ContainerRepository) IsTag(column string) bool {
	for _, tag := range recommendation_entity.ContainerTags {
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

// CreateContainerRecommendations add containers information container measurement
func (containerRepository *ContainerRepository) CreateContainerRecommendations(podRecommendations []*datahub_recommendation_v1alpha2.PodRecommendation) error {
	points := []*influxdb_client.Point{}
	for _, podRecommendation := range podRecommendations {
		if podRecommendation.GetApplyRecommendationNow() {
			//TODO
		}

		podNS := podRecommendation.GetNamespacedName().GetNamespace()
		podName := podRecommendation.GetNamespacedName().GetName()
		containerRecommendations := podRecommendation.GetContainerRecommendations()
		for _, containerRecommendation := range containerRecommendations {
			tags := map[string]string{
				string(recommendation_entity.ContainerNamespace): podNS,
				string(recommendation_entity.ContainerPodName):   podName,
				string(recommendation_entity.ContainerName):      containerRecommendation.GetName(),
			}
			fields := map[string]interface{}{
				//TODO
				string(recommendation_entity.ContainerPolicy): "",
			}

			for metricKey, metricData := range containerRecommendation.GetLimitRecommendations() {
				if data := metricData.GetData(); len(data) > 0 {
					for _, datum := range data {
						newFields := map[string]interface{}{}
						for key, value := range fields {
							newFields[key] = value
						}
						if numVal, err := utils.StringToInt64(datum.NumValue); err == nil {
							switch metricKey {
							case int32(datahub_metric_v1alpha2.MetricType_CPU_USAGE_SECONDS_PERCENTAGE):
								newFields[string(recommendation_entity.ContainerResourceLimitCPU)] = numVal
							case int32(datahub_metric_v1alpha2.MetricType_MEMORY_USAGE_BYTES):
								newFields[string(recommendation_entity.ContainerResourceLimitMemory)] = numVal
							}
						}
						if pt, err := influxdb_client.NewPoint(string(Container), tags, newFields, time.Unix(datum.GetTime().GetSeconds(), 0)); err == nil {
							points = append(points, pt)
						} else {
							containerScope.Error(err.Error())
						}
					}
				}
			}
			for metricKey, metricData := range containerRecommendation.GetRequestRecommendations() {
				if data := metricData.GetData(); len(data) > 0 {
					for _, datum := range data {
						newFields := map[string]interface{}{}
						for key, value := range fields {
							newFields[key] = value
						}
						if numVal, err := utils.StringToInt64(datum.NumValue); err == nil {
							switch metricKey {
							case int32(datahub_metric_v1alpha2.MetricType_CPU_USAGE_SECONDS_PERCENTAGE):
								newFields[string(recommendation_entity.ContainerResourceRequestCPU)] = numVal
							case int32(datahub_metric_v1alpha2.MetricType_MEMORY_USAGE_BYTES):
								newFields[string(recommendation_entity.ContainerResourceRequestMemory)] = numVal
							}
						}
						if pt, err := influxdb_client.NewPoint(string(Container), tags, newFields, time.Unix(datum.GetTime().GetSeconds(), 0)); err == nil {
							points = append(points, pt)
						} else {
							containerScope.Error(err.Error())
						}
					}
				}
			}
			for metricKey, metricData := range containerRecommendation.GetInitialLimitRecommendations() {
				if data := metricData.GetData(); len(data) > 0 {
					for _, datum := range data {
						newFields := map[string]interface{}{}
						for key, value := range fields {
							newFields[key] = value
						}
						if numVal, err := utils.StringToInt64(datum.NumValue); err == nil {
							switch metricKey {
							case int32(datahub_metric_v1alpha2.MetricType_CPU_USAGE_SECONDS_PERCENTAGE):
								newFields[string(recommendation_entity.ContainerInitialResourceLimitCPU)] = numVal
							case int32(datahub_metric_v1alpha2.MetricType_MEMORY_USAGE_BYTES):
								newFields[string(recommendation_entity.ContainerInitialResourceLimitMemory)] = numVal
							}
						}
						if pt, err := influxdb_client.NewPoint(string(Container), tags, newFields, time.Unix(datum.GetTime().GetSeconds(), 0)); err == nil {
							points = append(points, pt)
						} else {
							containerScope.Error(err.Error())
						}
					}
				}
			}
			for metricKey, metricData := range containerRecommendation.GetInitialRequestRecommendations() {
				if data := metricData.GetData(); len(data) > 0 {
					for _, datum := range data {
						newFields := map[string]interface{}{}
						for key, value := range fields {
							newFields[key] = value
						}
						if numVal, err := utils.StringToInt64(datum.NumValue); err == nil {
							switch metricKey {
							case int32(datahub_metric_v1alpha2.MetricType_CPU_USAGE_SECONDS_PERCENTAGE):
								newFields[string(recommendation_entity.ContainerInitialResourceRequestCPU)] = numVal
							case int32(datahub_metric_v1alpha2.MetricType_MEMORY_USAGE_BYTES):
								newFields[string(recommendation_entity.ContainerInitialResourceRequestMemory)] = numVal
							}
						}
						if pt, err := influxdb_client.NewPoint(string(Container), tags, newFields, time.Unix(datum.GetTime().GetSeconds(), 0)); err == nil {
							points = append(points, pt)
						} else {
							containerScope.Error(err.Error())
						}
					}
				}
			}
		}
	}
	err := containerRepository.influxDB.WritePoints(points, influxdb_client.BatchPointsConfig{
		Database: string(influxdb.Recommendation),
	})
	if err != nil {
		return errors.Wrapf(err, "create container recommendations failed: %s", err.Error())
	}

	return nil
}

// ListContainerRecommendations list container recommendations
func (containerRepository *ContainerRepository) ListContainerRecommendations(podNamespacedName *datahub_resource_metadata_v1alpha2.NamespacedName, queryCondition *datahub_v1alpha2.QueryCondition) ([]*datahub_recommendation_v1alpha2.PodRecommendation, error) {
	podRecommendations := []*datahub_recommendation_v1alpha2.PodRecommendation{}
	reqPodNS := podNamespacedName.GetNamespace()
	reqPodName := podNamespacedName.GetName()
	var (
		reqStartTime *timestamp.Timestamp
		reqEndTime   *timestamp.Timestamp
	)
	timeRange := queryCondition.GetTimeRange()
	if timeRange != nil {
		reqStartTime = timeRange.GetStartTime()
		reqEndTime = timeRange.GetEndTime()
	}

	whereStr := ""
	if reqPodNS != "" && reqPodName == "" {
		whereStr = fmt.Sprintf("WHERE \"%s\"='%s'", string(recommendation_entity.ContainerNamespace), reqPodNS)
	} else if reqPodNS == "" && reqPodName != "" {
		whereStr = fmt.Sprintf("WHERE \"%s\"='%s'", string(recommendation_entity.ContainerPodName), reqPodName)
	} else if reqPodNS != "" && reqPodName != "" {
		whereStr = fmt.Sprintf("WHERE \"%s\"='%s' AND \"%s\"='%s'", string(recommendation_entity.ContainerNamespace), reqPodNS, string(recommendation_entity.ContainerPodName), reqPodName)
	}

	timeConditionStr := ""
	if reqStartTime != nil && reqEndTime != nil {
		timeConditionStr = fmt.Sprintf("time >= %v AND time <= %v", utils.TimeStampToNanoSecond(reqStartTime), utils.TimeStampToNanoSecond(reqEndTime))
	} else if reqStartTime != nil && reqEndTime == nil {
		timeConditionStr = fmt.Sprintf("time >= %v", utils.TimeStampToNanoSecond(reqStartTime))
	} else if reqStartTime == nil && reqEndTime != nil {
		timeConditionStr = fmt.Sprintf("time <= %v", utils.TimeStampToNanoSecond(reqEndTime))
	}

	if whereStr == "" && timeConditionStr != "" {
		whereStr = fmt.Sprintf("WHERE %s", timeConditionStr)
	} else if whereStr != "" && timeConditionStr != "" {
		whereStr = fmt.Sprintf("%s AND %s", whereStr, timeConditionStr)
	}

	orderStr := containerRepository.buildOrderClause(queryCondition)
	limitStr := containerRepository.buildLimitClause(queryCondition)

	cmd := fmt.Sprintf("SELECT * FROM %s %s GROUP BY \"%s\",\"%s\",\"%s\" %s %s",
		string(Container), whereStr, recommendation_entity.ContainerName,
		recommendation_entity.ContainerNamespace, recommendation_entity.ContainerPodName, orderStr, limitStr)
	containerScope.Infof(fmt.Sprintf("ListContainerRecommendations: %s", cmd))
	if results, err := containerRepository.influxDB.QueryDB(cmd, string(influxdb.Recommendation)); err == nil {
		for _, result := range results {
			//container recommendation time series data
			for _, ser := range result.Series {
				podName := ser.Tags[string(recommendation_entity.ContainerPodName)]
				podNS := ser.Tags[string(recommendation_entity.ContainerNamespace)]
				containerRecommendation := &datahub_recommendation_v1alpha2.ContainerRecommendation{
					Name:                          ser.Tags[string(recommendation_entity.ContainerName)],
					InitialLimitRecommendations:   map[int32]*datahub_metric_v1alpha2.MetricData{},
					InitialRequestRecommendations: map[int32]*datahub_metric_v1alpha2.MetricData{},
					LimitRecommendations:          map[int32]*datahub_metric_v1alpha2.MetricData{},
					RequestRecommendations:        map[int32]*datahub_metric_v1alpha2.MetricData{},
				}
				initialResourceLimitCPUData := []*datahub_metric_v1alpha2.Sample{}
				initialResourceRequestCPUData := []*datahub_metric_v1alpha2.Sample{}
				resourceLimitCPUData := []*datahub_metric_v1alpha2.Sample{}
				resourceRequestCPUData := []*datahub_metric_v1alpha2.Sample{}
				initialResourceLimitMemoryData := []*datahub_metric_v1alpha2.Sample{}
				initialResourceRequestMemoryData := []*datahub_metric_v1alpha2.Sample{}
				resourceLimitMemoryData := []*datahub_metric_v1alpha2.Sample{}
				resourceRequestMemoryData := []*datahub_metric_v1alpha2.Sample{}
				for _, val := range ser.Values {
					timeColIdx := utils.GetTimeIdxFromColumns(ser.Columns)
					timeObj, _ := utils.ParseTime(val[timeColIdx].(string))
					for columnIdx, column := range ser.Columns {
						if val[columnIdx] == nil {
							continue
						}

						if column == string(recommendation_entity.ContainerInitialResourceLimitCPU) {
							colVal := val[columnIdx].(json.Number).String()
							sampleObj := utils.GetSampleInstance(&timeObj, colVal)
							initialResourceLimitCPUData = append(initialResourceLimitCPUData, sampleObj)
						} else if column == string(recommendation_entity.ContainerInitialResourceRequestCPU) {
							colVal := val[columnIdx].(json.Number).String()
							sampleObj := utils.GetSampleInstance(&timeObj, colVal)
							initialResourceRequestCPUData = append(initialResourceRequestCPUData, sampleObj)
						} else if column == string(recommendation_entity.ContainerResourceLimitCPU) {
							colVal := val[columnIdx].(json.Number).String()
							sampleObj := utils.GetSampleInstance(&timeObj, colVal)
							resourceLimitCPUData = append(resourceLimitCPUData, sampleObj)
						} else if column == string(recommendation_entity.ContainerResourceRequestCPU) {
							colVal := val[columnIdx].(json.Number).String()
							sampleObj := utils.GetSampleInstance(&timeObj, colVal)
							resourceRequestCPUData = append(resourceRequestCPUData, sampleObj)
						} else if column == string(recommendation_entity.ContainerInitialResourceLimitMemory) {
							colVal := val[columnIdx].(json.Number).String()
							sampleObj := utils.GetSampleInstance(&timeObj, colVal)
							initialResourceLimitMemoryData = append(initialResourceLimitMemoryData, sampleObj)
						} else if column == string(recommendation_entity.ContainerInitialResourceRequestMemory) {
							colVal := val[columnIdx].(json.Number).String()
							sampleObj := utils.GetSampleInstance(&timeObj, colVal)
							initialResourceRequestMemoryData = append(initialResourceRequestMemoryData, sampleObj)
						} else if column == string(recommendation_entity.ContainerResourceLimitMemory) {
							colVal := val[columnIdx].(json.Number).String()
							sampleObj := utils.GetSampleInstance(&timeObj, colVal)
							resourceLimitMemoryData = append(resourceLimitMemoryData, sampleObj)
						} else if column == string(recommendation_entity.ContainerResourceRequestMemory) {
							colVal := val[columnIdx].(json.Number).String()
							sampleObj := utils.GetSampleInstance(&timeObj, colVal)
							resourceRequestMemoryData = append(resourceRequestMemoryData, sampleObj)
						}
					}
				}

				if len(initialResourceLimitCPUData) > 0 {
					containerRecommendation.InitialLimitRecommendations[int32(datahub_metric_v1alpha2.MetricType_CPU_USAGE_SECONDS_PERCENTAGE)] =
						&datahub_metric_v1alpha2.MetricData{
							Data: initialResourceLimitCPUData,
						}
				}
				if len(initialResourceLimitMemoryData) > 0 {
					containerRecommendation.InitialLimitRecommendations[int32(datahub_metric_v1alpha2.MetricType_MEMORY_USAGE_BYTES)] =
						&datahub_metric_v1alpha2.MetricData{
							Data: initialResourceLimitMemoryData,
						}
				}
				if len(initialResourceRequestCPUData) > 0 {
					containerRecommendation.InitialRequestRecommendations[int32(datahub_metric_v1alpha2.MetricType_CPU_USAGE_SECONDS_PERCENTAGE)] =
						&datahub_metric_v1alpha2.MetricData{
							Data: initialResourceRequestCPUData,
						}
				}
				if len(initialResourceRequestMemoryData) > 0 {
					containerRecommendation.InitialRequestRecommendations[int32(datahub_metric_v1alpha2.MetricType_MEMORY_USAGE_BYTES)] =
						&datahub_metric_v1alpha2.MetricData{
							Data: initialResourceRequestMemoryData,
						}
				}
				if len(resourceLimitCPUData) > 0 {
					containerRecommendation.LimitRecommendations[int32(datahub_metric_v1alpha2.MetricType_CPU_USAGE_SECONDS_PERCENTAGE)] =
						&datahub_metric_v1alpha2.MetricData{
							Data: resourceLimitCPUData,
						}
				}
				if len(resourceLimitMemoryData) > 0 {
					containerRecommendation.LimitRecommendations[int32(datahub_metric_v1alpha2.MetricType_MEMORY_USAGE_BYTES)] =
						&datahub_metric_v1alpha2.MetricData{
							Data: resourceLimitMemoryData,
						}
				}
				if len(resourceRequestCPUData) > 0 {
					containerRecommendation.RequestRecommendations[int32(datahub_metric_v1alpha2.MetricType_CPU_USAGE_SECONDS_PERCENTAGE)] =
						&datahub_metric_v1alpha2.MetricData{
							Data: resourceRequestCPUData,
						}
				}
				if len(resourceRequestMemoryData) > 0 {
					containerRecommendation.RequestRecommendations[int32(datahub_metric_v1alpha2.MetricType_MEMORY_USAGE_BYTES)] =
						&datahub_metric_v1alpha2.MetricData{
							Data: resourceRequestMemoryData,
						}
				}

				foundPodRec := false
				for podRecommendationIdx, podRecommendation := range podRecommendations {
					if podRecommendation.GetNamespacedName().GetNamespace() == podNS && podRecommendation.GetNamespacedName().GetName() == podName {
						foundPodRec = true
						podRecommendations[podRecommendationIdx].ContainerRecommendations = append(podRecommendations[podRecommendationIdx].ContainerRecommendations, containerRecommendation)
					}
				}
				if !foundPodRec {
					podRecommendations = append(podRecommendations, &datahub_recommendation_v1alpha2.PodRecommendation{
						NamespacedName: &datahub_resource_metadata_v1alpha2.NamespacedName{
							Namespace: podNS,
							Name:      podName,
						},
						ContainerRecommendations: []*datahub_recommendation_v1alpha2.ContainerRecommendation{
							containerRecommendation,
						},
					})
				}
			}
		}
		return podRecommendations, nil
	} else {
		return podRecommendations, errors.Wrapf(err, "list container recommendations failed: %s", err.Error())
	}
}

func (containerRepository *ContainerRepository) buildOrderClause(queryCondition *datahub_v1alpha2.QueryCondition) string {
	if queryCondition == nil {
		return "ORDER BY time ASC"
	}
	if queryCondition.GetOrder() == datahub_v1alpha2.QueryCondition_DESC {
		return "ORDER BY time DESC"
	} else if queryCondition.GetOrder() == datahub_v1alpha2.QueryCondition_ASC {
		return "ORDER BY time ASC"
	}
	return "ORDER BY time ASC"
}

func (containerRepository *ContainerRepository) buildLimitClause(queryCondition *datahub_v1alpha2.QueryCondition) string {
	if queryCondition == nil {
		return ""
	}
	limit := queryCondition.GetLimit()
	if queryCondition.GetLimit() > 0 {
		return fmt.Sprintf("LIMIT %v", limit)
	}
	return ""
}
