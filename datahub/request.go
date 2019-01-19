package datahub

import (
	"errors"
	"time"

	datahub_metric_v1alpha2 "github.com/containers-ai/api/datahub/metric/v1alpha2"
	datahub_v1alpha2 "github.com/containers-ai/api/datahub/v1alpha2"
	prediction_dao "github.com/containers-ai/karina/datahub/pkg/dao/prediction"
	"github.com/containers-ai/karina/datahub/pkg/metric"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
)

type datahubListPodMetricsRequestExtended struct {
	datahub_v1alpha2.ListPodMetricsRequest
}

func (r datahubListPodMetricsRequestExtended) validate() error {

	var (
		startTime *timestamp.Timestamp
		endTime   *timestamp.Timestamp
	)

	if r.GetQueryCondition() == nil && r.GetQueryCondition().GetTimeRange() == nil {
		return errors.New("field \"time_range\" cannot be empty")
	}

	startTime = r.GetQueryCondition().GetTimeRange().StartTime
	endTime = r.GetQueryCondition().GetTimeRange().EndTime
	if startTime == nil || endTime == nil {
		return errors.New("field \"start_time\" and \"end_time\"  cannot be empty")
	}

	if startTime.Seconds+int64(startTime.Nanos) >= endTime.Seconds+int64(endTime.Nanos) {
		return errors.New("\"end_time\" must not be before \"start_time\"")
	}

	return nil
}

type datahubListNodeMetricsRequestExtended struct {
	datahub_v1alpha2.ListNodeMetricsRequest
}

func (r datahubListNodeMetricsRequestExtended) validate() error {

	var (
		startTime *timestamp.Timestamp
		endTime   *timestamp.Timestamp
	)

	if r.GetQueryCondition() != nil && r.GetQueryCondition().GetTimeRange() != nil {
		return errors.New("field \"time_range\" cannot be empty")
	}

	startTime = r.GetQueryCondition().GetTimeRange().StartTime
	endTime = r.GetQueryCondition().GetTimeRange().EndTime
	if startTime == nil || endTime == nil {
		return errors.New("field \"start_time\" and \"end_time\"  cannot be empty")
	}

	if startTime.Seconds+int64(startTime.Nanos) >= endTime.Seconds+int64(endTime.Nanos) {
		return errors.New("\"end_time\" must not be before \"start_time\"")
	}

	return nil
}

type datahubCreatePodPredictionsRequestExtended struct {
	datahub_v1alpha2.CreatePodPredictionsRequest
}

func (r datahubCreatePodPredictionsRequestExtended) validate() error {
	return nil
}

func (r datahubCreatePodPredictionsRequestExtended) daoContainerPredictions() []*prediction_dao.ContainerPrediction {

	var (
		containerPredictions []*prediction_dao.ContainerPrediction
	)

	for _, datahubPodPrediction := range r.PodPredictions {

		podNamespace := ""
		podName := ""
		if datahubPodPrediction.GetNamespacedName() != nil {
			podNamespace = datahubPodPrediction.GetNamespacedName().GetNamespace()
			podName = datahubPodPrediction.GetNamespacedName().GetName()
		}

		for _, datahubContainerPrediction := range datahubPodPrediction.GetContainerPredictions() {
			containerName := datahubContainerPrediction.GetName()

			for rawKey, rawData := range datahubContainerPrediction.GetPredictedRawData() {

				containerPrediction := prediction_dao.ContainerPrediction{
					Namespace:     podNamespace,
					PodName:       podName,
					ContainerName: containerName,
					Predictions:   make(map[metric.ContainerMetricType][]metric.Sample),
				}

				samples := []metric.Sample{}
				for _, datahubSample := range rawData.GetData() {
					time, err := ptypes.Timestamp(datahubSample.GetTime())
					if err != nil {
						scope.Error(" failed: " + err.Error())
					}
					sample := metric.Sample{
						Timestamp: time,
						Value:     datahubSample.GetNumValue(),
					}
					samples = append(samples, sample)
				}

				var metricType metric.ContainerMetricType
				switch rawKey {
				case int32(datahub_metric_v1alpha2.MetricType_CPU_USAGE_SECONDS_PERCENTAGE):
					metricType = metric.TypeContainerCPUUsageSecondsPercentage
				case int32(datahub_metric_v1alpha2.MetricType_MEMORY_USAGE_BYTES):
					metricType = metric.TypeContainerMemoryUsageBytes
				}
				containerPrediction.Predictions[metricType] = samples

				containerPredictions = append(containerPredictions, &containerPrediction)
			}
		}
	}

	return containerPredictions
}

type datahubCreateNodePredictionsRequestExtended struct {
	datahub_v1alpha2.CreateNodePredictionsRequest
}

func (r datahubCreateNodePredictionsRequestExtended) validate() error {
	return nil
}

func (r datahubCreateNodePredictionsRequestExtended) daoNodePredictions() []*prediction_dao.NodePrediction {

	var (
		NodePredictions []*prediction_dao.NodePrediction
	)

	for _, datahubNodePrediction := range r.NodePredictions {

		nodeName := datahubNodePrediction.GetName()
		isScheduled := datahubNodePrediction.GetIsScheduled()

		for rawKey, rawData := range datahubNodePrediction.GetPredictedRawData() {

			samples := []metric.Sample{}
			for _, datahubSample := range rawData.GetData() {
				time, err := ptypes.Timestamp(datahubSample.GetTime())
				if err != nil {
					scope.Error(" failed: " + err.Error())
				}
				sample := metric.Sample{
					Timestamp: time,
					Value:     datahubSample.GetNumValue(),
				}
				samples = append(samples, sample)
			}

			NodePrediction := prediction_dao.NodePrediction{
				NodeName:    nodeName,
				IsScheduled: isScheduled,
				Predictions: make(map[metric.NodeMetricType][]metric.Sample),
			}

			var metricType metric.ContainerMetricType
			switch rawKey {
			case int32(datahub_metric_v1alpha2.MetricType_CPU_USAGE_SECONDS_PERCENTAGE):
				metricType = metric.TypeNodeCPUUsageSecondsPercentage
			case int32(datahub_metric_v1alpha2.MetricType_MEMORY_USAGE_BYTES):
				metricType = metric.TypeNodeMemoryUsageBytes
			}
			NodePrediction.Predictions[metricType] = samples

			NodePredictions = append(NodePredictions, &NodePrediction)
		}
	}

	return NodePredictions
}

type datahubListPodPredictionsRequestExtended struct {
	datahub_v1alpha2.ListPodPredictionsRequest
}

func (r datahubListPodPredictionsRequestExtended) daoListPodPredictionsRequest() prediction_dao.ListPodPredictionsRequest {

	var (
		namespace string
		podName   string
		startTime *time.Time
		endTime   *time.Time
	)

	if r.GetNamespacedName() != nil {
		namespace = r.GetNamespacedName().GetNamespace()
		podName = r.GetNamespacedName().GetName()
	}

	if r.GetQueryCondition() != nil && r.GetQueryCondition().GetTimeRange() != nil {

		if r.GetQueryCondition().GetTimeRange().GetStartTime() != nil {
			tmpStartTime, _ := ptypes.Timestamp(r.GetQueryCondition().GetTimeRange().GetStartTime())
			startTime = &tmpStartTime
		}

		if r.GetQueryCondition().GetTimeRange().GetEndTime() != nil {
			tmpEndTime, _ := ptypes.Timestamp(r.GetQueryCondition().GetTimeRange().GetEndTime())
			endTime = &tmpEndTime
		}
	}

	listContainerPredictionsRequest := prediction_dao.ListPodPredictionsRequest{
		Namespace: namespace,
		PodName:   podName,
		StartTime: startTime,
		EndTime:   endTime,
	}

	return listContainerPredictionsRequest
}

type datahubListNodePredictionsRequestExtended struct {
	datahub_v1alpha2.ListNodePredictionsRequest
}

func (r datahubListNodePredictionsRequestExtended) daoListNodePredictionsRequest() prediction_dao.ListNodePredictionsRequest {

	var (
		nodeNames []string
		startTime *time.Time
		endTime   *time.Time
	)

	if r.GetQueryCondition() != nil && r.GetQueryCondition().GetTimeRange() != nil {

		if r.GetQueryCondition().GetTimeRange().GetStartTime() != nil {
			tmpStartTime, _ := ptypes.Timestamp(r.GetQueryCondition().GetTimeRange().GetStartTime())
			startTime = &tmpStartTime
		}

		if r.GetQueryCondition().GetTimeRange().GetEndTime() != nil {
			tmpEndTime, _ := ptypes.Timestamp(r.GetQueryCondition().GetTimeRange().GetEndTime())
			endTime = &tmpEndTime
		}
	}

	for _, nodeName := range r.GetNodeNames() {
		nodeNames = append(nodeNames, nodeName)
	}

	listNodePredictionsRequest := prediction_dao.ListNodePredictionsRequest{
		NodeNames: nodeNames,
		StartTime: startTime,
		EndTime:   endTime,
	}

	return listNodePredictionsRequest
}
