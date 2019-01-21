package datahub

import (
	datahub_metric_v1alpha2 "github.com/containers-ai/api/datahub/metric/v1alpha2"
	datahub_prediction_v1alpha2 "github.com/containers-ai/api/datahub/prediction/v1alpha2"
	datahub_resource_metadata_v1alpha2 "github.com/containers-ai/api/datahub/resource/metadata/v1alpha2"
	metric_dao "github.com/containers-ai/karina/datahub/pkg/dao/metric"
	"github.com/containers-ai/karina/datahub/pkg/dao/prediction"
	"github.com/containers-ai/karina/datahub/pkg/metric"
	"github.com/golang/protobuf/ptypes"
)

type metricDataWrap struct {
	metricType int32
	metricData datahub_metric_v1alpha2.MetricData
}

type daoPodMetricExtended struct {
	*metric_dao.PodMetric
}

func (p daoPodMetricExtended) datahubPodMetric() *datahub_metric_v1alpha2.PodMetric {

	var (
		datahubPodMetric datahub_metric_v1alpha2.PodMetric
	)

	datahubPodMetric = datahub_metric_v1alpha2.PodMetric{
		NamespacedName: &datahub_resource_metadata_v1alpha2.NamespacedName{
			Namespace: string(p.Namespace),
			Name:      string(p.PodName),
		},
		ContainerMetrics: make([]*datahub_metric_v1alpha2.ContainerMetric, 0),
	}

	for _, containerMetric := range *p.ContainersMetricMap {
		containerMetricExtended := daoContainerMetricExtended{containerMetric}
		datahubContainerMetric := containerMetricExtended.datahubContainerMetric()
		datahubPodMetric.ContainerMetrics = append(datahubPodMetric.ContainerMetrics, datahubContainerMetric)
	}

	return &datahubPodMetric
}

type daoContainerMetricExtended struct {
	*metric_dao.ContainerMetric
}

func (c daoContainerMetricExtended) datahubContainerMetric() *datahub_metric_v1alpha2.ContainerMetric {

	var (
		metricDataWrap  = make(chan metricDataWrap)
		numOfGoroutines = 0

		datahubContainerMetric datahub_metric_v1alpha2.ContainerMetric
	)

	datahubContainerMetric = datahub_metric_v1alpha2.ContainerMetric{
		Name:       string(c.ContainerName),
		MetricData: make(map[int32]*datahub_metric_v1alpha2.MetricData),
	}

	for metricType, samples := range c.Metrics {
		if datahubMetricType, exist := metric.TypeToDatahubMetricType[metricType]; exist {
			numOfGoroutines++
			go produceDatahubMetricDataFromSamples(datahubMetricType, samples, metricDataWrap)
		}
	}

	for i := 0; i < numOfGoroutines; i++ {
		receivedMetricDataWrap := <-metricDataWrap
		datahubContainerMetric.MetricData[receivedMetricDataWrap.metricType] = &receivedMetricDataWrap.metricData
	}

	return &datahubContainerMetric
}

type daoNodeMetricExtended struct {
	*metric_dao.NodeMetric
}

func (n daoNodeMetricExtended) datahubNodeMetric() *datahub_metric_v1alpha2.NodeMetric {

	var (
		metricDataWrap  = make(chan metricDataWrap)
		numOfGoroutines = 0

		datahubNodeMetric datahub_metric_v1alpha2.NodeMetric
	)

	datahubNodeMetric = datahub_metric_v1alpha2.NodeMetric{
		Name:       n.NodeName,
		MetricData: make(map[int32]*datahub_metric_v1alpha2.MetricData),
	}

	for metricType, samples := range n.Metrics {
		if datahubMetricType, exist := metric.TypeToDatahubMetricType[metricType]; exist {
			numOfGoroutines++
			go produceDatahubMetricDataFromSamples(datahubMetricType, samples, metricDataWrap)
		}
	}

	for i := 0; i < numOfGoroutines; i++ {
		receivedMetricDataWrap := <-metricDataWrap
		datahubNodeMetric.MetricData[receivedMetricDataWrap.metricType] = &receivedMetricDataWrap.metricData
	}

	return &datahubNodeMetric
}

type daoPtrPodPredictionExtended struct {
	*prediction.PodPrediction
}

func (p daoPtrPodPredictionExtended) datahubPodPrediction() *datahub_prediction_v1alpha2.PodPrediction {

	var (
		datahubPodPrediction datahub_prediction_v1alpha2.PodPrediction
	)

	datahubPodPrediction = datahub_prediction_v1alpha2.PodPrediction{
		NamespacedName: &datahub_resource_metadata_v1alpha2.NamespacedName{
			Namespace: string(p.Namespace),
			Name:      string(p.PodName),
		},
	}

	for _, ptrContainerPrediction := range *p.ContainersPredictionMap {
		containerPredictionExtended := daoContainerPredictionExtended{ptrContainerPrediction}
		datahubContainerPrediction := containerPredictionExtended.datahubContainerPrediction()
		datahubPodPrediction.ContainerPredictions = append(datahubPodPrediction.ContainerPredictions, datahubContainerPrediction)
	}

	return &datahubPodPrediction
}

type daoContainerPredictionExtended struct {
	*prediction.ContainerPrediction
}

func (c daoContainerPredictionExtended) datahubContainerPrediction() *datahub_prediction_v1alpha2.ContainerPrediction {

	var (
		metricDataWrap = make(chan metricDataWrap)
		numOfGoroutine = 0

		datahubContainerPrediction datahub_prediction_v1alpha2.ContainerPrediction
	)

	datahubContainerPrediction = datahub_prediction_v1alpha2.ContainerPrediction{
		Name:             string(c.ContainerName),
		PredictedRawData: make(map[int32]*datahub_metric_v1alpha2.MetricData),
	}

	for metricType, samples := range c.Predictions {
		if datahubMetricType, exist := metric.TypeToDatahubMetricType[metricType]; exist {
			numOfGoroutine++
			go produceDatahubMetricDataFromSamples(datahubMetricType, samples, metricDataWrap)
		}
	}

	for i := 0; i < numOfGoroutine; i++ {
		receivedMetricDataWrap := <-metricDataWrap
		datahubContainerPrediction.PredictedRawData[receivedMetricDataWrap.metricType] = &receivedMetricDataWrap.metricData
	}

	return &datahubContainerPrediction
}

type daoPtrNodePredictionExtended struct {
	*prediction.NodePrediction
}

func (d daoPtrNodePredictionExtended) datahubNodePrediction() *datahub_prediction_v1alpha2.NodePrediction {

	var (
		metricDataWrap = make(chan metricDataWrap)
		numOfGoroutine = 0

		datahubNodePrediction datahub_prediction_v1alpha2.NodePrediction
	)

	datahubNodePrediction = datahub_prediction_v1alpha2.NodePrediction{
		Name:             string(d.NodeName),
		IsScheduled:      d.IsScheduled,
		PredictedRawData: make(map[int32]*datahub_metric_v1alpha2.MetricData),
	}

	for metricType, samples := range d.Predictions {
		if datahubMetricType, exist := metric.TypeToDatahubMetricType[metricType]; exist {
			numOfGoroutine++
			go produceDatahubMetricDataFromSamples(datahubMetricType, samples, metricDataWrap)
		}
	}

	for i := 0; i < numOfGoroutine; i++ {
		receivedMetricDataWrap := <-metricDataWrap
		datahubNodePrediction.PredictedRawData[receivedMetricDataWrap.metricType] = &receivedMetricDataWrap.metricData
	}

	return &datahubNodePrediction
}

type daoPtrNodesPredictionMapExtended struct {
	*prediction.NodesPredictionMap
}

func (d daoPtrNodesPredictionMapExtended) datahubNodePredictions() []*datahub_prediction_v1alpha2.NodePrediction {

	var (
		datahubNodePredictions = make([]*datahub_prediction_v1alpha2.NodePrediction, 0)
	)

	for _, ptrIsScheduledNodePredictionMap := range *d.NodesPredictionMap {

		if ptrScheduledNodePrediction, exist := (*ptrIsScheduledNodePredictionMap)[true]; exist {

			scheduledNodePredictionExtended := daoPtrNodePredictionExtended{ptrScheduledNodePrediction}
			sechduledDatahubNodePrediction := scheduledNodePredictionExtended.datahubNodePrediction()
			datahubNodePredictions = append(datahubNodePredictions, sechduledDatahubNodePrediction)
		}

		if noneScheduledNodePrediction, exist := (*ptrIsScheduledNodePredictionMap)[false]; exist {

			noneScheduledNodePredictionExtended := daoPtrNodePredictionExtended{noneScheduledNodePrediction}
			noneSechduledDatahubNodePrediction := noneScheduledNodePredictionExtended.datahubNodePrediction()
			datahubNodePredictions = append(datahubNodePredictions, noneSechduledDatahubNodePrediction)
		}
	}

	return datahubNodePredictions
}

func produceDatahubMetricDataFromSamples(metricType datahub_metric_v1alpha2.MetricType, samples []metric.Sample, mdWrap chan<- metricDataWrap) {

	var (
		datahubMetricData datahub_metric_v1alpha2.MetricData
	)

	datahubMetricData = datahub_metric_v1alpha2.MetricData{}

	for _, sample := range samples {

		// TODO: Send error to caller
		googleTimestamp, err := ptypes.TimestampProto(sample.Timestamp)
		if err != nil {
			googleTimestamp = nil
		}

		datahubSample := datahub_metric_v1alpha2.Sample{Time: googleTimestamp, NumValue: sample.Value}
		datahubMetricData.Data = append(datahubMetricData.Data, &datahubSample)
	}

	mdWrap <- metricDataWrap{
		metricType: int32(metricType),
		metricData: datahubMetricData,
	}
}
