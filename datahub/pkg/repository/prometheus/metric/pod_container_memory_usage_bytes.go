package metric

import (
	"errors"
	"fmt"
	"time"

	"github.com/containers-ai/karina/datahub/pkg/entity/prometheus/containerMemoryUsageBytes"
	"github.com/containers-ai/karina/datahub/pkg/repository/prometheus"
)

// PodContainerMemoryUsageBytesRepository Repository to access metric container_memory_usage_bytes from prometheus
type PodContainerMemoryUsageBytesRepository struct {
	PrometheusConfig prometheus.Config
}

// NewPodContainerMemoryUsageBytesRepositoryWithConfig New pod container memory usage bytes repository with prometheus configuration
func NewPodContainerMemoryUsageBytesRepositoryWithConfig(cfg prometheus.Config) PodContainerMemoryUsageBytesRepository {
	return PodContainerMemoryUsageBytesRepository{PrometheusConfig: cfg}
}

// ListMetricsByPodNamespacedName Provide metrics from response of querying request contain namespace, pod_name and default labels
func (c PodContainerMemoryUsageBytesRepository) ListMetricsByPodNamespacedName(namespace string, podName string, startTime, endTime *time.Time, stepTime *time.Duration) ([]prometheus.Entity, error) {

	var (
		err error

		prometheusClient *prometheus.Prometheus

		metricName        string
		queryLabelsString string
		queryExpression   string

		response prometheus.Response

		entities []prometheus.Entity
	)

	prometheusClient, err = prometheus.New(c.PrometheusConfig)
	if err != nil {
		return entities, errors.New("QueryRangeByPodNamespacedName failed: " + err.Error())
	}

	metricName = containerMemoryUsageBytes.MetricName
	queryLabelsString = c.buildQueryLabelsStringByNamespaceAndPodName(namespace, podName)

	if queryLabelsString != "" {
		queryExpression = fmt.Sprintf("%s{%s}", metricName, queryLabelsString)
	} else {
		queryExpression = fmt.Sprintf("%s", metricName)
	}

	response, err = prometheusClient.QueryRange(queryExpression, startTime, endTime, stepTime)
	if err != nil {
		return entities, errors.New("QueryRangeByPodNamespacedName failed: " + err.Error())
	} else if response.Status != prometheus.StatusSuccess {
		return entities, errors.New("QueryRangeByPodNamespacedName failed: receive error response from prometheus: " + response.Error)
	}

	entities, err = response.GetEntitis()
	if err != nil {
		return entities, errors.New("ListMetricsByPodNamespacedName failed: " + err.Error())
	}

	return entities, nil
}

func (c PodContainerMemoryUsageBytesRepository) buildDefaultQueryLabelsString() string {

	var queryLabelsString = ""

	queryLabelsString += fmt.Sprintf(`%s != "" ,`, containerMemoryUsageBytes.PodLabelName)
	queryLabelsString += fmt.Sprintf(`%s != "" ,`, containerMemoryUsageBytes.ContainerLabel)
	queryLabelsString += fmt.Sprintf(`%s != "POD"`, containerMemoryUsageBytes.ContainerLabel)

	return queryLabelsString
}

func (c PodContainerMemoryUsageBytesRepository) buildQueryLabelsStringByNamespaceAndPodName(namespace string, podName string) string {

	var (
		queryLabelsString = c.buildDefaultQueryLabelsString()
	)

	if namespace != "" {
		queryLabelsString += fmt.Sprintf(`,%s = "%s"`, containerMemoryUsageBytes.NamespaceLabel, namespace)
	}

	if podName != "" {
		queryLabelsString += fmt.Sprintf(`,%s = "%s"`, containerMemoryUsageBytes.PodLabelName, podName)
	}

	return queryLabelsString
}
