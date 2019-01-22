package metric

import (
	"fmt"
	"time"

	"github.com/containers-ai/karina/datahub/pkg/entity/prometheus/containerCPUUsagePercentage"
	"github.com/containers-ai/karina/datahub/pkg/repository/prometheus"
	"github.com/pkg/errors"
)

// PodContainerCPUUsagePercentageRepository Repository to access metric namespace_pod_name_container_name:container_cpu_usage_seconds_total:sum_rate from prometheus
type PodContainerCPUUsagePercentageRepository struct {
	PrometheusConfig prometheus.Config
}

// NewPodContainerCPUUsagePercentageRepositoryWithConfig New pod container cpu usage percentage repository with prometheus configuration
func NewPodContainerCPUUsagePercentageRepositoryWithConfig(cfg prometheus.Config) PodContainerCPUUsagePercentageRepository {
	return PodContainerCPUUsagePercentageRepository{PrometheusConfig: cfg}
}

// ListMetricsByPodNamespacedName Provide metrics from response of querying request contain namespace, pod_name and default labels
func (c PodContainerCPUUsagePercentageRepository) ListMetricsByPodNamespacedName(namespace string, podName string, startTime, endTime *time.Time, stepTime *time.Duration) ([]prometheus.Entity, error) {

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
		return entities, errors.Wrapf(err, "list metrics by pod namespaced name failed: %s", err.Error())
	}

	metricName = containerCPUUsagePercentage.MetricName
	queryLabelsString = c.buildQueryLabelsStringByNamespaceAndPodName(namespace, podName)

	if queryLabelsString != "" {
		queryExpression = fmt.Sprintf("%s{%s}", metricName, queryLabelsString)
	} else {
		queryExpression = fmt.Sprintf("%s", metricName)
	}

	response, err = prometheusClient.QueryRange(queryExpression, startTime, endTime, stepTime)
	if err != nil {
		return entities, errors.Wrapf(err, "list metrics by pod namespaced name failed: %s", err.Error())
	} else if response.Status != prometheus.StatusSuccess {
		return entities, errors.Wrapf(err, "list metrics by pod namespaced name failed: receive error response from prometheus: %s", response.Error)
	}

	entities, err = response.GetEntitis()
	if err != nil {
		return entities, errors.Wrapf(err, "list metrics by pod namespaced name failed: %s", err.Error())
	}

	return entities, nil
}

func (c PodContainerCPUUsagePercentageRepository) buildDefaultQueryLabelsString() string {

	var queryLabelsString = ""

	queryLabelsString += fmt.Sprintf(`%s != "",`, containerCPUUsagePercentage.PodLabelName)
	queryLabelsString += fmt.Sprintf(`%s != "POD"`, containerCPUUsagePercentage.ContainerLabel)

	return queryLabelsString
}

func (c PodContainerCPUUsagePercentageRepository) buildQueryLabelsStringByNamespaceAndPodName(namespace string, podName string) string {

	var (
		queryLabelsString = c.buildDefaultQueryLabelsString()
	)

	if namespace != "" {
		queryLabelsString += fmt.Sprintf(`,%s = "%s"`, containerCPUUsagePercentage.NamespaceLabel, namespace)
	}

	if podName != "" {
		queryLabelsString += fmt.Sprintf(`,%s = "%s"`, containerCPUUsagePercentage.PodLabelName, podName)
	}

	return queryLabelsString
}
