package metric

import (
	"errors"
	"fmt"
	"time"

	"github.com/containers-ai/karina/datahub/pkg/entity/prometheus/nodeMemoryAvailableBytes"
	"github.com/containers-ai/karina/datahub/pkg/entity/prometheus/nodeMemoryBytesTotal"
	"github.com/containers-ai/karina/datahub/pkg/repository/prometheus"
)

// NodeMemoryUsageBytesRepository Repository to access metric from prometheus
type NodeMemoryUsageBytesRepository struct {
	PrometheusConfig prometheus.Config
}

// NewNodeMemoryUsageBytesRepositoryWithConfig New node cpu usage percentage repository with prometheus configuration
func NewNodeMemoryUsageBytesRepositoryWithConfig(cfg prometheus.Config) NodeMemoryUsageBytesRepository {
	return NodeMemoryUsageBytesRepository{PrometheusConfig: cfg}
}

// ListMetricsByPodNamespacedName Provide metrics from response of querying request contain namespace, pod_name and default labels
func (n NodeMemoryUsageBytesRepository) ListMetricsByNodeName(nodeName string, startTime, endTime time.Time) ([]prometheus.Entity, error) {

	var (
		err error

		prometheusClient *prometheus.Prometheus

		nodeMemoryBytesTotalMetricName            string
		nodeMemoryAvailableBytesMetricName        string
		nodeMemoryBytesTotalQueryLabelsString     string
		nodeMemoryAvailableBytesQueryLabelsString string
		queryExpression                           string

		response prometheus.Response

		entities []prometheus.Entity
	)

	prometheusClient, err = prometheus.New(n.PrometheusConfig)
	if err != nil {
		return entities, errors.New("ListMetricsByNodeName failed: " + err.Error())
	}

	nodeMemoryBytesTotalMetricName = nodeMemoryBytesTotal.MetricName
	nodeMemoryAvailableBytesMetricName = nodeMemoryAvailableBytes.MetricName
	nodeMemoryBytesTotalQueryLabelsString = n.buildNodeMemoryBytesTotalQueryLabelsStringByNodeName(nodeName)
	nodeMemoryAvailableBytesQueryLabelsString = n.buildNodeMemoryAvailableBytesQueryLabelsStringByNodeName(nodeName)

	if nodeMemoryBytesTotalQueryLabelsString != "" && nodeMemoryAvailableBytesQueryLabelsString != "" {
		queryExpression = fmt.Sprintf("%s{%s} - %s{%s}", nodeMemoryBytesTotalMetricName, nodeMemoryBytesTotalQueryLabelsString, nodeMemoryAvailableBytesMetricName, nodeMemoryAvailableBytesQueryLabelsString)
	} else {
		queryExpression = fmt.Sprintf("%s - %s", nodeMemoryBytesTotalMetricName, nodeMemoryAvailableBytesMetricName)
	}

	response, err = prometheusClient.QueryRange(queryExpression, startTime, endTime)
	if err != nil {
		return entities, errors.New("ListMetricsByNodeName failed: " + err.Error())
	} else if response.Status != prometheus.StatusSuccess {
		return entities, errors.New("ListMetricsByNodeName failed: receive error response from prometheus: " + response.Error)
	}

	entities, err = response.GetEntitis()
	if err != nil {
		return entities, errors.New("ListMetricsByNodeName failed: " + err.Error())
	}

	return entities, nil
}

func (n NodeMemoryUsageBytesRepository) buildNodeMemoryBytesTotalQueryLabelsStringByNodeName(nodeName string) string {

	var (
		queryLabelsString = ""
	)

	if nodeName != "" {
		queryLabelsString += fmt.Sprintf(`%s = "%s"`, nodeMemoryBytesTotal.NodeLabel, nodeName)
	}

	return queryLabelsString
}

func (n NodeMemoryUsageBytesRepository) buildNodeMemoryAvailableBytesQueryLabelsStringByNodeName(nodeName string) string {

	var (
		queryLabelsString = ""
	)

	if nodeName != "" {
		queryLabelsString += fmt.Sprintf(`%s = "%s"`, nodeMemoryAvailableBytes.NodeLabel, nodeName)
	}

	return queryLabelsString
}
