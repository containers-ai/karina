package metric

import (
	"fmt"
	"time"

	"github.com/containers-ai/karina/datahub/pkg/entity/prometheus/nodeCPUUsagePercentage"
	"github.com/containers-ai/karina/datahub/pkg/repository/prometheus"
	"github.com/pkg/errors"
)

// NodeCPUUsagePercentageRepository Repository to access metric node:node_cpu_utilisation:avg1m from prometheus
type NodeCPUUsagePercentageRepository struct {
	PrometheusConfig prometheus.Config
}

// NewNodeCPUUsagePercentageRepositoryWithConfig New node cpu usage percentage repository with prometheus configuration
func NewNodeCPUUsagePercentageRepositoryWithConfig(cfg prometheus.Config) NodeCPUUsagePercentageRepository {
	return NodeCPUUsagePercentageRepository{PrometheusConfig: cfg}
}

// ListMetricsByPodNamespacedName Provide metrics from response of querying request contain namespace, pod_name and default labels
func (n NodeCPUUsagePercentageRepository) ListMetricsByNodeName(nodeName string, startTime, endTime *time.Time, stepTime *time.Duration) ([]prometheus.Entity, error) {

	var (
		err error

		prometheusClient *prometheus.Prometheus

		metricName        string
		queryLabelsString string
		queryExpression   string

		response prometheus.Response

		entities []prometheus.Entity
	)

	prometheusClient, err = prometheus.New(n.PrometheusConfig)
	if err != nil {
		return entities, errors.Wrapf(err, "list cpu metrics by node name failed: %s", err.Error())
	}

	metricName = nodeCPUUsagePercentage.MetricName
	queryLabelsString = n.buildQueryLabelsStringByNodeName(nodeName)

	if queryLabelsString != "" {
		queryExpression = fmt.Sprintf("%s{%s}", metricName, queryLabelsString)
	} else {
		queryExpression = fmt.Sprintf("%s", metricName)
	}

	response, err = prometheusClient.QueryRange(queryExpression, startTime, endTime, stepTime)
	if err != nil {
		return entities, err
	} else if response.Status != prometheus.StatusSuccess {
		return entities, errors.New("list cpu metrics by node name failed: receive error response from prometheus: " + response.Error)
	}

	entities, err = response.GetEntitis()
	if err != nil {
		return entities, errors.Wrapf(err, "list cpu metrics by node name failed: %s", err.Error())
	}

	return entities, nil
}

func (n NodeCPUUsagePercentageRepository) buildQueryLabelsStringByNodeName(nodeName string) string {

	var (
		queryLabelsString = ""
	)

	if nodeName != "" {
		queryLabelsString += fmt.Sprintf(`%s = "%s"`, nodeCPUUsagePercentage.NodeLabel, nodeName)
	}

	return queryLabelsString
}
