package prometheus

import (
	"strings"
	"sync"
	"time"

	"github.com/containers-ai/karina/datahub/pkg/dao/metric"
	"github.com/containers-ai/karina/datahub/pkg/entity/prometheus/containerCPUUsagePercentage"
	"github.com/containers-ai/karina/datahub/pkg/entity/prometheus/containerMemoryUsageBytes"
	"github.com/containers-ai/karina/datahub/pkg/entity/prometheus/nodeCPUUsagePercentage"
	"github.com/containers-ai/karina/datahub/pkg/entity/prometheus/nodeMemoryUsageBytes"
	"github.com/containers-ai/karina/datahub/pkg/repository/prometheus"
	promRepository "github.com/containers-ai/karina/datahub/pkg/repository/prometheus/metric"
	"github.com/pkg/errors"
)

const (
	prometheusDecreaseQueryMessage = "Try decreasing the query resolution"
)

type prometheusMetricDAOImpl struct {
	prometheusConfig prometheus.Config
}

// NewWithConfig Constructor of prometheus metric dao
func NewWithConfig(config prometheus.Config) metric.MetricsDAO {
	return &prometheusMetricDAOImpl{prometheusConfig: config}
}

// ListPodMetrics Method implementation of MetricsDAO
func (p *prometheusMetricDAOImpl) ListPodMetrics(req metric.ListPodMetricsRequest) (metric.PodsMetricMap, error) {

	var (
		err error

		podContainerCPURepo     promRepository.PodContainerCPUUsagePercentageRepository
		podContainerMemoryRepo  promRepository.PodContainerMemoryUsageBytesRepository
		containerCPUEntities    []prometheus.Entity
		containerMemoryEntities []prometheus.Entity

		podsMetricMap    = metric.PodsMetricMap{}
		ptrPodsMetricMap = &podsMetricMap
	)

	podContainerCPURepo = promRepository.NewPodContainerCPUUsagePercentageRepositoryWithConfig(p.prometheusConfig)
	containerCPUEntities, err = podContainerCPURepo.ListMetricsByPodNamespacedName(req.Namespace, req.PodName, req.StartTime, req.EndTime, req.StepTime)
	if err != nil {
		if strings.Contains(err.Error(), prometheusDecreaseQueryMessage) {
			e := metric.NewErrorQueryConditionExceedMaximum("list pod metrics failed: %s"+err.Error(), err)
			return podsMetricMap, e
		}
		return podsMetricMap, errors.Wrapf(err, "list pod metrics failed: %s", err.Error())
	}

	for _, entity := range containerCPUEntities {
		containerCPUEntity := containerCPUUsagePercentage.NewEntityFromPrometheusEntity(entity)
		containerMetric := containerCPUEntity.ContainerMetric()
		ptrPodsMetricMap.AddContainerMetric(&containerMetric)
	}

	podContainerMemoryRepo = promRepository.NewPodContainerMemoryUsageBytesRepositoryWithConfig(p.prometheusConfig)
	containerMemoryEntities, err = podContainerMemoryRepo.ListMetricsByPodNamespacedName(req.Namespace, req.PodName, req.StartTime, req.EndTime, req.StepTime)
	if err != nil {
		if strings.Contains(err.Error(), prometheusDecreaseQueryMessage) {
			e := metric.NewErrorQueryConditionExceedMaximum(err.Error(), err)
			return podsMetricMap, e
		}
		return podsMetricMap, errors.Wrapf(err, "list pod metrics failed: %s", err.Error())
	}

	for _, entity := range containerMemoryEntities {
		containerMemoryEntity := containerMemoryUsageBytes.NewEntityFromPrometheusEntity(entity)
		containerMetric := containerMemoryEntity.ContainerMetric()
		ptrPodsMetricMap.AddContainerMetric(&containerMetric)
	}

	ptrPodsMetricMap.SortByTimestamp(req.QueryCondition.TimestampOrder)
	ptrPodsMetricMap.Limit(req.QueryCondition.Limit)

	return *ptrPodsMetricMap, nil
}

// ListNodesMetric Method implementation of MetricsDAO
func (p *prometheusMetricDAOImpl) ListNodesMetric(req metric.ListNodeMetricsRequest) (metric.NodesMetricMap, error) {

	var (
		wg             = sync.WaitGroup{}
		nodeNames      []string
		nodeMetricChan = make(chan metric.NodeMetric)
		errChan        chan error
		done           = make(chan bool)

		nodesMetricMap    = metric.NodesMetricMap{}
		ptrNodesMetricMap = &nodesMetricMap
	)

	if len(req.GetNodeNames()) != 0 {
		nodeNames = req.GetNodeNames()
	} else {
		nodeNames = req.GetEmptyNodeNames()
	}

	errChan = make(chan error, len(nodeNames))
	wg.Add(len(nodeNames))
	for _, nodeName := range nodeNames {
		go p.produceNodeMetric(nodeName, req.StartTime, req.EndTime, req.StepTime, nodeMetricChan, errChan, &wg)
	}

	go addNodeMetricToNodesMetricMap(ptrNodesMetricMap, nodeMetricChan, done)

	wg.Wait()
	close(nodeMetricChan)

	select {
	case _ = <-done:
	case err := <-errChan:
		if strings.Contains(err.Error(), prometheusDecreaseQueryMessage) {
			e := metric.NewErrorQueryConditionExceedMaximum("list node metrics failed: %s"+err.Error(), err)
			return metric.NodesMetricMap{}, e
		}
		return metric.NodesMetricMap{}, errors.Wrapf(err, "list node metrics failed: %s", err.Error())
	}

	ptrNodesMetricMap.SortByTimestamp(req.QueryCondition.TimestampOrder)
	ptrNodesMetricMap.Limit(req.QueryCondition.Limit)

	return *ptrNodesMetricMap, nil
}

func (p *prometheusMetricDAOImpl) produceNodeMetric(nodeName string, startTime *time.Time, endTime *time.Time, stepTime *time.Duration, nodeMetricChan chan<- metric.NodeMetric, errChan chan<- error, wg *sync.WaitGroup) {

	var (
		err                     error
		nodeCPUUsageRepo        promRepository.NodeCPUUsagePercentageRepository
		nodeMemoryUsageRepo     promRepository.NodeMemoryUsageBytesRepository
		nodeCPUUsageEntities    []prometheus.Entity
		nodeMemoryUsageEntities []prometheus.Entity
	)

	defer wg.Done()

	nodeCPUUsageRepo = promRepository.NewNodeCPUUsagePercentageRepositoryWithConfig(p.prometheusConfig)
	nodeCPUUsageEntities, err = nodeCPUUsageRepo.ListMetricsByNodeName(nodeName, startTime, endTime, stepTime)
	if err != nil {
		errChan <- err
		return
	}

	for _, entity := range nodeCPUUsageEntities {
		nodeCPUUsageEntity := nodeCPUUsagePercentage.NewEntityFromPrometheusEntity(entity)
		nodeMetric := nodeCPUUsageEntity.NodeMetric()
		nodeMetricChan <- nodeMetric
	}

	nodeMemoryUsageRepo = promRepository.NewNodeMemoryUsageBytesRepositoryWithConfig(p.prometheusConfig)
	nodeMemoryUsageEntities, err = nodeMemoryUsageRepo.ListMetricsByNodeName(nodeName, startTime, endTime, stepTime)
	if err != nil {
		errChan <- err
		return
	}

	for _, entity := range nodeMemoryUsageEntities {
		noodeMemoryUsageEntity := nodeMemoryUsageBytes.NewEntityFromPrometheusEntity(entity)
		nodeMetric := noodeMemoryUsageEntity.NodeMetric()
		nodeMetricChan <- nodeMetric
	}
}

func addNodeMetricToNodesMetricMap(nodesMetricMap *metric.NodesMetricMap, nodeMetricChan <-chan metric.NodeMetric, done chan<- bool) {

	for {
		nodeMetric, more := <-nodeMetricChan
		if more {
			nodesMetricMap.AddNodeMetric(&nodeMetric)
		} else {
			done <- true
			return
		}
	}
}
