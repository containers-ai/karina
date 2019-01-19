package prometheus

import (
	"errors"
	"sync"
	"time"

	"github.com/containers-ai/karina/datahub/pkg/dao/metric"
	"github.com/containers-ai/karina/datahub/pkg/entity/prometheus/containerCPUUsagePercentage"
	"github.com/containers-ai/karina/datahub/pkg/entity/prometheus/containerMemoryUsageBytes"
	"github.com/containers-ai/karina/datahub/pkg/entity/prometheus/nodeCPUUsagePercentage"
	"github.com/containers-ai/karina/datahub/pkg/entity/prometheus/nodeMemoryUsageBytes"
	"github.com/containers-ai/karina/datahub/pkg/repository/prometheus"
	promRepository "github.com/containers-ai/karina/datahub/pkg/repository/prometheus/metric"
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
	containerCPUEntities, err = podContainerCPURepo.ListMetricsByPodNamespacedName(req.Namespace, req.PodName, req.StartTime, req.EndTime)
	if err != nil {
		return podsMetricMap, errors.New("list pod metrics failed: " + err.Error())
	}

	for _, entity := range containerCPUEntities {
		containerCPUEntity := containerCPUUsagePercentage.NewEntityFromPrometheusEntity(entity)
		containerMetric := containerCPUEntity.ContainerMetric()
		ptrPodsMetricMap.AddContainerMetric(&containerMetric)
	}

	podContainerMemoryRepo = promRepository.NewPodContainerMemoryUsageBytesRepositoryWithConfig(p.prometheusConfig)
	containerMemoryEntities, err = podContainerMemoryRepo.ListMetricsByPodNamespacedName(req.Namespace, req.PodName, req.StartTime, req.EndTime)
	if err != nil {
		return podsMetricMap, errors.New("list pod metrics failed: " + err.Error())
	}

	for _, entity := range containerMemoryEntities {
		containerMemoryEntity := containerMemoryUsageBytes.NewEntityFromPrometheusEntity(entity)
		containerMetric := containerMemoryEntity.ContainerMetric()
		ptrPodsMetricMap.AddContainerMetric(&containerMetric)
	}

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
		go p.produceNodeMetric(nodeName, req.StartTime, req.EndTime, nodeMetricChan, errChan, &wg)
	}

	go addNodeMetricToNodesMetricMap(ptrNodesMetricMap, nodeMetricChan, done)

	wg.Wait()
	close(nodeMetricChan)

	select {
	case _ = <-done:
	case err := <-errChan:
		return metric.NodesMetricMap{}, err
	}

	return *ptrNodesMetricMap, nil
}

func (p *prometheusMetricDAOImpl) produceNodeMetric(nodeName string, startTime time.Time, endTime time.Time, nodeMetricChan chan<- metric.NodeMetric, errChan chan<- error, wg *sync.WaitGroup) {

	var (
		err                     error
		nodeCPUUsageRepo        promRepository.NodeCPUUsagePercentageRepository
		nodeMemoryUsageRepo     promRepository.NodeMemoryUsageBytesRepository
		nodeCPUUsageEntities    []prometheus.Entity
		nodeMemoryUsageEntities []prometheus.Entity
	)

	defer wg.Done()

	nodeCPUUsageRepo = promRepository.NewNodeCPUUsagePercentageRepositoryWithConfig(p.prometheusConfig)
	nodeCPUUsageEntities, err = nodeCPUUsageRepo.ListMetricsByNodeName(nodeName, startTime, endTime)
	if err != nil {
		errChan <- errors.New("list node metrics failed: " + err.Error())
		return
	}

	for _, entity := range nodeCPUUsageEntities {
		nodeCPUUsageEntity := nodeCPUUsagePercentage.NewEntityFromPrometheusEntity(entity)
		nodeMetric := nodeCPUUsageEntity.NodeMetric()
		nodeMetricChan <- nodeMetric
	}

	nodeMemoryUsageRepo = promRepository.NewNodeMemoryUsageBytesRepositoryWithConfig(p.prometheusConfig)
	nodeMemoryUsageEntities, err = nodeMemoryUsageRepo.ListMetricsByNodeName(nodeName, startTime, endTime)
	if err != nil {
		errChan <- errors.New("list node metrics failed: " + err.Error())
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
