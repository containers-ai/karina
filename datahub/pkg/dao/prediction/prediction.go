package prediction

import (
	"fmt"

	"github.com/containers-ai/karina/datahub/pkg/dao"
	"github.com/containers-ai/karina/datahub/pkg/kubernetes/metadata"
	"github.com/containers-ai/karina/datahub/pkg/metric"
)

// IsScheduled Specified if the node prediction is scheduled
type IsScheduled = bool

// DAO DAO interface of prediction
type DAO interface {
	ListPodPredictions(ListPodPredictionsRequest) (*PodsPredictionMap, error)
	ListNodePredictions(ListNodePredictionsRequest) (*NodesPredictionMap, error)
	CreateContainerPredictions([]*ContainerPrediction) error
	CreateNodePredictions([]*NodePrediction) error
}

// ListPodPredictionsRequest ListPodPredictionsRequest
type ListPodPredictionsRequest struct {
	Namespace string
	PodName   string
	dao.QueryCondition
}

// ListNodePredictionsRequest ListNodePredictionsRequest
type ListNodePredictionsRequest struct {
	NodeNames []string
	dao.QueryCondition
}

// ContainerPrediction Prediction model to represent one container Prediction
type ContainerPrediction struct {
	Namespace     metadata.NamespaceName
	PodName       metadata.PodName
	ContainerName metadata.ContainerName
	Predictions   map[metric.ContainerMetricType][]metric.Sample
}

// BuildPodPrediction Build PodPrediction consist of the receiver in ContainersPredictionMap.
func (c *ContainerPrediction) BuildPodPrediction() *PodPrediction {

	containersPredictionMap := ContainersPredictionMap{}
	containersPredictionMap[c.NamespacePodContainerName()] = c

	return &PodPrediction{
		Namespace:               c.Namespace,
		PodName:                 c.PodName,
		ContainersPredictionMap: &containersPredictionMap,
	}
}

// NamespacePodContainerName Return identity of the container Prediction.
func (c ContainerPrediction) NamespacePodContainerName() metadata.NamespacePodContainerName {
	return metadata.NamespacePodContainerName(fmt.Sprintf("%s/%s/%s", c.Namespace, c.PodName, c.ContainerName))
}

// ContainersPredictionMap Containers Prediction map
type ContainersPredictionMap map[metadata.NamespacePodContainerName]*ContainerPrediction

// BuildPodsPredictionMap Build PodsPredictionMap base on current ContainersPredictionMap
func (c ContainersPredictionMap) BuildPodsPredictionMap() *PodsPredictionMap {

	var (
		podsPredictionMap = &PodsPredictionMap{}
	)

	for _, containerPrediction := range c {
		podsPredictionMap.AddContainerPrediction(containerPrediction)
	}

	return podsPredictionMap
}

// Merge Merge current ContainersPredictionMap with input ContainersPredictionMap
func (c *ContainersPredictionMap) Merge(in *ContainersPredictionMap) {

	for namespacePodContainerName, containerPrediction := range *in {
		if existedContainerPrediction, exist := (*c)[namespacePodContainerName]; exist {
			for metricType, predictions := range containerPrediction.Predictions {
				existedContainerPrediction.Predictions[metricType] = append(existedContainerPrediction.Predictions[metricType], predictions...)
			}
			(*c)[namespacePodContainerName] = existedContainerPrediction
		} else {
			(*c)[namespacePodContainerName] = containerPrediction
		}
	}
}

// PodPrediction Prediction model to represent one pod's Prediction
type PodPrediction struct {
	Namespace               metadata.NamespaceName
	PodName                 metadata.PodName
	ContainersPredictionMap *ContainersPredictionMap
}

// NamespacePodName Return identity of the pod Prediction
func (p PodPrediction) NamespacePodName() metadata.NamespacePodName {
	return metadata.NamespacePodName(fmt.Sprintf("%s/%s", p.Namespace, p.PodName))
}

// Merge Merge current PodPrediction with input PodPrediction
func (p *PodPrediction) Merge(in *PodPrediction) {
	p.ContainersPredictionMap.Merge(in.ContainersPredictionMap)
}

// PodsPredictionMap Pods' Prediction map
type PodsPredictionMap map[metadata.NamespacePodName]*PodPrediction

// AddContainerPrediction Add container Prediction into PodsPredictionMap
func (p *PodsPredictionMap) AddContainerPrediction(c *ContainerPrediction) {

	podPrediction := c.BuildPodPrediction()
	namespacePodName := podPrediction.NamespacePodName()
	if existedPodPrediction, exist := (*p)[namespacePodName]; exist {
		existedPodPrediction.Merge(podPrediction)
	} else {
		(*p)[namespacePodName] = podPrediction
	}
}

// NodePrediction Prediction model to represent one node Prediction
type NodePrediction struct {
	NodeName    metadata.NodeName
	IsScheduled bool
	Predictions map[metric.NodeMetricType][]metric.Sample
}

// Merge Merge current NodePrediction with input NodePrediction
func (n *NodePrediction) Merge(in *NodePrediction) {

	for metricType, metrics := range in.Predictions {
		n.Predictions[metricType] = append(n.Predictions[metricType], metrics...)
	}
}

// IsScheduledNodePredictionMap Nodes' Prediction map
type IsScheduledNodePredictionMap map[IsScheduled]*NodePrediction

// NodesPredictionMap Nodes' Prediction map
type NodesPredictionMap map[metadata.NodeName]*IsScheduledNodePredictionMap

// AddNodePrediction Add node Prediction into NodesPredictionMap
func (n *NodesPredictionMap) AddNodePrediction(nodePrediction *NodePrediction) {

	nodeName := nodePrediction.NodeName
	isScheduled := nodePrediction.IsScheduled

	if existIsScheduledNodePredictionMap, exist := (*n)[nodeName]; exist {
		if existNodePrediction, exist := (*existIsScheduledNodePredictionMap)[isScheduled]; exist {
			existNodePrediction.Merge(nodePrediction)
		} else {
			(*existIsScheduledNodePredictionMap)[isScheduled] = nodePrediction
		}
	} else {
		isScheduledNodePredictionMap := make(IsScheduledNodePredictionMap)
		(*n)[nodeName] = &isScheduledNodePredictionMap
		(*(*n)[nodeName])[isScheduled] = nodePrediction
	}
}
