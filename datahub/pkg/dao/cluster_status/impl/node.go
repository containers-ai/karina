package impl

import (
	datahub_resource_v1alpha2 "github.com/containers-ai/api/datahub/resource/v1alpha2"
	datahub_v1alpha2 "github.com/containers-ai/api/datahub/v1alpha2"
	influxdb_repository "github.com/containers-ai/karina/datahub/pkg/repository/influxdb"
	influxdb_repository_cluster_status "github.com/containers-ai/karina/datahub/pkg/repository/influxdb/cluster_status"
	"github.com/containers-ai/karina/pkg/utils/log"
)

var (
	scope = log.RegisterScope("node_dao_implement", "node dao implement", 0)
)

// Node Implement Node interface
type Node struct {
	InfluxDBConfig influxdb_repository.Config
}

// AddNodes adds nodes
func (node *Node) AddNodes(nodes []*datahub_resource_v1alpha2.Node) error {
	nodeRepository := influxdb_repository_cluster_status.NewNodeRepository(&node.InfluxDBConfig)
	return nodeRepository.AddNodes(nodes)
}

// DeleteNodes deletes nodes
func (node *Node) DeleteNodes(nodes []*datahub_resource_v1alpha2.Node) error {
	nodeRepository := influxdb_repository_cluster_status.NewNodeRepository(&node.InfluxDBConfig)
	nodeEntities, _ := nodeRepository.ListNodeEntities(nodes)
	for nodeEntityIdx := range nodeEntities {
		inCluster := false
		nodeEntities[nodeEntityIdx].InCluster = &inCluster
	}
	return nodeRepository.UpdateNodes(nodeEntities)
}

// UpdateNodes update nodes
func (node *Node) UpdateNodes(updatedNodes []*datahub_v1alpha2.UpdateNodesRequest_UpdatedNode) error {
	nodes := []*datahub_resource_v1alpha2.Node{}
	for _, updateNode := range updatedNodes {
		nodes = append(nodes, &datahub_resource_v1alpha2.Node{
			Name: updateNode.Name,
		})
	}

	nodeRepository := influxdb_repository_cluster_status.NewNodeRepository(&node.InfluxDBConfig)
	nodeEntities, _ := nodeRepository.ListNodeEntities(nodes)
	for nodeEntityIdx := range nodeEntities {
		for _, updateNode := range updatedNodes {
			if updateNode.Name == *nodeEntities[nodeEntityIdx].Name {
				if isPredictedWrap := updateNode.GetIsPredicted(); isPredictedWrap != nil {
					isPredicted := isPredictedWrap.GetIsPredicted()
					nodeEntities[nodeEntityIdx].IsPredicted = &isPredicted
				}
				break
			}
		}
	}
	return nodeRepository.UpdateNodes(nodeEntities)
}

// ListNodes lists nodes
func (node *Node) ListNodes(isPredicted bool) ([]*datahub_resource_v1alpha2.Node, error) {
	predictedNodes := []*datahub_resource_v1alpha2.Node{}
	nodeRepository := influxdb_repository_cluster_status.NewNodeRepository(&node.InfluxDBConfig)
	entities, _ := nodeRepository.ListNodes(&isPredicted)
	for _, entity := range entities {
		predictedNodes = append(predictedNodes, &datahub_resource_v1alpha2.Node{
			Name: *entity.Name,
		})
	}
	return predictedNodes, nil
}
