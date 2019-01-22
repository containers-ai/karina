package clusterstatus

import (
	"fmt"

	datahub_resource_v1alpha2 "github.com/containers-ai/api/datahub/resource/v1alpha2"
	cluster_status_entity "github.com/containers-ai/karina/datahub/pkg/entity/influxdb/cluster_status"
	"github.com/containers-ai/karina/datahub/pkg/repository/influxdb"
	"github.com/containers-ai/karina/pkg/utils/log"
	influxdb_client "github.com/influxdata/influxdb/client/v2"
	"github.com/pkg/errors"
)

var (
	scope = log.RegisterScope("influxdb_repo_node_measurement", "InfluxDB repository node measurement", 0)
)

type NodeRepository struct {
	influxDB *influxdb.InfluxDBRepository
}

func (nodeRepository *NodeRepository) IsTag(column string) bool {
	for _, tag := range cluster_status_entity.NodeTags {
		if column == string(tag) {
			return true
		}
	}
	return false
}

// NewNodeRepository returns NodeRepository instance
func NewNodeRepository(influxDBCfg *influxdb.Config) *NodeRepository {
	return &NodeRepository{
		influxDB: &influxdb.InfluxDBRepository{
			Address:  influxDBCfg.Address,
			Username: influxDBCfg.Username,
			Password: influxDBCfg.Password,
		},
	}
}

// AddNodes add node information to database
func (nodeRepository *NodeRepository) AddNodes(nodes []*datahub_resource_v1alpha2.Node) error {
	points := []*influxdb_client.Point{}
	for _, node := range nodes {
		tags := map[string]string{
			string(cluster_status_entity.NodeName): node.Name,
		}
		fields := map[string]interface{}{
			string(cluster_status_entity.NodeInCluster):   true,
			string(cluster_status_entity.NodeIsPredicted): node.GetIsPredicted(),
		}

		if pt, err := influxdb_client.NewPoint(string(Node), tags, fields, influxdb.ZeroTime); err == nil {
			points = append(points, pt)
		} else {
			scope.Error(err.Error())
		}
	}
	err := nodeRepository.influxDB.WritePoints(points, influxdb_client.BatchPointsConfig{
		Database: string(influxdb.ClusterStatus),
	})
	if err != nil {
		return errors.Wrapf(err, "add nodes failed: %s", err.Error())
	}

	return nil
}

// RemoveNodes removes nodes
func (nodeRepository *NodeRepository) RemoveNodes(nodes []*datahub_resource_v1alpha2.Node) error {

	nodeEntities, err := nodeRepository.ListNodeEntities(nodes)
	if err != nil {
		return errors.Wrapf(err, "remove nodes failed: %s", err)
	}

	for nodeEntityIdx := range nodeEntities {
		inCluster := false
		nodeEntities[nodeEntityIdx].InCluster = &inCluster
	}

	err = nodeRepository.UpdateNodes(nodeEntities)
	if err != nil {
		return errors.Wrapf(err, "remove nodes failed: %s", err)
	}

	return nil
}

// ListNodeEntities returns node entities
func (nodeRepository *NodeRepository) ListNodeEntities(nodes []*datahub_resource_v1alpha2.Node) ([]*cluster_status_entity.NodeEntity, error) {
	nodeEntities := make([]*cluster_status_entity.NodeEntity, 0)
	for _, node := range nodes {
		// SELECT * FROM node WHERE "name"='%s' AND in_cluster=true ORDER BY time ASC LIMIT 1
		cmd := fmt.Sprintf("SELECT * FROM %s WHERE \"%s\"='%s' AND \"%s\"=%t ORDER BY time ASC LIMIT 1",
			string(Node), string(cluster_status_entity.NodeName), node.Name,
			string(cluster_status_entity.NodeInCluster), true)

		if results, err := nodeRepository.influxDB.QueryDB(cmd, string(influxdb.ClusterStatus)); err == nil {
			rows := influxdb.PackMap(results)
			for _, row := range rows {
				for _, data := range row.Data {
					entity := cluster_status_entity.NewNodeEntityFromMap(data)
					nodeEntities = append(nodeEntities, &entity)
				}
			}
		} else {
			return nodeEntities, errors.Wrapf(err, "list node entities failed: %s", err.Error())
		}
	}
	return nodeEntities, nil
}

// UpdateNodes updates nodes' fields into container measurement
func (nodeRepository *NodeRepository) UpdateNodes(nodeEntities []*cluster_status_entity.NodeEntity) error {

	var (
		pointsToUpdate = make([]*influxdb_client.Point, 0)
	)

	for _, nodeEntity := range nodeEntities {
		point, err := (*nodeEntity).InfluxDBPoint(string(Node))
		if err != nil {
			return errors.Wrapf(err, "update nodes failed: %s", err.Error())
		}

		pointsToUpdate = append(pointsToUpdate, point)
	}

	err := nodeRepository.influxDB.WritePoints(pointsToUpdate, influxdb_client.BatchPointsConfig{
		Database: string(influxdb.ClusterStatus),
	})
	if err != nil {
		return errors.Wrapf(err, "update nodes failed: %s", err.Error())
	}

	return nil
}

// ListNodes lists nodes in cluster
func (nodeRepository *NodeRepository) ListNodes(isPredicted *bool) ([]*cluster_status_entity.NodeEntity, error) {
	nodeEntities := []*cluster_status_entity.NodeEntity{}
	cmd := fmt.Sprintf("SELECT * FROM %s WHERE \"%s\"=%t", string(Node), string(cluster_status_entity.NodeInCluster), true)
	if isPredicted != nil {
		cmd = fmt.Sprintf("SELECT * FROM %s WHERE \"%s\"=%t AND \"%s\"=%t", string(Node),
			string(cluster_status_entity.NodeInCluster), true, string(cluster_status_entity.NodeIsPredicted), *isPredicted)
	}
	scope.Infof(fmt.Sprintf("List nodes in cluster CMD: %s", cmd))
	if results, err := nodeRepository.influxDB.QueryDB(cmd, string(influxdb.ClusterStatus)); err == nil {
		rows := influxdb.PackMap(results)
		for _, row := range rows {
			for _, data := range row.Data {
				entity := cluster_status_entity.NewNodeEntityFromMap(data)
				nodeEntities = append(nodeEntities, &entity)
			}
		}
	} else {
		return nodeEntities, errors.Wrapf(err, "list nodes failed: %s", err.Error())
	}
	return nodeEntities, nil
}
