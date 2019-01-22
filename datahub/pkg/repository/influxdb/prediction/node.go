package prediction

import (
	"fmt"
	"strconv"
	"strings"

	prediction_dao "github.com/containers-ai/karina/datahub/pkg/dao/prediction"
	node_entity "github.com/containers-ai/karina/datahub/pkg/entity/influxdb/prediction/node"
	"github.com/containers-ai/karina/datahub/pkg/repository/influxdb"
	influxdb_client "github.com/influxdata/influxdb/client/v2"
	"github.com/pkg/errors"
)

// NodeRepository Repository to access containers' prediction data
type NodeRepository struct {
	influxDB *influxdb.InfluxDBRepository
}

// NewNodeRepositoryWithConfig New container repository with influxDB configuration
func NewNodeRepositoryWithConfig(influxDBCfg influxdb.Config) *NodeRepository {
	return &NodeRepository{
		influxDB: &influxdb.InfluxDBRepository{
			Address:  influxDBCfg.Address,
			Username: influxDBCfg.Username,
			Password: influxDBCfg.Password,
		},
	}
}

// CreateNodePrediction Create containers' prediction into influxDB
func (r *NodeRepository) CreateNodePrediction(nodePredictions []*prediction_dao.NodePrediction) error {

	var (
		err error

		points []*influxdb_client.Point
	)

	for _, nodePrediction := range nodePredictions {

		nodeName := nodePrediction.NodeName
		isScheduled := strconv.FormatBool(nodePrediction.IsScheduled)

		for metricType, samples := range nodePrediction.Predictions {

			if metricName, exist := node_entity.PkgMetricTypeToLocalMetricType[metricType]; exist {

				for _, sample := range samples {

					tags := map[string]string{
						node_entity.Name:        nodeName,
						node_entity.IsScheduled: isScheduled,
						node_entity.Metric:      metricName,
					}
					fields := map[string]interface{}{
						node_entity.Value: sample.Value,
					}
					point, err := influxdb_client.NewPoint(string(Node), tags, fields, sample.Timestamp)
					if err != nil {
						return errors.New("create node prediction failed: new influxdb datapoint failed: " + err.Error())
					}
					points = append(points, point)
				}
			} else {
				return errors.Wrapf(err, "create node prediction failed: map metric type from github.com/containers-ai/karina.datahub.metric.NodeMetricType to type in db falied: metric type not exist %+v", metricType)
			}
		}
	}

	err = r.influxDB.WritePoints(points, influxdb_client.BatchPointsConfig{
		Database: string(influxdb.Prediction),
	})
	if err != nil {
		return errors.Wrapf(err, "create node prediction failed: %s", err.Error())
	}

	return nil
}

// ListNodePredictionsByRequest list containers' prediction from influxDB
func (r *NodeRepository) ListNodePredictionsByRequest(request prediction_dao.ListNodePredictionsRequest) ([]*node_entity.Entity, error) {

	var (
		err error

		results  []influxdb_client.Result
		rows     []*influxdb.InfluxDBRow
		entities []*node_entity.Entity
	)

	whereClause := r.buildInfluxQLWhereClauseFromRequest(request)
	influxdbStatement := influxdb.Statement{
		Measurement: Node,
		WhereClause: whereClause,
		GroupByTags: []string{node_entity.Name, node_entity.Metric, node_entity.IsScheduled},
	}

	queryCondition := influxdb.QueryCondition{
		StartTime:      request.QueryCondition.StartTime,
		EndTime:        request.QueryCondition.EndTime,
		StepTime:       request.QueryCondition.StepTime,
		TimestampOrder: request.QueryCondition.TimestampOrder,
		Limit:          request.QueryCondition.Limit,
	}
	influxdbStatement.AppendTimeConditionIntoWhereClause(queryCondition)
	influxdbStatement.SetLimitClauseFromQueryCondition(queryCondition)
	influxdbStatement.SetOrderClauseFromQueryCondition(queryCondition)
	cmd := influxdbStatement.BuildQueryCmd()

	results, err = r.influxDB.QueryDB(cmd, string(influxdb.Prediction))
	if err != nil {
		return entities, errors.Wrapf(err, "list node prediction by request failed: %s", err.Error())

	}

	rows = influxdb.PackMap(results)
	for _, row := range rows {
		for _, data := range row.Data {
			entity := node_entity.NewEntityFromMap(data)
			entities = append(entities, &entity)
		}
	}

	return entities, nil
}

func (r *NodeRepository) buildInfluxQLWhereClauseFromRequest(request prediction_dao.ListNodePredictionsRequest) string {

	var (
		whereClause string
		conditions  string
	)

	for _, nodeName := range request.NodeNames {
		conditions += fmt.Sprintf(`"%s" = '%s' or `, node_entity.Name, nodeName)
	}

	conditions = strings.TrimSuffix(conditions, "or ")

	if conditions != "" {
		whereClause = fmt.Sprintf("where %s", conditions)
	}

	return whereClause
}
