package prediction

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	prediction_dao "github.com/containers-ai/karina/datahub/pkg/dao/prediction"
	node_entity "github.com/containers-ai/karina/datahub/pkg/entity/influxdb/prediction/node"
	"github.com/containers-ai/karina/datahub/pkg/repository/influxdb"
	influxdb_client "github.com/influxdata/influxdb/client/v2"
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
						return errors.New("new influxdb datapoint failed: " + err.Error())
					}
					points = append(points, point)
				}
			} else {
				return fmt.Errorf("map metric type from github.com/containers-ai/karina.datahub.metric.NodeMetricType to type in db falied: metric type not exist %+v", metricType)
			}
		}
	}

	err = r.influxDB.WritePoints(points, influxdb_client.BatchPointsConfig{
		Database: string(influxdb.Prediction),
	})
	if err != nil {
		return errors.New("write influxdb datapoint failed: " + err.Error())
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
	lastClause, whereTimeClause := r.buildTimeClause(request)
	if whereTimeClause != "" {
		if whereClause != "" {
			whereClause += "and " + whereTimeClause
		} else {
			whereClause = "where " + whereTimeClause
		}
	}
	whereClause = strings.TrimSuffix(whereClause, "and ")

	cmd := fmt.Sprintf("SELECT * FROM %s %s %s", string(Node), whereClause, lastClause)

	results, err = r.influxDB.QueryDB(cmd, string(influxdb.Prediction))
	if err != nil {
		return entities, err

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

func (r *NodeRepository) buildTimeClause(request prediction_dao.ListNodePredictionsRequest) (string, string) {

	var (
		lastClause      string
		whereTimeClause string

		startTime = time.Now()
	)

	if request.StartTime == nil && request.EndTime == nil {
		lastClause = "order by time desc limit 1"
	} else {

		if request.StartTime != nil {
			startTime = *request.StartTime
		}

		nanoTimestampInString := strconv.FormatInt(int64(startTime.UnixNano()), 10)
		whereTimeClause = fmt.Sprintf("time > %s", nanoTimestampInString)

		if request.EndTime != nil {
			nanoTimestampInString := strconv.FormatInt(int64(request.EndTime.UnixNano()), 10)
			whereTimeClause += fmt.Sprintf(" and time <= %s", nanoTimestampInString)
		}
	}

	return lastClause, whereTimeClause
}
