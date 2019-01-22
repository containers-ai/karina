package prediction

import (
	"fmt"

	prediction_dao "github.com/containers-ai/karina/datahub/pkg/dao/prediction"
	container_entity "github.com/containers-ai/karina/datahub/pkg/entity/influxdb/prediction/container"
	"github.com/containers-ai/karina/datahub/pkg/repository/influxdb"
	influxdb_client "github.com/influxdata/influxdb/client/v2"
	"github.com/pkg/errors"
)

// ContainerRepository Repository to access containers' prediction data
type ContainerRepository struct {
	influxDB *influxdb.InfluxDBRepository
}

// NewContainerRepositoryWithConfig New container repository with influxDB configuration
func NewContainerRepositoryWithConfig(influxDBCfg influxdb.Config) *ContainerRepository {
	return &ContainerRepository{
		influxDB: &influxdb.InfluxDBRepository{
			Address:  influxDBCfg.Address,
			Username: influxDBCfg.Username,
			Password: influxDBCfg.Password,
		},
	}
}

// CreateContainerPrediction Create containers' prediction into influxDB
func (r *ContainerRepository) CreateContainerPrediction(containersPrediction []*prediction_dao.ContainerPrediction) error {

	var (
		err error

		points []*influxdb_client.Point
	)

	for _, containerPrediction := range containersPrediction {

		namespace := containerPrediction.Namespace
		podName := containerPrediction.PodName
		containerName := containerPrediction.ContainerName

		for metricType, samples := range containerPrediction.Predictions {

			if metricName, exist := container_entity.PkgMetricTypeToLocalMetricType[metricType]; exist {

				for _, sample := range samples {

					tags := map[string]string{
						container_entity.Namespace: namespace,
						container_entity.PodName:   podName,
						container_entity.Name:      containerName,
						container_entity.Metric:    metricName,
					}
					fields := map[string]interface{}{
						container_entity.Value: sample.Value,
					}
					point, err := influxdb_client.NewPoint(string(Container), tags, fields, sample.Timestamp)
					if err != nil {
						return errors.Errorf("create container predictions failed: new influxdb data point failed: %s", err.Error())
					}
					points = append(points, point)
				}
			} else {
				return errors.Wrapf(err, `create container predictions failed: map metric type from github.com/containers-ai/karina.datahub.metric.ContainerMetricType
				 to type in github.com/containers-ai/karina/datahub/pkg/entity/influxdb/prediction/container falied: metric type not exist %+v`, metricType)
			}
		}
	}

	err = r.influxDB.WritePoints(points, influxdb_client.BatchPointsConfig{
		Database: string(influxdb.Prediction),
	})
	if err != nil {
		return errors.Wrapf(err, "create container predictions failed: %s", err.Error())
	}

	return nil
}

// ListContainerPredictionsByRequest list containers' prediction from influxDB
func (r *ContainerRepository) ListContainerPredictionsByRequest(request prediction_dao.ListPodPredictionsRequest) ([]*container_entity.Entity, error) {

	var (
		err error

		results  []influxdb_client.Result
		rows     []*influxdb.InfluxDBRow
		entities []*container_entity.Entity
	)

	whereClause := r.buildInfluxQLWhereClauseFromRequest(request)
	influxdbStatement := influxdb.Statement{
		Measurement: Container,
		WhereClause: whereClause,
		GroupByTags: []string{container_entity.Namespace, container_entity.PodName, container_entity.Name, container_entity.Metric},
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
		return entities, errors.Wrapf(err, "list container prediction failed: %s", err.Error())
	}

	rows = influxdb.PackMap(results)
	for _, row := range rows {
		for _, data := range row.Data {
			entity := container_entity.NewEntityFromMap(data)
			entities = append(entities, &entity)
		}
	}

	return entities, nil
}

func (r *ContainerRepository) buildInfluxQLWhereClauseFromRequest(request prediction_dao.ListPodPredictionsRequest) string {

	var (
		whereClause string
		conditions  string
	)

	if request.Namespace != "" {
		conditions += fmt.Sprintf(`"%s" = '%s' and `, container_entity.Namespace, request.Namespace)
	}
	if request.PodName != "" {
		conditions += fmt.Sprintf(`"%s" = '%s'`, container_entity.PodName, request.PodName)
	}

	if conditions != "" {
		whereClause = fmt.Sprintf("where %s", conditions)
	}

	return whereClause
}
