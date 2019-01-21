package impl

import (
	"errors"

	"github.com/containers-ai/karina/datahub/pkg/dao/prediction"
	influxdb_container_preditcion_entity "github.com/containers-ai/karina/datahub/pkg/entity/influxdb/prediction/container"
	influxdb_node_preditcion_entity "github.com/containers-ai/karina/datahub/pkg/entity/influxdb/prediction/node"
	influxdb_repository "github.com/containers-ai/karina/datahub/pkg/repository/influxdb"
	influxdb_repository_preditcion "github.com/containers-ai/karina/datahub/pkg/repository/influxdb/prediction"
)

type influxDB struct {
	influxDBConfig influxdb_repository.Config
}

// NewInfluxDBWithConfig Constructor of influxdb prediction dao
func NewInfluxDBWithConfig(config influxdb_repository.Config) prediction.DAO {
	return influxDB{
		influxDBConfig: config,
	}
}

// CreateContainerPredictions Implementation of prediction dao interface
func (i influxDB) CreateContainerPredictions(containerPredictions []*prediction.ContainerPrediction) error {

	var (
		err error

		predictionRepo *influxdb_repository_preditcion.ContainerRepository
	)

	predictionRepo = influxdb_repository_preditcion.NewContainerRepositoryWithConfig(i.influxDBConfig)

	err = predictionRepo.CreateContainerPrediction(containerPredictions)
	if err != nil {
		return errors.New("create container prediction failed: " + err.Error())
	}

	return nil
}

// ListPodPredictions Implementation of prediction dao interface
func (i influxDB) ListPodPredictions(request prediction.ListPodPredictionsRequest) (*prediction.PodsPredictionMap, error) {

	var (
		err error

		predictionRepo                      *influxdb_repository_preditcion.ContainerRepository
		influxDBContainerPredictionEntities []*influxdb_container_preditcion_entity.Entity
		podsPredictionMap                   *prediction.PodsPredictionMap
	)

	podsPredictionMap = &prediction.PodsPredictionMap{}
	predictionRepo = influxdb_repository_preditcion.NewContainerRepositoryWithConfig(i.influxDBConfig)

	influxDBContainerPredictionEntities, err = predictionRepo.ListContainerPredictionsByRequest(request)
	if err != nil {
		return podsPredictionMap, errors.New("list pod prediction failed: " + err.Error())
	}

	for _, entity := range influxDBContainerPredictionEntities {

		containerPrediction := entity.ContainerPrediction()
		podsPredictionMap.AddContainerPrediction(&containerPrediction)
	}

	return podsPredictionMap, nil
}

// CreateNodePredictions Implementation of prediction dao interface
func (i influxDB) CreateNodePredictions(nodePredictions []*prediction.NodePrediction) error {

	var (
		err error

		predictionRepo *influxdb_repository_preditcion.NodeRepository
	)

	predictionRepo = influxdb_repository_preditcion.NewNodeRepositoryWithConfig(i.influxDBConfig)

	err = predictionRepo.CreateNodePrediction(nodePredictions)
	if err != nil {
		return errors.New("create node prediction failed: ")
	}

	return nil
}

// ListNodePredictions Implementation of prediction dao interface
func (i influxDB) ListNodePredictions(request prediction.ListNodePredictionsRequest) (*prediction.NodesPredictionMap, error) {

	var (
		err error

		predictionRepo                 *influxdb_repository_preditcion.NodeRepository
		influxDBNodePredictionEntities []*influxdb_node_preditcion_entity.Entity
		nodesPredictionMap             *prediction.NodesPredictionMap
	)

	nodesPredictionMap = &prediction.NodesPredictionMap{}
	predictionRepo = influxdb_repository_preditcion.NewNodeRepositoryWithConfig(i.influxDBConfig)

	influxDBNodePredictionEntities, err = predictionRepo.ListNodePredictionsByRequest(request)
	if err != nil {
		return nodesPredictionMap, errors.New("create container prediction failed: ")
	}

	for _, entity := range influxDBNodePredictionEntities {
		nodePrediction := entity.NodePrediction()
		nodesPredictionMap.AddNodePrediction(&nodePrediction)
	}

	return nodesPredictionMap, nil
}
