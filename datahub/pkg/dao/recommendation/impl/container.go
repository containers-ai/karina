package impl

import (
	influxdb_repository "github.com/containers-ai/karina/datahub/pkg/repository/influxdb"

	datahub_recommendation_v1alpha2 "github.com/containers-ai/api/datahub/recommendation/v1alpha2"
	datahub_resource_metadata_v1alpha2 "github.com/containers-ai/api/datahub/resource/metadata/v1alpha2"
	datahub_v1alpha2 "github.com/containers-ai/api/datahub/v1alpha2"
	influxdb_repository_recommendation "github.com/containers-ai/karina/datahub/pkg/repository/influxdb/recommendation"
	"github.com/containers-ai/karina/pkg/utils/log"
)

var (
	containerImplScope = log.RegisterScope("recommendation_container_dao_implement", "recommended container dao implement", 0)
)

// Container Implements ContainerOperation interface
type Container struct {
	InfluxDBConfig influxdb_repository.Config
}

// AddPodRecommendations add pod recommendations to database
func (container *Container) AddPodRecommendations(podRecommendations []*datahub_recommendation_v1alpha2.PodRecommendation) error {
	containerRepository := influxdb_repository_recommendation.NewContainerRepository(&container.InfluxDBConfig)
	return containerRepository.CreateContainerRecommendations(podRecommendations)
}

// ListPodRecommendations list pod recommendations
func (container *Container) ListPodRecommendations(podNamespacedName *datahub_resource_metadata_v1alpha2.NamespacedName, queryCondition *datahub_v1alpha2.QueryCondition) ([]*datahub_recommendation_v1alpha2.PodRecommendation, error) {
	containerRepository := influxdb_repository_recommendation.NewContainerRepository(&container.InfluxDBConfig)
	return containerRepository.ListContainerRecommendations(podNamespacedName, queryCondition)
}
