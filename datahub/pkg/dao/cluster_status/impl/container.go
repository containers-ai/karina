package impl

import (
	"errors"

	datahub_resource_v1alpha2 "github.com/containers-ai/api/datahub/resource/v1alpha2"
	datahub_v1alpha2 "github.com/containers-ai/api/datahub/v1alpha2"
	cluster_status_entity "github.com/containers-ai/karina/datahub/pkg/entity/influxdb/cluster_status"
	influxdb_repository "github.com/containers-ai/karina/datahub/pkg/repository/influxdb"
	influxdb_repository_cluster_status "github.com/containers-ai/karina/datahub/pkg/repository/influxdb/cluster_status"
	"github.com/containers-ai/karina/pkg/utils/log"
)

var (
	containerImplScope = log.RegisterScope("container_dao_implement", "container dao implement", 0)
)

// Container implements ContainerOperation interface
type Container struct {
	InfluxDBConfig influxdb_repository.Config
}

// AddPods adds pods
func (container *Container) AddPods(pods []*datahub_resource_v1alpha2.Pod) error {
	containerRepository := influxdb_repository_cluster_status.NewContainerRepository(&container.InfluxDBConfig)
	return containerRepository.CreateContainers(pods)
}

// DeletePods deletes pods
func (container *Container) DeletePods(pods []*datahub_resource_v1alpha2.Pod) error {
	pointsToDelete := []*cluster_status_entity.ContainerEntity{}
	containerRepository := influxdb_repository_cluster_status.NewContainerRepository(&container.InfluxDBConfig)
	if containersEntity, err := containerRepository.ListPodsContainers(pods); err != nil {
		return errors.New("delete pods failed: " + err.Error())
	} else {
		for _, containerEntity := range containersEntity {
			entity := *containerEntity

			trueValue := true
			entity.IsDeleted = &trueValue
			pointsToDelete = append(pointsToDelete, &entity)
		}
	}
	return containerRepository.UpdateContainers(pointsToDelete)
}

// UpdatePods updates pods
func (container *Container) UpdatePods(updatedPods []*datahub_v1alpha2.UpdatePodsRequest_UpdatedPod) error {
	pods := []*datahub_resource_v1alpha2.Pod{}
	for _, updatePod := range updatedPods {
		if updatePod.NamespacedName == nil || updatePod.NamespacedName.Namespace == "" || updatePod.NamespacedName.Name == "" {
			return errors.New("Namespace and name of pod are required")
		}
		pods = append(pods, &datahub_resource_v1alpha2.Pod{
			NamespacedName: updatePod.NamespacedName,
		})
	}
	pointsToUpdate := []*cluster_status_entity.ContainerEntity{}
	containerRepository := influxdb_repository_cluster_status.NewContainerRepository(&container.InfluxDBConfig)
	if containersEntity, err := containerRepository.ListPodsContainers(pods); err != nil {
		return errors.New("delete pods failed: " + err.Error())
	} else {
		for _, containerEntity := range containersEntity {
			for _, updatePod := range updatedPods {
				if updatePod.GetIsPredicted() != nil && (updatePod.NamespacedName.Namespace == *containerEntity.Namespace || updatePod.NamespacedName.Name == *containerEntity.PodName) {
					entity := *containerEntity
					isPredicted := updatePod.GetIsPredicted().GetIsPredicted()
					entity.IsPredicted = &isPredicted
					pointsToUpdate = append(pointsToUpdate, &entity)
					break
				}
			}
		}
	}
	return containerRepository.UpdateContainers(pointsToUpdate)
}

// ListPredictedPods lists predicted pods
func (container *Container) ListPredictedPods(scalerNS, scalerName string, isPredicted bool) ([]*datahub_resource_v1alpha2.Pod, error) {
	containerRepository := influxdb_repository_cluster_status.NewContainerRepository(&container.InfluxDBConfig)
	return containerRepository.ListPredictedContainers(scalerNS, scalerName, isPredicted)
}
