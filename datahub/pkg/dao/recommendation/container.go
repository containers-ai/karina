package recommendation

import (
	datahub_recommendation_v1alpha2 "github.com/containers-ai/api/datahub/recommendation/v1alpha2"
	datahub_resource_v1alpha2 "github.com/containers-ai/api/datahub/resource/metadata/v1alpha2"
	datahub_v1alpha2 "github.com/containers-ai/api/datahub/v1alpha2"
)

// ContainerOperation defines container measurement operation of recommendation database
type ContainerOperation interface {
	AddPodRecommendations([]*datahub_recommendation_v1alpha2.PodRecommendation) error
	ListPodRecommendations(podNamespacedName *datahub_resource_v1alpha2.NamespacedName, queryCondition *datahub_v1alpha2.QueryCondition) ([]*datahub_recommendation_v1alpha2.PodRecommendation, error)
}
