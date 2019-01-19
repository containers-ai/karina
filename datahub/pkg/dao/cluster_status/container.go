package clusterstatus

import (
	datahub_resource_api "github.com/containers-ai/api/datahub/resource/v1alpha2"
	datahub_v1alpha2 "github.com/containers-ai/api/datahub/v1alpha2"
)

// ContainerOperation provides container measurement operations
type ContainerOperation interface {
	AddPods([]*datahub_resource_api.Pod) error
	DeletePods([]*datahub_resource_api.Pod) error
	UpdatePods([]*datahub_v1alpha2.UpdatePodsRequest_UpdatedPod) error
	ListPredictedPods(string, string, bool) ([]*datahub_resource_api.Pod, error)
}
