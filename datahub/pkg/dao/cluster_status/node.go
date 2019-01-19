package clusterstatus

import (
	datahub_api "github.com/containers-ai/api/datahub/resource/v1alpha2"
	datahub_v1alpha2 "github.com/containers-ai/api/datahub/v1alpha2"
)

// NodeOperation provides node measurement operations
type NodeOperation interface {
	AddNodes([]*datahub_api.Node) error
	DeleteNodes([]*datahub_api.Node) error
	UpdateNodes([]*datahub_v1alpha2.UpdateNodesRequest_UpdatedNode) error
	ListNodes(bool) ([]*datahub_api.Node, error)
}
