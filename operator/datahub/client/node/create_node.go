package node

import (
	"context"
	"fmt"

	datahub_resource_v1alpha2 "github.com/containers-ai/api/datahub/resource/v1alpha2"
	datahub_v1alpha2 "github.com/containers-ai/api/datahub/v1alpha2"
	"github.com/containers-ai/karina/operator"
	logUtil "github.com/containers-ai/karina/pkg/utils/log"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
)

var (
	createNodeScope = logUtil.RegisterScope("create_node", "Create node.", 0)
)

// CreateNode creates predicted node to datahub
type CreateNode struct{}

// NewCreateNode return CreateNode instance
func NewCreateNode() *CreateNode {
	return &CreateNode{}
}

// CreateNode creates predicted node to datahub
func (createNode *CreateNode) CreateNode(nodeList []corev1.Node) error {
	nodes := []*datahub_resource_v1alpha2.Node{}
	for _, node := range nodeList {
		nodes = append(nodes, &datahub_resource_v1alpha2.Node{
			Name: node.GetName(),
		})
	}
	req := datahub_v1alpha2.CreateNodesRequest{
		Nodes: nodes,
	}
	conn, err := grpc.Dial(operator.GetOperator().Config.Datahub.Address, grpc.WithInsecure())

	if err != nil {
		createNodeScope.Error(err.Error())
		return err
	}

	defer conn.Close()
	datahubServiceClnt := datahub_v1alpha2.NewDatahubServiceClient(conn)
	reqRes, err := datahubServiceClnt.CreateNodes(context.Background(), &req)
	if err != nil {
		createNodeScope.Errorf("query CreateNodes to datahub failed: %s", err.Error())
		return err
	} else if reqRes.Code != int32(code.Code_OK) {
		createNodeScope.Errorf("receive status code %d from datahub CreateNodes response: %s", reqRes.GetCode(), reqRes.GetMessage())
		return fmt.Errorf("receive status code %d from datahub CreateNodes response: %s", reqRes.GetCode(), reqRes.GetMessage())
	}

	return nil
}
