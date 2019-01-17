package resources

import (
	"context"
	"fmt"
	"strings"

	autuscaling "github.com/containers-ai/karina/operator/pkg/apis/autoscaling/v1alpha1"
	logUtil "github.com/containers-ai/karina/pkg/utils/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	listResourcesScope = logUtil.RegisterScope("list_resources", "List resources", 0)
)

// ListResources define resource list functions
type ListResources struct {
	client client.Client
}

// NewListResources return ListResources instance
func NewListResources(client client.Client) *ListResources {
	return &ListResources{
		client: client,
	}
}

// ListAllNodes return all nodes in cluster
func (listResources *ListResources) ListAllNodes() ([]corev1.Node, error) {
	nodeList := &corev1.NodeList{}
	if err := listResources.listAllResources(nodeList); err != nil {
		return []corev1.Node{}, err
	}
	return nodeList.Items, nil
}

// ListPodsByLabels return pods by labels
func (listResources *ListResources) ListPodsByLabels(labels map[string]string) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := listResources.listResourcesByLabels(podList, labels); err != nil {
		return []corev1.Pod{}, err
	}

	return podList.Items, nil
}

// ListDeploymentsByLabels return deployments by labels
func (listResources *ListResources) ListDeploymentsByLabels(labels map[string]string) ([]appsv1.Deployment, error) {
	deploymentList := &appsv1.DeploymentList{}
	if err := listResources.listResourcesByLabels(deploymentList, labels); err != nil {
		return []appsv1.Deployment{}, err
	}

	return deploymentList.Items, nil
}

// ListPodsByDeployment return pods by deployment namespace and name
func (listResources *ListResources) ListPodsByDeployment(deployNS, deployName string) ([]corev1.Pod, error) {
	deploymentPods := []corev1.Pod{}
	podList := &corev1.PodList{}
	if err := listResources.listResourcesByNamespace(podList, deployNS); err != nil {
		return deploymentPods, err
	}

	for _, pod := range podList.Items {
		podName := pod.GetName()
		for _, or := range pod.GetOwnerReferences() {
			if *or.Controller && strings.ToLower(or.Kind) == "replicaset" && strings.HasPrefix(podName, fmt.Sprintf("%s-", deployName)) && strings.HasPrefix(podName, fmt.Sprintf("%s-", or.Name)) {
				deploymentPods = append(deploymentPods, pod)
				break
			}
		}
	}

	return deploymentPods, nil
}

// ListAllScaler return all nodes in cluster
func (listResources *ListResources) ListAllScaler() ([]autuscaling.Scaler, error) {
	scalerList := &autuscaling.ScalerList{}
	if err := listResources.listAllResources(scalerList); err != nil {
		return []autuscaling.Scaler{}, err
	}
	return scalerList.Items, nil
}

func (listResources *ListResources) listAllResources(resourceList runtime.Object) error {
	if err := listResources.client.List(context.TODO(),
		&client.ListOptions{},
		resourceList); err != nil {
		listResourcesScope.Debug(err.Error())
		return err
	}
	return nil
}

func (listResources *ListResources) listResourcesByNamespace(resourceList runtime.Object, namespace string) error {
	if err := listResources.client.List(context.TODO(),
		&client.ListOptions{
			Namespace: namespace,
		}, resourceList); err != nil {
		listResourcesScope.Debug(err.Error())
		return err
	}
	return nil
}

func (listResources *ListResources) listResourcesByLabels(resourceList runtime.Object, lbls map[string]string) error {
	if err := listResources.client.List(context.TODO(),
		client.MatchingLabels(lbls),
		resourceList); err != nil {
		listResourcesScope.Debug(err.Error())
		return err
	}
	return nil
}
