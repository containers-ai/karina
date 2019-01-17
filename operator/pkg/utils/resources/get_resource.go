package resources

import (
	"context"
	"fmt"

	autuscaling "github.com/containers-ai/karina/operator/pkg/apis/autoscaling/v1alpha1"
	logUtil "github.com/containers-ai/karina/pkg/utils/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	getResourcesScope = logUtil.RegisterScope("get_resource", "Get resource", 0)
)

// GetResource define resource list functions
type GetResource struct {
	client.Client
}

// NewGetResource return GetResource instance
func NewGetResource(client client.Client) *GetResource {
	return &GetResource{
		client,
	}
}

// GetPod returns pod
func (getResource *GetResource) GetPod(namespace, name string) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	err := getResource.getResource(pod, namespace, name)
	return pod, err
}

// GetDeployment returns deployment
func (getResource *GetResource) GetDeployment(namespace, name string) (*appsv1.Deployment, error) {
	deployment := &appsv1.Deployment{}
	err := getResource.getResource(deployment, namespace, name)
	return deployment, err
}

// GetScaler return  scaler
func (getResource *GetResource) GetScaler(namespace, name string) (*autuscaling.Scaler, error) {
	scaler := &autuscaling.Scaler{}
	err := getResource.getResource(scaler, namespace, name)
	return scaler, err
}

// GetRecommendation return Recommendation
func (getResource *GetResource) GetRecommendation(namespace, name string) (*autuscaling.Recommendation, error) {
	recommendation := &autuscaling.Recommendation{}
	err := getResource.getResource(recommendation, namespace, name)
	return recommendation, err
}

func (getResource *GetResource) getResource(resource runtime.Object, namespace, name string) error {
	if namespace == "" || name == "" {
		return fmt.Errorf("Namespace: %s or name: %s is empty", namespace, name)
	}
	if err := getResource.Get(context.TODO(),
		types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		},
		resource); err != nil {
		getResourcesScope.Debug(err.Error())
		return err
	}
	return nil
}
