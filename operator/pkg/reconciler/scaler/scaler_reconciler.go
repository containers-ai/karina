package scaler

import (
	"fmt"

	autoscaling_v1alpha1 "github.com/containers-ai/karina/operator/pkg/apis/autoscaling/v1alpha1"
	utils "github.com/containers-ai/karina/operator/pkg/utils"
	utilsresource "github.com/containers-ai/karina/operator/pkg/utils/resources"
	logUtil "github.com/containers-ai/karina/pkg/utils/log"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	scalerReconcilerScope = logUtil.RegisterScope("scaler_reconciler", "scaler_reconciler", 0)
)

// Reconciler reconciles Scaler object
type Reconciler struct {
	client client.Client
	scaler *autoscaling_v1alpha1.Scaler
}

// NewReconciler creates Reconciler object
func NewReconciler(client client.Client, scaler *autoscaling_v1alpha1.Scaler) *Reconciler {
	return &Reconciler{
		client: client,
		scaler: scaler,
	}
}

// HasDeployment checks the Scaler has the deployment or not
func (reconciler *Reconciler) HasDeployment(deploymentNS, deploymentName string) bool {
	key := utils.GetNamespacedNameKey(deploymentNS, deploymentName)
	_, ok := reconciler.scaler.Status.Controller.Deployments[autoscaling_v1alpha1.NamespacedName(key)]
	return ok
}

// HasPod checks the Scaler has the Pod or not
func (reconciler *Reconciler) HasPod(podNS, podName string) bool {
	for _, deployment := range reconciler.scaler.Status.Controller.Deployments {
		deploymentNS := deployment.Namespace
		for _, pod := range deployment.Pods {
			if deploymentNS == podNS && pod.Name == podName {
				return true
			}
		}
	}
	return false
}

// RemoveDeployment removes deployment from Controller of Scaler
func (reconciler *Reconciler) RemoveDeployment(deploymentNS, deploymentName string) *autoscaling_v1alpha1.Scaler {
	key := utils.GetNamespacedNameKey(deploymentNS, deploymentName)

	if _, ok := reconciler.scaler.Status.Controller.Deployments[autoscaling_v1alpha1.NamespacedName(key)]; ok {
		delete(reconciler.scaler.Status.Controller.Deployments, autoscaling_v1alpha1.NamespacedName(key))
		return reconciler.scaler
	}
	return reconciler.scaler
}

// InitController try to initialize Controller field of Scaler
func (reconciler *Reconciler) InitController() (scaler *autoscaling_v1alpha1.Scaler, needUpdated bool) {
	if reconciler.scaler.Status.Controller.Deployments == nil {
		reconciler.scaler.Status.Controller.Deployments = map[autoscaling_v1alpha1.NamespacedName]autoscaling_v1alpha1.Deployment{}
		return reconciler.scaler, true
	}
	return reconciler.scaler, false
}

// UpdateStatusByDeployment updates status by deployment
func (reconciler *Reconciler) UpdateStatusByDeployment(deployment *appsv1.Deployment) *autoscaling_v1alpha1.Scaler {
	ScalerNS := reconciler.scaler.GetNamespace()
	ScalerName := reconciler.scaler.GetName()

	listResources := utilsresource.NewListResources(reconciler.client)
	deploymentNS := deployment.GetNamespace()
	deploymentName := deployment.GetName()
	deploymentUID := deployment.GetUID()
	podsMap := map[autoscaling_v1alpha1.NamespacedName]autoscaling_v1alpha1.Pod{}
	deploymentsMap := reconciler.scaler.Status.Controller.Deployments
	if pods, err := listResources.ListPodsByDeployment(deploymentNS, deploymentName); err == nil && len(pods) > 0 {
		for _, pod := range pods {
			podNS := pod.GetNamespace()
			podName := pod.GetName()
			podUID := pod.GetUID()
			scalerReconcilerScope.Infof(fmt.Sprintf("Pod (%s/%s) belongs to Scaler (%s/%s).", podNS, podName, ScalerNS, ScalerName))
			Containers := []autoscaling_v1alpha1.Container{}
			for _, Container := range pod.Spec.Containers {
				Containers = append(Containers, autoscaling_v1alpha1.Container{
					Name: Container.Name,
				})
			}
			podsMap[utils.GetNamespacedNameKey(podNS, podName)] = autoscaling_v1alpha1.Pod{
				Name:       podName,
				UID:        string(podUID),
				Containers: Containers,
			}
		}
	}

	deploymentsMap[utils.GetNamespacedNameKey(deploymentNS, deploymentName)] = autoscaling_v1alpha1.Deployment{
		Namespace: deploymentNS,
		Name:      deploymentName,
		UID:       string(deploymentUID),
		Pods:      podsMap,
	}
	reconciler.scaler.Status.Controller.Deployments = deploymentsMap
	return reconciler.scaler
}
