/*
Copyright 2019 The Karina Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scaler

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"google.golang.org/genproto/googleapis/rpc/code"

	datahub_recommendation_v1alpha2 "github.com/containers-ai/api/datahub/recommendation/v1alpha2"
	datahub_resource_metadata_v1alpha2 "github.com/containers-ai/api/datahub/resource/metadata/v1alpha2"
	datahub_resource_v1alpha2 "github.com/containers-ai/api/datahub/resource/v1alpha2"
	datahub_v1alpha2 "github.com/containers-ai/api/datahub/v1alpha2"
	"github.com/containers-ai/karina/operator"
	autoscalingv1alpha1 "github.com/containers-ai/karina/operator/pkg/apis/autoscaling/v1alpha1"
	scaler_reconciler "github.com/containers-ai/karina/operator/pkg/reconciler/scaler"
	utilsresource "github.com/containers-ai/karina/operator/pkg/utils/resources"
	logUtil "github.com/containers-ai/karina/pkg/utils/log"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var (
	scope             = logUtil.RegisterScope("scaler_controller", "scaler log", 0)
	cachedFirstSynced = false
)

type scaler = string

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new Scaler Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileScaler{Client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("scaler-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		scope.Error(err.Error())
		return err
	}

	if err = c.Watch(&source.Kind{Type: &autoscalingv1alpha1.Scaler{}}, &handler.EnqueueRequestForObject{}); err != nil {
		scope.Error(err.Error())
	}

	if err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcileScaler{}

// ReconcileScaler reconciles a Scaler object
type ReconcileScaler struct {
	client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a Scaler object and makes changes based on the state read
// and what is in the Scaler.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  The scaffolding writes
// a Deployment as an example
// Automatically generate RBAC rules to allow the Controller to read and write Deployments
// +kubebuilder:rbac:groups=autoscaling.federator.ai,resources=scalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling.federator.ai,resources=scalers/status,verbs=get;update;patch
func (r *ReconcileScaler) Reconcile(request reconcile.Request) (reconcile.Result, error) {

	if !cachedFirstSynced {
		time.Sleep(5 * time.Second)
	}
	cachedFirstSynced = true

	getResource := utilsresource.NewGetResource(r)
	listResources := utilsresource.NewListResources(r)
	updateResource := utilsresource.NewUpdateResource(r)

	if scaler, err := getResource.GetScaler(request.Namespace, request.Name); err != nil && errors.IsNotFound(err) {
	} else if err == nil {
		// TODO: deployment already in the Scaler cannot join the other
		scalerNS := scaler.GetNamespace()
		scalerName := scaler.GetName()
		scalerReconciler := scaler_reconciler.NewReconciler(r, scaler)
		if scaler, needUpdated := scalerReconciler.InitController(); needUpdated {
			updateResource.UpdateScaler(scaler)
		}

		scope.Infof(fmt.Sprintf("Scaler (%s/%s) found, try to sync latest controllers.", scalerNS, scalerName))
		if deployments, err := listResources.ListDeploymentsByLabels(scaler.Spec.Selector.MatchLabels); err == nil {
			for _, deployment := range deployments {
				if deployment.ObjectMeta.Namespace == request.Namespace {
					scaler = scalerReconciler.UpdateStatusByDeployment(&deployment)
				}
			}
			updateResource.UpdateScaler(scaler)
		}

		// after updating Pod in Scaler, start create Recommendation if necessary and register pod to datahub
		if conn, err := grpc.Dial(operator.GetOperator().Config.Datahub.Address, grpc.WithInsecure()); err == nil {
			defer conn.Close()
			datahubServiceClnt := datahub_v1alpha2.NewDatahubServiceClient(conn)
			pods := []*datahub_resource_v1alpha2.Pod{}
			policy := datahub_recommendation_v1alpha2.RecommendationPolicy_STABLE
			if strings.ToLower(string(scaler.Spec.Policy)) == strings.ToLower(string(autoscalingv1alpha1.RecommendationPolicyCOMPACT)) {
				policy = datahub_recommendation_v1alpha2.RecommendationPolicy_COMPACT
			} else if strings.ToLower(string(scaler.Spec.Policy)) == strings.ToLower(string(autoscalingv1alpha1.RecommendationPolicySTABLE)) {
				policy = datahub_recommendation_v1alpha2.RecommendationPolicy_STABLE
			}
			for _, scalerDeployment := range scaler.Status.Controller.Deployments {
				for _, pod := range scalerDeployment.Pods {
					containers := []*datahub_resource_v1alpha2.Container{}
					startTime := &timestamp.Timestamp{}
					for _, container := range pod.Containers {
						containers = append(containers, &datahub_resource_v1alpha2.Container{
							Name: container.Name,
						})
					}
					nodeName := ""
					if pod, err := getResource.GetPod(scalerDeployment.Namespace, pod.Name); err == nil {
						nodeName = pod.Spec.NodeName
						startTime = &timestamp.Timestamp{
							Seconds: pod.ObjectMeta.GetCreationTimestamp().Unix(),
						}
					} else {
						scope.Error(err.Error())
					}

					pods = append(pods, &datahub_resource_v1alpha2.Pod{
						IsPredicted: true,
						Scaler: &datahub_resource_metadata_v1alpha2.NamespacedName{
							Namespace: scalerNS,
							Name:      scalerName,
						},
						NamespacedName: &datahub_resource_metadata_v1alpha2.NamespacedName{
							Namespace: scalerDeployment.Namespace,
							Name:      pod.Name,
						},
						Policy:     datahub_recommendation_v1alpha2.RecommendationPolicy(policy),
						Containers: containers,
						NodeName:   nodeName,
						// TODO
						ResourceLink: "",
						StartTime:    startTime,
					})
					// try to create the recommendation by pod
					recommendationNS := scalerDeployment.Namespace
					recommendationName := pod.Name

					recommendation := &autoscalingv1alpha1.Recommendation{
						ObjectMeta: metav1.ObjectMeta{
							Name:      recommendationName,
							Namespace: recommendationNS,
							Labels: map[string]string{
								"scaler": fmt.Sprintf("%s.%s", scaler.GetName(), scaler.GetNamespace()),
							},
						},
						Spec: autoscalingv1alpha1.RecommendationSpec{
							Containers: pod.Containers,
						},
					}

					if err := controllerutil.SetControllerReference(scaler, recommendation, r.scheme); err == nil {
						_, err := getResource.GetRecommendation(recommendationNS, recommendationName)
						if err != nil && errors.IsNotFound(err) {
							err = r.Create(context.TODO(), recommendation)
							if err != nil {
								scope.Error(err.Error())
							}
						}
					}
				}
			}
			req := datahub_v1alpha2.CreatePodsRequest{
				Pods: pods,
			}
			resp, err := datahubServiceClnt.CreatePods(context.Background(), &req)
			if err != nil {
				scope.Error(err.Error())
			} else if resp != nil {
				if resp.GetCode() != int32(code.Code_OK) {
					scope.Errorf("receive status code %d from datahub CreatePods response: %s", resp.GetCode(), resp.GetMessage())
				} else {
					scope.Infof(fmt.Sprintf("Add/Update pods for Scaler (%s/%s) successfully", scaler.GetNamespace(), scaler.GetName()))
				}
			} else {
				scope.Error("receive nil status from datahub CreatePods response")
			}
		} else {
			scope.Error(err.Error())
		}
	} else {
		scope.Error(err.Error())
	}

	return reconcile.Result{}, nil
}
