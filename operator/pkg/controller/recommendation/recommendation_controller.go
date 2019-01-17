/*

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

package recommendation

import (
	"context"
	"fmt"
	"google.golang.org/genproto/googleapis/rpc/code"
	"strings"
	"time"

	datahub_resource_metadata_v1alpha2 "github.com/containers-ai/api/datahub/resource/metadata/v1alpha2"
	datahub_v1alpha2 "github.com/containers-ai/api/datahub/v1alpha2"
	"github.com/containers-ai/karina/operator"
	autoscalingv1alpha1 "github.com/containers-ai/karina/operator/pkg/apis/autoscaling/v1alpha1"
	recommendation_reconciler "github.com/containers-ai/karina/operator/pkg/reconciler/recommendation"
	scaler_reconciler "github.com/containers-ai/karina/operator/pkg/reconciler/scaler"
	utilsresource "github.com/containers-ai/karina/operator/pkg/utils/resources"
	logUtil "github.com/containers-ai/karina/pkg/utils/log"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var (
	recommendationScope = logUtil.RegisterScope("recommendation_controller", "recommendation", 0)
	cachedFirstSynced   = false
)

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new Recommendation Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileRecommendation{Client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("recommendation-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to Recommendation
	err = c.Watch(&source.Kind{Type: &autoscalingv1alpha1.Recommendation{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcileRecommendation{}

// ReconcileRecommendation reconciles a Recommendation object
type ReconcileRecommendation struct {
	client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a Recommendation object and makes changes based on the state read
// and what is in the Recommendation.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  The scaffolding writes
// a Deployment as an example
// Automatically generate RBAC rules to allow the Controller to read and write Deployments
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=autoscaling.federator.ai,resources=recommendations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling.federator.ai,resources=recommendations/status,verbs=get;update;patch
func (r *ReconcileRecommendation) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	if !cachedFirstSynced {
		time.Sleep(5 * time.Second)
	}
	cachedFirstSynced = true

	getResource := utilsresource.NewGetResource(r)
	listResources := utilsresource.NewListResources(r)

	if recommendation, err := getResource.GetRecommendation(request.Namespace, request.Name); err == nil {
		// Remove Resource if target is not existed
		for _, or := range recommendation.OwnerReferences {
			if *or.Controller && strings.ToLower(or.Kind) == "scaler" {
				if scalers, err := listResources.ListAllScaler(); err == nil {
					for _, scaler := range scalers {
						if scaler.GetUID() == or.UID {
							scalerReconciler := scaler_reconciler.NewReconciler(r, &scaler)
							if !scalerReconciler.HasPod(recommendation.Namespace, recommendation.Name) {
								recommendationScope.Infof(fmt.Sprintf("Recommendation (%s/%s) is already removed from Scaler (%s/%s)", request.Namespace, request.Name, scaler.Namespace, scaler.Name))
								if err = r.Delete(context.TODO(), recommendation); err != nil {
									recommendationScope.Error(err.Error())
								}
								return reconcile.Result{}, nil
							}
						}
					}
				} else {
					recommendationScope.Error(err.Error())
				}
			}
		}

		// Update Recommendation from datahub
		recommendationReconciler := recommendation_reconciler.NewReconciler(r, recommendation)
		if conn, err := grpc.Dial(operator.GetOperator().Config.Datahub.Address, grpc.WithInsecure()); err == nil {
			defer conn.Close()
			datahubServiceClnt := datahub_v1alpha2.NewDatahubServiceClient(conn)
			req := datahub_v1alpha2.ListPodRecommendationsRequest{
				NamespacedName: &datahub_resource_metadata_v1alpha2.NamespacedName{
					Namespace: recommendation.GetNamespace(),
					Name:      recommendation.GetName(),
				},
			}
			if podRecommendationsRes, err := datahubServiceClnt.ListPodRecommendations(context.Background(), &req); err == nil && len(podRecommendationsRes.GetPodRecommendations()) == 1 {
				if recommendation, err = recommendationReconciler.UpdateResourceRecommendation(podRecommendationsRes.GetPodRecommendations()[0]); err == nil {
					if err = r.Update(context.TODO(), recommendation); err != nil {
						recommendationScope.Error(err.Error())
					}
				}
			} else if err != nil {
				recommendationScope.Errorf("query ListPodRecommendations to datahub failed: %s", err.Error())
			} else if podRecommendationsRes.GetStatus() != nil {
				if podRecommendationsRes.GetStatus().GetCode() != int32(code.Code_OK) {
					recommendationScope.Errorf("receive status code %d from datahub ListPodRecommendations response: %s", podRecommendationsRes.GetStatus().GetCode(), podRecommendationsRes.GetStatus().GetMessage())
				}
			}
		} else {
			recommendationScope.Error(err.Error())
		}
	} else {
		recommendationScope.Error(err.Error())
	}

	return reconcile.Result{}, nil
}
