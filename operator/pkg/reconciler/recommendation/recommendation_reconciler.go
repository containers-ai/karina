package recommendation

import (
	datahub_metric_v1alpha2 "github.com/containers-ai/api/datahub/metric/v1alpha2"
	datahub_recommendation_v1alpha2 "github.com/containers-ai/api/datahub/recommendation/v1alpha2"
	"github.com/containers-ai/karina/datahub/pkg/utils"
	autoscaling_v1alpha1 "github.com/containers-ai/karina/operator/pkg/apis/autoscaling/v1alpha1"
	logUtil "github.com/containers-ai/karina/pkg/utils/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	recommendationReconcilerScope = logUtil.RegisterScope("recommendation_reconciler", "recommendation_reconciler", 0)
)

// Reconciler reconciles Recommendation object
type Reconciler struct {
	client         client.Client
	recommendation *autoscaling_v1alpha1.Recommendation
}

// NewReconciler creates Reconciler object
func NewReconciler(client client.Client, recommendation *autoscaling_v1alpha1.Recommendation) *Reconciler {
	return &Reconciler{
		client:         client,
		recommendation: recommendation,
	}
}

// UpdateResourceRecommendation updates resource of Recommendation
func (reconciler *Reconciler) UpdateResourceRecommendation(podRecommendation *datahub_recommendation_v1alpha2.PodRecommendation) (*autoscaling_v1alpha1.Recommendation, error) {
	for containerIdx, container := range reconciler.recommendation.Spec.Containers {
		for _, containerRecommendation := range podRecommendation.ContainerRecommendations {
			if container.Name == containerRecommendation.Name {
				if container.Resources.Limits == nil {
					container.Resources.Limits = corev1.ResourceList{}
				}
				if container.Resources.Requests == nil {
					container.Resources.Requests = corev1.ResourceList{}
				}
				for metricType, limitRecommendation := range containerRecommendation.LimitRecommendations {
					if metricType == int32(datahub_metric_v1alpha2.MetricType_CPU_USAGE_SECONDS_PERCENTAGE) {
						cpuLimitTime := int64(0)
						for _, data := range limitRecommendation.Data {
							curNanoSec := utils.TimeStampToNanoSecond(data.Time)
							if numVal, err := utils.StringToInt64(data.NumValue); err == nil && curNanoSec > cpuLimitTime {
								container.Resources.Limits[corev1.ResourceCPU] = *resource.NewMilliQuantity(numVal, resource.DecimalSI)
								cpuLimitTime = curNanoSec
							} else if err != nil {
								recommendationReconcilerScope.Error(err.Error())
							}
						}
					} else if metricType == int32(datahub_metric_v1alpha2.MetricType_MEMORY_USAGE_BYTES) {
						memoryLimitTime := int64(0)
						for _, data := range limitRecommendation.Data {
							curNanoSec := utils.TimeStampToNanoSecond(data.Time)
							if numVal, err := utils.StringToInt64(data.NumValue); err == nil && curNanoSec > memoryLimitTime {
								container.Resources.Limits[corev1.ResourceMemory] = *resource.NewQuantity(numVal, resource.BinarySI)
								memoryLimitTime = curNanoSec
							} else if err != nil {
								recommendationReconcilerScope.Error(err.Error())
							}
						}
					}
				}
				for metricType, requestRecommendation := range containerRecommendation.RequestRecommendations {
					if metricType == int32(datahub_metric_v1alpha2.MetricType_CPU_USAGE_SECONDS_PERCENTAGE) {
						cpuRequestTime := int64(0)
						for _, data := range requestRecommendation.Data {
							curNanoSec := utils.TimeStampToNanoSecond(data.Time)
							if numVal, err := utils.StringToInt64(data.NumValue); err == nil && curNanoSec > cpuRequestTime {
								container.Resources.Requests[corev1.ResourceCPU] = *resource.NewMilliQuantity(numVal, resource.DecimalSI)
								cpuRequestTime = curNanoSec
							} else if err != nil {
								recommendationReconcilerScope.Error(err.Error())
							}
						}
					} else if metricType == int32(datahub_metric_v1alpha2.MetricType_MEMORY_USAGE_BYTES) {
						memoryRequestTime := int64(0)
						for _, data := range requestRecommendation.Data {
							curNanoSec := utils.TimeStampToNanoSecond(data.Time)
							if numVal, err := utils.StringToInt64(data.NumValue); err == nil && curNanoSec > memoryRequestTime {
								container.Resources.Requests[corev1.ResourceMemory] = *resource.NewQuantity(numVal, resource.BinarySI)
								memoryRequestTime = curNanoSec
							} else if err != nil {
								recommendationReconcilerScope.Error(err.Error())
							}
						}
					}
				}
			}
		}
		reconciler.recommendation.Spec.Containers[containerIdx] = container
	}
	return reconciler.recommendation, nil
}
