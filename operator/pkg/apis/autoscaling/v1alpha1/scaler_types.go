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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type predictEnable = bool
type policy = string
type NamespacedName = string

const (
	RecommendationPolicySTABLE  policy = "stable"
	RecommendationPolicyCOMPACT policy = "compact"
)

type Container struct {
	Name      string                      `json:"name" protobuf:"bytes,1,opt,name=name"`
	Resources corev1.ResourceRequirements `json:"resources,omitempty" protobuf:"bytes,2,opt,name=resources"`
}

type Pod struct {
	Name       string      `json:"name" protobuf:"bytes,2,opt,name=name"`
	UID        string      `json:"uid" protobuf:"bytes,3,opt,name=uid"`
	Containers []Container `json:"containers" protobuf:"bytes,4,opt,name=containers"`
}

type Deployment struct {
	Namespace string                 `json:"namespace" protobuf:"bytes,1,opt,name=namespace"`
	Name      string                 `json:"name" protobuf:"bytes,2,opt,name=name"`
	UID       string                 `json:"uid" protobuf:"bytes,3,opt,name=uid"`
	Pods      map[NamespacedName]Pod `json:"pods" protobuf:"bytes,4,opt,name=pods"`
}

type Controller struct {
	Deployments map[NamespacedName]Deployment `json:"deployments" protobuf:"bytes,1,opt,name=deployments"`
}

// ScalerSpec defines the desired state of Scaler
type ScalerSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	Selector *metav1.LabelSelector `json:"selector" protobuf:"bytes,1,opt,name=selector"`
	Enable   predictEnable         `json:"enable" protobuf:"bytes,2,opt,name=enable"`
	Policy   policy                `json:"policy,omitempty" protobuf:"bytes,3,opt,name=policy"`
}

// ScalerStatus defines the observed state of Scaler
type ScalerStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	Controller Controller `json:"controller,omitempty" protobuf:"bytes,4,opt,name=controller"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Scaler is the Schema for the scalers API
// +k8s:openapi-gen=true
type Scaler struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ScalerSpec   `json:"spec,omitempty"`
	Status ScalerStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ScalerList contains a list of Scaler
type ScalerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Scaler `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Scaler{}, &ScalerList{})
}
