/*
Copyright 2020 Dynatrace LLC.

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

package v1alpha2

import (
	dynatracev1alpha1 "github.com/Dynatrace/dynatrace-oneagent-operator/pkg/apis/dynatrace/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// OneAgentSpec defines the desired state of OneAgent
// +k8s:openapi-gen=true
type OneAgentSpec struct {
	dynatracev1alpha1.OneAgentSpec `json:",inline"`
}

// OneAgentStatus defines the observed state of OneAgent
// +k8s:openapi-gen=true
type OneAgentStatus struct {
	dynatracev1alpha1.OneAgentStatus `json:",inline"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// OneAgent is the Schema for the oneagents API
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=oneagents,scope=Namespaced
// +kubebuilder:storageversion
type OneAgent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   OneAgentSpec   `json:"spec,omitempty"`
	Status OneAgentStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// OneAgentList contains a list of OneAgent
type OneAgentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []OneAgent `json:"items"`
}

func init() {
	SchemeBuilder.Register(&OneAgent{}, &OneAgentList{})
}

// GetSpec returns the corresponding BaseOneAgentSpec for the instance's Spec.
func (oa *OneAgent) GetSpec() *dynatracev1alpha1.BaseOneAgentSpec {
	return &oa.Spec.BaseOneAgentSpec
}

// GetStatus returns the corresponding BaseOneAgentStatus for the instance's Status.
func (oa *OneAgent) GetStatus() *dynatracev1alpha1.BaseOneAgentStatus {
	return &oa.Status.BaseOneAgentStatus
}

// SetPhase sets the status phase on the OneAgent object
func (oa *OneAgentStatus) SetPhase(phase dynatracev1alpha1.OneAgentPhaseType) bool {
	upd := phase != oa.Phase
	oa.Phase = phase
	return upd
}

// SetPhaseOnError fills the phase with the Error value in case of any error
func (oa *OneAgentStatus) SetPhaseOnError(err error) bool {
	if err != nil {
		return oa.SetPhase(dynatracev1alpha1.Error)
	}
	return false
}

func (oa *OneAgent) GetOneAgentSpec() *dynatracev1alpha1.OneAgentSpec {
	return &oa.Spec.OneAgentSpec
}

func (oa *OneAgent) GetOneAgentStatus() *dynatracev1alpha1.OneAgentStatus {
	return &oa.Status.OneAgentStatus
}

var _ dynatracev1alpha1.BaseOneAgentDaemonSet = &OneAgent{}
