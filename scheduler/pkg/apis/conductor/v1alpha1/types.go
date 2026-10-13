// Package v1alpha1 contains Conductor's Kubernetes desired-state API.
//
// The API deliberately separates permanent GitOps-owned workload definitions
// from short-lived operator/UI overrides. Controllers observe existing
// Deployments; they do not own or render their pod templates.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	Group   = "conductor.projectinitiative.io"
	Version = "v1alpha1"
)

var (
	GroupVersion  = schema.GroupVersion{Group: Group, Version: Version}
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion,
		&ResourcePool{}, &ResourcePoolList{},
		&ManagedWorkload{}, &ManagedWorkloadList{},
		&WorkloadOverride{}, &WorkloadOverrideList{},
	)
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}

type ManagementMode string

const (
	ManagementObserve ManagementMode = "Observe"
	ManagementManage  ManagementMode = "Manage"
)

type DesiredState string

const (
	DesiredResident DesiredState = "Resident"
	DesiredStandby  DesiredState = "Standby"
	DesiredDisabled DesiredState = "Disabled"
)

// ResourcePool is a contention and failure domain. A pool begins with one
// exclusive slot, but the API leaves room for richer capacity accounting.
type ResourcePoolSpec struct {
	// +kubebuilder:validation:Enum=Observe;Manage
	ManagementMode ManagementMode       `json:"managementMode,omitempty"`
	Capacity       ResourcePoolCapacity `json:"capacity,omitempty"`
	NodeSelector   map[string]string    `json:"nodeSelector,omitempty"`
	Routing        *RoutingSpec         `json:"routing,omitempty"`
	Policy         PoolPolicy           `json:"policy,omitempty"`
}

type ResourcePoolCapacity struct {
	Slots int32 `json:"slots,omitempty"`
}

type PoolPolicy struct {
	NotReadyGracePeriod metav1.Duration `json:"notReadyGracePeriod,omitempty"`
	RequireFencing      *bool           `json:"requireFencing,omitempty"`
	DefaultMinDwell     metav1.Duration `json:"defaultMinDwell,omitempty"`
	Cooldown            metav1.Duration `json:"cooldown,omitempty"`
}

type RoutingSpec struct {
	ServiceName string            `json:"serviceName"`
	Selector    map[string]string `json:"selector,omitempty"`
}

type ResourcePoolStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	ActiveWorkload     string             `json:"activeWorkload,omitempty"`
	EffectiveWorkload  string             `json:"effectiveWorkload,omitempty"`
	Operation          *OperationStatus   `json:"operation,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type ResourcePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ResourcePoolSpec   `json:"spec,omitempty"`
	Status            ResourcePoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ResourcePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ResourcePool `json:"items"`
}

// ManagedWorkload is a permanent Git-managed entry. Milestone one implements
// only DeploymentSet: ordered references to existing Deployments.
type ManagedWorkloadSpec struct {
	PoolRef       string             `json:"poolRef"`
	DesiredState  DesiredState       `json:"desiredState,omitempty"`
	Priority      int32              `json:"priority,omitempty"`
	Paused        bool               `json:"paused,omitempty"`
	Driver        WorkloadDriver     `json:"driver"`
	Qualification *QualificationSpec `json:"qualification,omitempty"`
	Routing       *RoutingSpec       `json:"routing,omitempty"`
}

type WorkloadDriver struct {
	Type          string             `json:"type"`
	DeploymentSet *DeploymentSetSpec `json:"deploymentSet,omitempty"`
}

type DeploymentSetSpec struct {
	Members        []DeploymentMember `json:"members"`
	RecoveryPolicy string             `json:"recoveryPolicy,omitempty"`
}

type DeploymentMember struct {
	Name       string `json:"name"`
	Role       string `json:"role"`
	StartOrder int32  `json:"startOrder"`
	StopOrder  int32  `json:"stopOrder"`
	Replicas   int32  `json:"replicas,omitempty"`
	ReadyGate  string `json:"readyGate,omitempty"`
}

type QualificationSpec struct {
	ServiceName string          `json:"serviceName,omitempty"`
	Port        int32           `json:"port,omitempty"`
	Path        string          `json:"path,omitempty"`
	Timeout     metav1.Duration `json:"timeout,omitempty"`
}

type OperationStatus struct {
	ID            string      `json:"id,omitempty"`
	Phase         string      `json:"phase,omitempty"`
	Reason        string      `json:"reason,omitempty"`
	CurrentMember string      `json:"currentMember,omitempty"`
	StartedAt     metav1.Time `json:"startedAt,omitempty"`
	UpdatedAt     metav1.Time `json:"updatedAt,omitempty"`
}

type MemberStatus struct {
	Name       string `json:"name"`
	Node       string `json:"node,omitempty"`
	Replicas   int32  `json:"replicas,omitempty"`
	Ready      int32  `json:"ready,omitempty"`
	Available  int32  `json:"available,omitempty"`
	Generation int64  `json:"generation,omitempty"`
}

type ManagedWorkloadStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	EffectiveIntent    string             `json:"effectiveIntent,omitempty"`
	IntentSource       string             `json:"intentSource,omitempty"`
	Operation          *OperationStatus   `json:"operation,omitempty"`
	Members            []MemberStatus     `json:"members,omitempty"`
	ReadyEndpoints     int32              `json:"readyEndpoints,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type ManagedWorkload struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ManagedWorkloadSpec   `json:"spec,omitempty"`
	Status            ManagedWorkloadStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ManagedWorkloadList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ManagedWorkload `json:"items"`
}

// WorkloadOverride is the future CLI/web write surface. It is deliberately
// separate from ManagedWorkload so temporary intent never fights GitOps.
type WorkloadOverrideSpec struct {
	PoolRef     string `json:"poolRef"`
	WorkloadRef string `json:"workloadRef,omitempty"`
	// +kubebuilder:validation:Enum=Pin;Suspend;FreezePool;Automatic
	Mode           string       `json:"mode"`
	ExpiresAt      *metav1.Time `json:"expiresAt,omitempty"`
	Actor          string       `json:"actor,omitempty"`
	Reason         string       `json:"reason,omitempty"`
	IdempotencyKey string       `json:"idempotencyKey,omitempty"`
}

type WorkloadOverrideStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Effective          bool               `json:"effective,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type WorkloadOverride struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WorkloadOverrideSpec   `json:"spec,omitempty"`
	Status            WorkloadOverrideStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type WorkloadOverrideList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkloadOverride `json:"items"`
}
