// Package v1alpha1 contains Conductor's Kubernetes desired-state API.
//
// The API deliberately separates permanent GitOps-owned workload definitions
// from short-lived operator/UI overrides. Controllers observe existing
// Deployments; they do not own or render their pod templates.
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
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
		&ModelDownload{}, &ModelDownloadList{},
		&DownloaderClass{}, &DownloaderClassList{},
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

// ModelDownloadOperation selects whether the downloader pulls or removes the
// pinned models from the shared cache.
type ModelDownloadOperation string

const (
	ModelDownloadEnsure ModelDownloadOperation = "Ensure"
	ModelDownloadDelete ModelDownloadOperation = "Delete"
)

// ModelDownloadPhase is deliberately small: the Job is the source of truth for
// progress, while this phase is the controller's bounded read model.
type ModelDownloadPhase string

const (
	ModelDownloadPending   ModelDownloadPhase = "Pending"
	ModelDownloadRunning   ModelDownloadPhase = "Running"
	ModelDownloadSucceeded ModelDownloadPhase = "Succeeded"
	ModelDownloadFailed    ModelDownloadPhase = "Failed"
	ModelDownloadCancelled ModelDownloadPhase = "Cancelled"
)

type PinnedModel struct {
	// +kubebuilder:validation:MinLength=1
	Repository string `json:"repository"`
	// +kubebuilder:validation:MinLength=1
	Revision string   `json:"revision"`
	Include  []string `json:"include,omitempty"`
}

type ModelDownloadRateLimit struct {
	// +kubebuilder:validation:Minimum=0
	BytesPerSecond int64 `json:"bytesPerSecond"`
}

type ModelDownloadWorkers struct {
	// +kubebuilder:validation:Minimum=1
	FileWorkers int32 `json:"fileWorkers,omitempty"`
	// +kubebuilder:validation:Minimum=1
	RepositoryWorkers int32 `json:"repositoryWorkers,omitempty"`
	// +kubebuilder:validation:Minimum=1
	DeleteWorkers int32 `json:"deleteWorkers,omitempty"`
}

type ModelDownloadVerification struct {
	ReceiptPath string `json:"receiptPath,omitempty"`
	Required    bool   `json:"required,omitempty"`
}

// ModelDownloadSpec contains only the typed downloader contract. It does not
// accept arbitrary Pod or Job manifests.
type ModelDownloadCache struct {
	// +kubebuilder:validation:MinLength=1
	PVC          string            `json:"pvc"`
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

type ModelDownloadSpec struct {
	// +kubebuilder:validation:Enum=Ensure;Delete
	Operation     ModelDownloadOperation `json:"operation"`
	DownloaderRef string                 `json:"downloaderRef"`
	Cache         ModelDownloadCache     `json:"cache"`
	Models        []PinnedModel          `json:"models"`
	RateLimit     ModelDownloadRateLimit `json:"rateLimit"`
	Workers       ModelDownloadWorkers   `json:"workers,omitempty"`
	// +kubebuilder:validation:Minimum=1
	ProgressIntervalSeconds int32                     `json:"progressIntervalSeconds,omitempty"`
	Verification            ModelDownloadVerification `json:"verification"`
}

type ModelDownloadStatus struct {
	// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Cancelled
	Phase                       ModelDownloadPhase `json:"phase,omitempty"`
	JobName                     string             `json:"jobName,omitempty"`
	ObservedGeneration          int64              `json:"observedGeneration,omitempty"`
	FilesCompleted              int64              `json:"filesCompleted,omitempty"`
	FilesTotal                  int64              `json:"filesTotal,omitempty"`
	BytesPersisted              int64              `json:"bytesPersisted,omitempty"`
	InstantaneousBytesPerSecond int64              `json:"instantaneousBytesPerSecond,omitempty"`
	AverageBytesPerSecond       int64              `json:"averageBytesPerSecond,omitempty"`
	ReceiptPath                 string             `json:"receiptPath,omitempty"`
	Conditions                  []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type ModelDownload struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ModelDownloadSpec   `json:"spec,omitempty"`
	Status            ModelDownloadStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ModelDownloadList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ModelDownload `json:"items"`
}

// DownloaderClass is cluster-scoped so namespaces can share a vetted
// downloader image. ModelDownload references it by name.
type DownloaderClassSpec struct {
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default=IfNotPresent
	ImagePullPolicy    corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
	ServiceAccountName string            `json:"serviceAccountName,omitempty"`
}

// +kubebuilder:object:root=true
type DownloaderClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              DownloaderClassSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true
type DownloaderClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DownloaderClass `json:"items"`
}
