package main

import (
	"fmt"
	"os"
	"time"

	conductorv1alpha1 "github.com/projectinitiative/conductor/pkg/apis/conductor/v1alpha1"
	conductorcontroller "github.com/projectinitiative/conductor/pkg/controller"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func runController() {
	ctrl.SetLogger(zap.New(zap.UseDevMode(envOr("CONDUCTOR_LOG_MODE", "production") == "development")))

	scheme := runtime.NewScheme()
	must(clientgoscheme.AddToScheme(scheme))
	must(conductorv1alpha1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		LeaderElection:          true,
		LeaderElectionID:        "conductor-controller.conductor.projectinitiative.io",
		LeaderElectionNamespace: envOr("CONDUCTOR_NAMESPACE", "llm-test"),
		LeaseDuration:           durationPtr(30 * time.Second),
		RenewDeadline:           durationPtr(20 * time.Second),
		RetryPeriod:             durationPtr(5 * time.Second),
		HealthProbeBindAddress:  ":8081",
		Metrics:                 metricsserver.Options{BindAddress: ":8080"},
		GracefulShutdownTimeout: durationPtr(20 * time.Second),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "conductor controller manager: %v\n", err)
		os.Exit(1)
	}

	reconciler := &conductorcontroller.WorkloadReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Clock:     clock.RealClock{},
		Recorder:  mgr.GetEventRecorderFor("conductor-controller"),
	}
	must(reconciler.SetupWithManager(mgr))
	modelDownloads := &conductorcontroller.ModelDownloadReconciler{
		Client: mgr.GetClient(),
		Clock:  clock.RealClock{},
	}
	must(modelDownloads.SetupWithManager(mgr))
	must(mgr.AddHealthzCheck("healthz", healthz.Ping))
	must(mgr.AddReadyzCheck("readyz", healthz.Ping))

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		fmt.Fprintf(os.Stderr, "conductor controller: %v\n", err)
		os.Exit(1)
	}
}

func durationPtr(v time.Duration) *time.Duration { return &v }

func must(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "conductor: %v\n", err)
		os.Exit(1)
	}
}
