package gateway

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/projectinitiative/conductor/pkg/config"
	"github.com/projectinitiative/conductor/pkg/k8s"
	"github.com/projectinitiative/conductor/pkg/scheduler"
)

// Options configures the running Conductor service.
type Options struct {
	ListenAddr   string
	ProfilesPath string // path to the model-profiles models.yaml
	Namespace    string
	Interval     time.Duration // driver reconcile interval
	K8sClient    k8s.Client    // optional; if nil, best-effort in-cluster or Noop
	CallerID     func(r *http.Request) string
}

// Service wires the policy engine, the swap lifecycle, the HTTP gateway and a
// reconcile driver loop into one running component.
type Service struct {
	Engine   *scheduler.Engine
	Swapper  *k8s.Swapper
	Profiles *config.Result
	interval time.Duration
}

// Run loads profiles, builds the service, and serves the gateway until ctx is
// cancelled. The driver goroutine swaps the resident model when the policy says
// a different model should be loaded.
func Run(ctx context.Context, opts Options) error {
	if opts.ListenAddr == "" {
		opts.ListenAddr = ":8000"
	}
	if opts.Interval == 0 {
		opts.Interval = 10 * time.Second
	}
	profiles, err := config.LoadFile(opts.ProfilesPath)
	if err != nil {
		return err
	}

	client := opts.K8sClient
	if client == nil {
		ns := opts.Namespace
		if ns == "" {
			ns = "llm-test"
		}
		if c, cerr := k8s.NewInCluster(ns); cerr == nil {
			client = c
		} else {
			log.Printf("not in cluster (%v); using no-op k8s client", cerr)
			client = k8s.NoopClient{}
		}
	}

	engine := scheduler.NewEngine(profiles.Models, profiles.Backends)
	svc := &Service{
		Engine:   engine,
		Swapper:  k8s.NewSwapper(client),
		Profiles: profiles,
		interval: opts.Interval,
	}

	handler := NewHandler(engine, opts.CallerID)
	srv := &http.Server{
		Addr:         opts.ListenAddr,
		Handler:      handler,
		ReadTimeout:  120 * time.Second,
		WriteTimeout: 0,
	}

	go svc.drive(ctx)

	go func() {
		log.Printf("conductor gateway listening on %s", opts.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("gateway: %v", err)
		}
	}()

	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(sctx)
}

// drive is the reconcile loop: when the policy says a model should be loaded,
// perform the swap and mark it resident.
func (s *Service) drive(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcile()
		}
	}
}

func (s *Service) reconcile() {
	next := s.Engine.NextModel()
	if next == "" {
		return
	}
	if next == s.Engine.Resident() {
		return
	}
	log.Printf("swapping to model %q", next)
	to := s.target(next)
	var from *k8s.Target
	if cur := s.Engine.Resident(); cur != "" {
		from = s.target(cur)
	}
	if err := s.Swapper.Swap(from, to); err != nil {
		log.Printf("swap to %q failed: %v", next, err)
		return
	}
	s.Engine.StartTurn(next)
	log.Printf("model %q resident", next)
}

func (s *Service) target(model string) *k8s.Target {
	dep := s.Profiles.Deploys[model]
	head := s.Profiles.Heads[model]
	hp := s.Profiles.HealthPaths[model]
	return &k8s.Target{
		HeadDeploy:   dep.Head,
		WorkerDeploy: dep.Worker,
		Namespace:    "llm-test",
		HostIP:       head.HostIP,
		Port:         head.Port,
		HealthPath:   hp,
		ReadyTimeout: 30 * time.Minute,
	}
}
