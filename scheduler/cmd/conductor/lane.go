// lane implements the operator surface:
//
//	conductor lane status
//	conductor lane deploy <lane>
//	conductor lane switch <to> [--from <lane>]
//	conductor lane drop <lane>
//	conductor lane alias <lane>
//	conductor lane reconcile [lane ...]
//
// Every mutating verb prints its plan and requires --yes to execute
// (--dry-run prints the plan without touching anything). Mutating runs hold
// a single-flight lock (ConfigMap lease) in the target namespace.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/user"
	"sort"
	"strings"
	"time"

	"github.com/projectinitiative/conductor/pkg/operator"
)

const laneUsage = `lane — model lane lifecycle operator

Usage:
  conductor lane status                                    show lanes, ranks, resident alias, issues
  conductor lane deploy <lane>     [--yes]                 bring a staged lane up (worker ranks first, head last)
  conductor lane switch <to>       [--yes] [--from lane]   drop resident, deploy target, move alias, verify
  conductor lane drop <lane>       [--yes]                 scale a lane down (head first)
  conductor lane alias <to>        [--yes]                 move the serving alias to <to>'s head
  conductor lane reconcile [lanes] [--yes]                 force-clear stuck pods; recreate unhealthy pairs

Flags:
  -n, --namespace NAME        target namespace (default llm-test, env CONDUCTOR_NAMESPACE)
      --context NAME          kubectl context override
      --alias-label KEY       serving alias label (default serving.homelab.io/alias)
      --alias-value VALUE     alias value to set (default dgx-spark)
      --stable-service NAME   stable Service to verify after switch (default dgx-spark-backend)
      --per-rank-wait DUR     per-rank wait budget (default 45m)
      --dry-run               print the plan only (implied unless --yes)
      --yes                   execute the plan
      --force                 steal a stale/foreign lane lock; bypass conflicts
      --from LANE             (switch) explicit source lane instead of alias detection

Examples:
  conductor lane status
  conductor lane switch glm53-full --dry-run      # preview the 3x cutover
  conductor lane switch glm53-full --yes          # perform it
  conductor lane reconcile --yes                  # today's stuck-pod fix, one command`

func runLane(args []string) error {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, laneUsage)
		return fmt.Errorf("lane: missing verb")
	}
	verb, rest := args[0], args[1:]

	fs := flag.NewFlagSet("lane "+verb, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	ns := fs.String("namespace", envOr("CONDUCTOR_NAMESPACE", operator.DefaultNamespace), "target namespace")
	fs.StringVar(ns, "n", *ns, "target namespace (shorthand)")
	kubeContext := fs.String("context", "", "kubectl context override")
	aliasLabel := fs.String("alias-label", operator.DefaultAliasLabel, "serving alias label key")
	aliasValue := fs.String("alias-value", "", "alias value to set (default: value already in use or dgx-spark)")
	stableSvc := fs.String("stable-service", operator.DefaultStableService, "stable Service to verify")
	perRankWait := fs.Duration("per-rank-wait", 45*time.Minute, "per-rank wait budget")
	dryRun := fs.Bool("dry-run", false, "print the plan only")
	yes := fs.Bool("yes", false, "execute the plan")
	force := fs.Bool("force", false, "steal stale/foreign lane lock")
	from := fs.String("from", "", "switch: explicit source lane")
	if err := fs.Parse(rest); err != nil {
		return err
	}

	opts := operator.Options{
		Namespace:     *ns,
		AliasLabel:    *aliasLabel,
		AliasValue:    *aliasValue,
		StableService: *stableSvc,
		PerRankWait:   *perRankWait,
		Out:           os.Stdout,
	}
	execute := *yes && !*dryRun
	k := operator.New(*kubeContext, !execute)

	switch verb {
	case "status", "ls":
		return laneStatus(k, opts)
	case "deploy", "drop", "alias", "switch", "reconcile":
		if !execute {
			fmt.Fprintln(os.Stderr, "PLAN ONLY — append --yes to execute, or --dry-run to silence this hint.")
			fmt.Fprintln(os.Stderr)
		}
	}

	var release func()
	if execute {
		var err error
		release, err = k.AcquireLock(opts.Namespace, lockOwner(), verb, *force)
		if err != nil {
			return err
		}
		defer release()
	}

	switch verb {
	case "deploy":
		if fs.NArg() != 1 {
			fmt.Fprintln(os.Stderr, laneUsage)
			return fmt.Errorf("deploy: exactly one <lane> required")
		}
		lanes, err := operator.Discover(k, opts.Namespace, opts.AliasLabel)
		if err != nil {
			return err
		}
		return operator.Deploy(k, lanes, fs.Arg(0), opts)
	case "drop":
		if fs.NArg() != 1 {
			fmt.Fprintln(os.Stderr, laneUsage)
			return fmt.Errorf("drop: exactly one <lane> required")
		}
		lanes, err := operator.Discover(k, opts.Namespace, opts.AliasLabel)
		if err != nil {
			return err
		}
		return operator.Drop(k, lanes, fs.Arg(0), opts)
	case "alias":
		if fs.NArg() != 1 {
			fmt.Fprintln(os.Stderr, laneUsage)
			return fmt.Errorf("alias: exactly one <lane> required")
		}
		lanes, err := operator.Discover(k, opts.Namespace, opts.AliasLabel)
		if err != nil {
			return err
		}
		return operator.MoveAlias(k, lanes, fs.Arg(0), opts)
	case "switch":
		if fs.NArg() != 1 {
			fmt.Fprintln(os.Stderr, laneUsage)
			return fmt.Errorf("switch: exactly one <to> lane required")
		}
		lanes, err := operator.Discover(k, opts.Namespace, opts.AliasLabel)
		if err != nil {
			return err
		}
		return operator.Switch(k, lanes, *from, fs.Arg(0), opts)
	case "reconcile":
		lanes, err := operator.Discover(k, opts.Namespace, opts.AliasLabel)
		if err != nil {
			return err
		}
		pods, err := k.ListPods(opts.Namespace)
		if err != nil {
			return err
		}
		owners, err := k.DeploymentOwners(opts.Namespace)
		if err != nil {
			return err
		}
		nodesReady, err := k.NodeReady()
		if err != nil {
			return err
		}
		_, err = operator.Reconcile(k, lanes, fs.Args(), pods, owners, nodesReady, opts)
		return err
	default:
		fmt.Fprintln(os.Stderr, laneUsage)
		return fmt.Errorf("lane: unknown verb %q", verb)
	}
}

// laneStatus prints lanes, rank states, the alias holder, and issues.
func laneStatus(k *operator.Kube, opts operator.Options) error {
	opts.Defaults()
	lanes, err := operator.Discover(k, opts.Namespace, opts.AliasLabel)
	if err != nil {
		return err
	}
	pods, err := k.ListPods(opts.Namespace)
	if err != nil {
		return err
	}
	owners, err := k.DeploymentOwners(opts.Namespace)
	if err != nil {
		return err
	}
	nodesReady, err := k.NodeReady()
	if err != nil {
		return err
	}

	resident := operator.AliasHolder(lanes, opts.AliasLabel)
	fmt.Printf("namespace %s — resident (alias %s): %s\n\n", opts.Namespace, opts.AliasLabel, orNone(resident))
	fmt.Printf("%-22s %-4s %-28s %5s %6s %5s  %s\n", "LANE", "RANK", "DEPLOYMENT", "SPEC", "READY", "PODS", "ISSUES")

	names := make([]string, 0, len(lanes))
	for n := range lanes {
		names = append(names, n)
	}
	sort.Strings(names)
	unhealthy := 0
	for _, name := range names {
		lane := lanes[name]
		for i, r := range lane.Ranks {
			a := operator.AssessLane(lane, pods, owners, nodesReady)
			issues := []string{}
			if len(a.ForceDeletes) > 0 {
				issues = append(issues, fmt.Sprintf("stuck/offnode:%d", len(a.ForceDeletes)))
			}
			if a.Recreate {
				issues = append(issues, "recreate:"+a.Reason)
			}
			if r.Replicas > 0 && r.Ready < 1 {
				issues = append(issues, "not-ready")
			}
			iss := strings.Join(issues, "; ")
			if iss != "" {
				unhealthy++
			}
			laneCol := ""
			if i == 0 {
				laneCol = name
				if name == resident {
					laneCol += " *"
				}
			}
			fmt.Printf("%-22s %-4d %-28s %5d %6d %5d  %s\n", laneCol, r.Rank, r.Name, r.Replicas, r.Ready, r.Current, iss)
		}
	}
	eps, err := k.Endpoints(opts.Namespace, opts.StableService)
	if err == nil {
		fmt.Printf("\nstable service %s endpoints: %s\n", opts.StableService, orNone(strings.Join(eps, ", ")))
	}
	if unhealthy > 0 {
		fmt.Printf("\n%d issue(s) — `conductor lane reconcile --yes` heals; preview with --dry-run first.\n", unhealthy)
	}
	return nil
}

func orNone(s string) string {
	if strings.TrimSpace(s) == "" {
		return "(none)"
	}
	return s
}

func lockOwner() string {
	host, _ := os.Hostname()
	usr := "unknown"
	if u, err := user.Current(); err == nil {
		usr = u.Username
	}
	return fmt.Sprintf("%s@%s pid%d", usr, host, os.Getpid())
}
