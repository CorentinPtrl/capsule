package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	capmeta "github.com/projectcapsule/capsule/pkg/api/meta"
)

type config struct {
	tenantLabelKey   string
	targetLabelKeys  []string
	extraLabels      map[string]string
	excludeResources sets.Set[string] // resource names like "events", "rolebindings"
	qps              float32
	burst            int
	timeout          time.Duration
}

// stringSliceFlag supports repeated flags: --x a --x b
type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	if s == nil {
		return ""
	}
	return strings.Join(*s, ",")
}

func (s *stringSliceFlag) Set(v string) error {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	*s = append(*s, v)
	return nil
}

func main() {
	var (
		tenantKey  = flag.String("tenant-label-key", capmeta.TenantLabel, "Namespace label key to read tenant value from (must exist).")
		qps        = flag.Float64("qps", 20, "Client QPS.")
		burst      = flag.Int("burst", 40, "Client burst.")
		kubeconfig = flag.String("kubeconfig", "", "Path to kubeconfig (optional; if empty uses in-cluster).")
		timeout    = flag.Duration("timeout", 2*time.Minute, "Overall timeout for the run.")
	)

	targetKeys := stringSliceFlag{
		capmeta.ManagedByCapsuleLabel,
		capmeta.TenantLabel,
	}

	extraLabelArgs := stringSliceFlag{}
	excludeArgs := stringSliceFlag{
		"events",
		"events.events.k8s.io",
	}

	flag.Var(&targetKeys, "target-label-key", "Label key to set tenant value on (repeatable). Example: --target-label-key a --target-label-key b")
	flag.Var(&extraLabelArgs, "extra-label", "Extra label to set (key=value) (repeatable). Example: --extra-label a=b --extra-label c=d")
	flag.Var(&excludeArgs, "exclude-resource", "Resource name to skip (repeatable). Example: --exclude-resource events --exclude-resource rolebindings")

	flag.Parse()

	targetKeysArr := []string(targetKeys)

	slices.Sort(targetKeysArr)
	slices.Compact(targetKeysArr)

	excludeArgsArr := []string(excludeArgs)

	slices.Sort(excludeArgsArr)
	slices.Compact(excludeArgsArr)

	cfg := config{
		tenantLabelKey:   *tenantKey,
		targetLabelKeys:  targetKeysArr,
		extraLabels:      parseLabelsFromArgs([]string(extraLabelArgs)),
		excludeResources: sets.New[string](excludeArgsArr...),
		qps:              float32(*qps),
		burst:            *burst,
		timeout:          *timeout,
	}

	if cfg.tenantLabelKey == "" {
		log.Fatalf("tenant-label-key must not be empty")
	}
	if len(cfg.targetLabelKeys) == 0 {
		log.Fatalf("at least one target label key must be provided (use --target-label-key)")
	}

	restCfg, err := buildRESTConfig(*kubeconfig)
	if err != nil {
		log.Fatalf("build config: %v", err)
	}
	restCfg.QPS = cfg.qps
	restCfg.Burst = cfg.burst

	ctx := context.Background()
	if cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.timeout)
		defer cancel()
	}

	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("clientset: %v", err)
	}
	dc, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("dynamic: %v", err)
	}
	disc, err := discovery.NewDiscoveryClientForConfig(restCfg)
	if err != nil {
		log.Fatalf("discovery: %v", err)
	}

	namespaces, err := listNamespacesWithLabelKey(ctx, clientset, cfg.tenantLabelKey)
	if err != nil {
		log.Fatalf("list namespaces: %v", err)
	}
	if len(namespaces) == 0 {
		fmt.Printf("No namespaces found with label key %q. Done.\n", cfg.tenantLabelKey)
		return
	}

	// Discover namespaced resources once
	namespacedGVRs, err := discoverNamespacedResources(disc, cfg.excludeResources)
	if err != nil {
		log.Fatalf("discover resources: %v", err)
	}

	fmt.Printf("Found %d namespaces with %q\n", len(namespaces), cfg.tenantLabelKey)
	fmt.Printf("Discovered %d namespaced resource types (after exclusions)\n", len(namespacedGVRs))

	// Apply labels
	for _, ns := range namespaces {
		tenantVal := ns.Labels[cfg.tenantLabelKey]
		if strings.TrimSpace(tenantVal) == "" {
			fmt.Printf("WARN: namespace %q has empty %q; skipping\n", ns.Name, cfg.tenantLabelKey)
			continue
		}

		desired := buildDesiredLabels(cfg, tenantVal)

		fmt.Printf("\n== Namespace: %s (%s=%s) ==\n", ns.Name, cfg.tenantLabelKey, tenantVal)

		// Label all resources in namespace
		for _, gvr := range namespacedGVRs {
			ul, err := dc.Resource(gvr).Namespace(ns.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				// RBAC forbidden or resource not served in this cluster, etc.
				continue
			}
			if len(ul.Items) == 0 {
				continue
			}

			// Patch each object (bounded retry for conflicts)
			for i := range ul.Items {
				name := ul.Items[i].GetName()

				err := wait.ExponentialBackoff(wait.Backoff{
					Duration: 50 * time.Millisecond,
					Factor:   2.0,
					Jitter:   0.1,
					Steps:    4,
				}, func() (bool, error) {
					if err := patchLabels(ctx, dc, gvr, ns.Name, name, desired); err != nil {
						// Some dynamic errors are effectively "skip".
						if meta.IsNoMatchError(err) {
							return true, nil
						}
						// Retry on other errors (notably conflicts / transient apiserver issues)
						return false, nil
					}
					return true, nil
				})
				if err != nil {
					fmt.Printf("WARN: label %s/%s %s failed\n", ns.Name, gvr.Resource, name)
				}
			}
		}
	}

	fmt.Println("\nDone.")
}

func buildRESTConfig(kubeconfig string) (*rest.Config, error) {
	// 1. If explicitly provided → use it
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}

	// 2. Try default kubeconfig (local dev)
	if cfg, err := clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile); err == nil {
		return cfg, nil
	}

	// 3. Fallback to in-cluster
	return rest.InClusterConfig()
}

func listNamespacesWithLabelKey(ctx context.Context, cs *kubernetes.Clientset, key string) ([]corev1.Namespace, error) {
	// Label selector that checks existence: "key" means "exists".
	nsl, err := cs.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: key})
	if err != nil {
		return nil, err
	}
	out := make([]corev1.Namespace, 0, len(nsl.Items))
	for i := range nsl.Items {
		out = append(out, nsl.Items[i])
	}
	return out, nil
}

func discoverNamespacedResources(disc discovery.DiscoveryInterface, exclude sets.Set[string]) ([]schema.GroupVersionResource, error) {
	rl, err := disc.ServerPreferredResources()
	if err != nil {
		if rl == nil {
			return nil, err
		}
	}

	var gvrs []schema.GroupVersionResource
	seen := sets.New[string]()

	for _, r := range rl {
		gv, err := schema.ParseGroupVersion(r.GroupVersion)
		if err != nil {
			continue
		}
		for _, res := range r.APIResources {
			if !res.Namespaced {
				continue
			}
			// Skip subresources (contain '/')
			if strings.Contains(res.Name, "/") {
				continue
			}
			// Exclude by resource name
			if exclude.Has(res.Name) {
				continue
			}
			key := gv.String() + "/" + res.Name
			if seen.Has(key) {
				continue
			}
			seen.Insert(key)

			gvrs = append(gvrs, schema.GroupVersionResource{
				Group:    gv.Group,
				Version:  gv.Version,
				Resource: res.Name,
			})
		}
	}

	return gvrs, nil
}

func buildDesiredLabels(cfg config, tenantValue string) map[string]string {
	out := make(map[string]string, len(cfg.targetLabelKeys)+len(cfg.extraLabels))
	for _, k := range cfg.targetLabelKeys {
		out[k] = tenantValue
	}
	for k, v := range cfg.extraLabels {
		out[k] = v
	}
	return out
}

func patchLabels(
	ctx context.Context,
	dc dynamic.Interface,
	gvr schema.GroupVersionResource,
	namespace string,
	name string,
	labels map[string]string,
) error {
	// Use JSON merge patch with json.Marshal to avoid manual JSON construction.
	type metadata struct {
		Labels map[string]string `json:"labels,omitempty"`
	}
	type patch struct {
		Metadata metadata `json:"metadata"`
	}

	b, err := json.Marshal(patch{
		Metadata: metadata{Labels: labels},
	})
	if err != nil {
		return err
	}

	var ri dynamic.ResourceInterface
	if namespace == "" {
		ri = dc.Resource(gvr)
	} else {
		ri = dc.Resource(gvr).Namespace(namespace)
	}

	_, err = ri.Patch(ctx, name, types.MergePatchType, b, metav1.PatchOptions{})
	return err
}

func parseLabelsFromArgs(args []string) map[string]string {
	out := map[string]string{}
	for _, kv := range args {
		i := strings.Index(kv, "=")
		if i <= 0 {
			// skip invalid input for safety
			continue
		}
		k := strings.TrimSpace(kv[:i])
		v := strings.TrimSpace(kv[i+1:])
		if k != "" {
			out[k] = v
		}
	}
	return out
}
