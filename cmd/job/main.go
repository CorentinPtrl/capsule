// Copyright 2020-2025 Project Capsule Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	goflag "flag"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	flag "github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	utilsjob "github.com/projectcapsule/capsule/internal/job/utils"
	capmeta "github.com/projectcapsule/capsule/pkg/api/meta"
	apimisc "github.com/projectcapsule/capsule/pkg/api/misc"
	"github.com/projectcapsule/capsule/pkg/utils"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var cfg utilsjob.JobOptions

	var ()
	flag.StringVar(&cfg.TenantLabelKey, "tenant-label-key", capmeta.TenantLabel, "Namespace label key to read tenant value from (must exist).")
	flag.Float32Var(&cfg.Qps, "qps", 20, "Client QPS.")
	flag.IntVar(&cfg.Burst, "burst", 40, "Client burst.")
	flag.DurationVar(&cfg.Timeout, "timeout", 2*time.Minute, "Overall timeout for the run.")
	flag.StringArrayVar(&cfg.TargetLabelKeys, "target-label-key", []string{capmeta.ManagedByCapsuleLabel, capmeta.TenantLabel}, "Label key to set tenant value on (repeatable). Example: --target-label-key a --target-label-key b")
	flag.StringToStringVar(&cfg.ExtraLabels, "extra-label", map[string]string{}, "Extra label to set (key=value) (repeatable). Example: --extra-label a=b --extra-label c=d")
	flag.StringArrayVar(&cfg.ExcludeResources, "exclude-resource", []string{"events", "events.events.k8s.io"}, "Resource name to skip (repeatable). Example: --exclude-resource events --exclude-resource rolebindings")
	config.RegisterFlags(goflag.CommandLine)
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)

	flag.Parse()

	slices.Sort(cfg.TargetLabelKeys)
	slices.Compact(cfg.TargetLabelKeys)

	slices.Sort(cfg.ExcludeResources)
	slices.Compact(cfg.ExcludeResources)

	if cfg.TenantLabelKey == "" {
		return fmt.Errorf("tenant-label-key must not be empty")
	}

	if len(cfg.TargetLabelKeys) == 0 {
		return fmt.Errorf("at least one target label key must be provided (use --target-label-key)")
	}

	restCfg, err := config.GetConfig()
	if err != nil {
		return fmt.Errorf("build jobConfig: %w", err)
	}

	restCfg.QPS = cfg.Qps
	restCfg.Burst = cfg.Burst

	ctx, cancel := buildContext(cfg.Timeout)
	defer cancel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("scheme: %w", err)
	}

	crCli, err := crclient.New(restCfg, crclient.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}

	dc, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("dynamic: %w", err)
	}

	disc, err := discovery.NewDiscoveryClientForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("discovery: %w", err)
	}

	namespaces, err := listNamespacesWithLabelKey(ctx, crCli, cfg.TenantLabelKey)
	if err != nil {
		return fmt.Errorf("list namespaces: %w", err)
	}

	if len(namespaces) == 0 {
		fmt.Printf("No namespaces found with label key %q. Done.\n", cfg.TenantLabelKey)

		return nil
	}

	namespacedGVRs, err := discoverNamespacedResources(disc, cfg.ExcludeResources)
	if err != nil {
		return fmt.Errorf("discover resources: %w", err)
	}

	fmt.Printf("Found %d namespaces with %q\n", len(namespaces), cfg.TenantLabelKey)
	fmt.Printf("Discovered %d namespaced resource types (after exclusions)\n", len(namespacedGVRs))

	for _, ns := range namespaces {
		tenantVal := ns.Labels[cfg.TenantLabelKey]
		if strings.TrimSpace(tenantVal) == "" {
			fmt.Printf("WARN: namespace %q has empty %q; skipping\n", ns.Name, cfg.TenantLabelKey)

			continue
		}

		desired := buildDesiredLabels(cfg, tenantVal)

		fmt.Printf("\n== Namespace: %s (%s=%s) ==\n", ns.Name, cfg.TenantLabelKey, tenantVal)

		for _, gvr := range namespacedGVRs {
			ul, err := dc.Resource(gvr).Namespace(ns.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				if utils.IsUnsupportedAPI(err) {
					continue
				}

				continue
			}

			if len(ul.Items) == 0 {
				continue
			}

			for i := range ul.Items {
				name := ul.Items[i].GetName()

				err := wait.ExponentialBackoff(wait.Backoff{
					Duration: 50 * time.Millisecond,
					Factor:   2.0,
					Jitter:   0.1,
					Steps:    4,
				}, func() (bool, error) {
					if err := patchLabels(ctx, dc, gvr, ns.Name, name, desired); err != nil {
						if utils.IsUnsupportedAPI(err) {
							return true, nil
						}

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

	return nil
}

func buildContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.Background(), func() {}
	}

	return context.WithTimeout(context.Background(), timeout)
}

func discoverNamespacedResources(disc discovery.DiscoveryInterface, exclude []string) ([]schema.GroupVersionResource, error) {
	excludeSet := sets.New(exclude...)
	rl, err := disc.ServerPreferredResources()
	if err != nil {
		if rl == nil || !utils.IsUnsupportedAPI(err) {
			return nil, err
		}
	}

	var gvrs []schema.GroupVersionResource

	for _, r := range rl {
		gv, err := schema.ParseGroupVersion(r.GroupVersion)
		if err != nil {
			continue
		}

		for _, res := range r.APIResources {
			if !res.Namespaced {
				continue
			}

			if strings.Contains(res.Name, "/") {
				continue
			}

			if excludeSet.Has(res.Name) {
				continue
			}

			gvrs = append(gvrs, schema.GroupVersionResource{
				Group:    gv.Group,
				Version:  gv.Version,
				Resource: res.Name,
			})
		}
	}

	return gvrs, nil
}

func buildDesiredLabels(cfg utilsjob.JobOptions, tenantValue string) map[string]string {
	out := make(map[string]string, len(cfg.TargetLabelKeys)+len(cfg.ExtraLabels))

	for _, k := range cfg.TargetLabelKeys {
		out[k] = tenantValue
	}

	for k, v := range cfg.ExtraLabels {
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
	type metadata struct {
		Labels map[string]string `json:"labels,omitempty"`
	}

	type patch struct {
		Metadata metadata `json:"metadata"`
	}

	b, err := json.Marshal(patch{Metadata: metadata{Labels: labels}})
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

func listNamespacesWithLabelKey(ctx context.Context, c crclient.Client, key string) ([]corev1.Namespace, error) {
	selector := &apimisc.NamespaceSelector{
		LabelSelector: &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      key,
				Operator: metav1.LabelSelectorOpExists,
			}},
		},
	}

	return selector.GetMatchingNamespaces(ctx, c)
}
