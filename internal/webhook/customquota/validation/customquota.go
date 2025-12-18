// Copyright 2020-2025 Project Capsule Authors
// SPDX-License-Identifier: Apache-2.0

package validation

import (
	"context"
	"errors"
	"fmt"
	"go/types"
	"slices"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	capsulev1beta2 "github.com/projectcapsule/capsule/api/v1beta2"
	"github.com/projectcapsule/capsule/internal/controllers/customquotas"
	capsulewebhook "github.com/projectcapsule/capsule/internal/webhook"
)

type customquotasHandler struct {
	client client.Client
	log    logr.Logger
}

type pair struct {
	a *capsulev1beta2.CustomQuotaStatus
	b capsulev1beta2.CustomQuotaSpec
}

func CustomQuotasHandler(client client.Client, log logr.Logger) capsulewebhook.Handler {
	return &customquotasHandler{
		client: client,
		log:    log,
	}
}

func (h *customquotasHandler) OnCreate(c client.Client, decoder admission.Decoder, recorder record.EventRecorder) capsulewebhook.Func {
	return func(ctx context.Context, req admission.Request) *admission.Response {
		u, err := getUnstructured(req.Object)
		if err != nil {
			h.log.Error(err, fmt.Sprintf("error getting unstrutured: %v", err))

			return nil
		}

		response := h.updateCustomQuota(ctx, req, err, u)
		if response != nil {
			return response
		}

		response = h.updateClusterCustomQuota(ctx, req, err, u)
		if response != nil {
			return response
		}

		return nil
	}
}

func (h *customquotasHandler) OnDelete(c client.Client, _ admission.Decoder, recorder record.EventRecorder) capsulewebhook.Func {
	return func(ctx context.Context, req admission.Request) *admission.Response {
		obj, err := getUnstructured(req.OldObject)
		if err != nil {
			h.log.Error(err, fmt.Sprintf("error getting unstrutured: %v", err))

			return nil
		}

		customQuotasMatched, err := h.getCustomQuotaMatched(ctx, req, obj)
		if err != nil {
			h.log.Error(err, fmt.Sprintf("error getting matched CustomQuotas: %v", err))

			return nil
		}

		claim := fmt.Sprintf("%s.%s", req.Namespace, req.Name)

		for _, cq := range customQuotasMatched {
			claimList := cq.Status.Claims
			if !slices.Contains(claimList, claim) {
				continue
			}

			err = h.deleteResourceFromCustomQuota(ctx, obj, cq)
			if err != nil {
				h.log.Error(err, fmt.Sprintf("error deleting resource from CustomQuota %s: %v", cq.Name, err))
			}
		}

		//cluster
		clusterCustomQuotasMatched, err := h.getClusterCustomQuotaMatched(ctx, req, obj)
		if err != nil {
			h.log.Error(err, fmt.Sprintf("error getting matched CustomQuotas: %v", err))

			return nil
		}

		for _, cq := range clusterCustomQuotasMatched {
			claimList := cq.Status.Claims
			if !slices.Contains(claimList, claim) {
				continue
			}

			err = h.deleteResourceFromClusterCustomQuota(ctx, obj, cq)
			if err != nil {
				h.log.Error(err, fmt.Sprintf("error deleting resource from CustomQuota %s: %v", cq.Name, err))
			}
		}

		return nil
	}
}

func (h *customquotasHandler) OnUpdate(c client.Client, _ admission.Decoder, recorder record.EventRecorder) capsulewebhook.Func {
	return func(ctx context.Context, req admission.Request) *admission.Response {
		oldObj, errOldUnstructured := getUnstructured(req.OldObject)
		newObj, errNewUnstructured := getUnstructured(req.Object)
		customQuotasMatched, errOldMatch := h.getCustomQuotaMatched(ctx, req, oldObj)
		newCustomQuotasMatched, errNewMatch := h.getCustomQuotaMatched(ctx, req, newObj)

		err := errors.Join(errOldUnstructured, errNewUnstructured, errOldMatch, errNewMatch)
		if err != nil {
			h.log.Error(err, "error getting old and new unstructured or matched CustomQuotas")

			return nil
		}

		for _, cq := range customQuotasMatched {
			if !slices.ContainsFunc(newCustomQuotasMatched, func(quota capsulev1beta2.CustomQuota) bool {
				return cq.Name == quota.Name
			}) {
				err := h.deleteResourceFromCustomQuota(ctx, oldObj, cq)
				if err != nil {
					h.log.Error(err, fmt.Sprintf("error deleting resource from CustomQuota %s: %v", cq.Name, err))
				}

				continue
			}

			oldUsage, errOldUsage := customquotas.GetUsageFromUnstructured(oldObj, cq.Spec.Source.Path)
			newUsage, errNewUsage := customquotas.GetUsageFromUnstructured(newObj, cq.Spec.Source.Path)

			err = errors.Join(errOldUsage, errNewUsage)
			if err != nil {
				h.log.Error(err, fmt.Sprintf("error getting usage from object for CustomQuota %s: %v", cq.Name, err))

				continue
			}

			if oldUsage == newUsage {
				continue
			}

			newUsed := cq.Status.Used.DeepCopy()
			newUsed.Sub(resource.MustParse(oldUsage))
			newUsed.Add(resource.MustParse(newUsage))

			if newUsed.Cmp(cq.Spec.Limit) == 1 {
				response := admission.Denied(fmt.Sprintf("updating resource exceeds limit for CustomQuota %s", cq.Name))

				return &response
			}

			cq.Status.Used.Sub(resource.MustParse(oldUsage))
			cq.Status.Available.Add(resource.MustParse(oldUsage))
			cq.Status.Used.Add(resource.MustParse(newUsage))
			cq.Status.Available.Sub(resource.MustParse(newUsage))

			if err := h.client.Status().Update(ctx, &cq); err != nil {
				h.log.Error(err, fmt.Sprintf("error updating CustomQuota %s status: %v", cq.Name, err))
			}
		}

		//cluster
		clusterCustomQuotasMatched, errOldMatch := h.getClusterCustomQuotaMatched(ctx, req, oldObj)
		clusternewCustomQuotasMatched, errNewMatch := h.getClusterCustomQuotaMatched(ctx, req, newObj)

		err = errors.Join(errOldMatch, errNewMatch)
		if err != nil {
			h.log.Error(err, "error getting old and new unstructured or matched CustomQuotas")

			return nil
		}

		for _, cq := range clusterCustomQuotasMatched {
			if !slices.ContainsFunc(clusternewCustomQuotasMatched, func(quota capsulev1beta2.ClusterCustomQuota) bool {
				return cq.Name == quota.Name
			}) {
				err := h.deleteResourceFromClusterCustomQuota(ctx, oldObj, cq)
				if err != nil {
					h.log.Error(err, fmt.Sprintf("error deleting resource from CustomQuota %s: %v", cq.Name, err))
				}

				continue
			}

			oldUsage, errOldUsage := customquotas.GetUsageFromUnstructured(oldObj, cq.Spec.Source.Path)
			newUsage, errNewUsage := customquotas.GetUsageFromUnstructured(newObj, cq.Spec.Source.Path)

			err = errors.Join(errOldUsage, errNewUsage)
			if err != nil {
				h.log.Error(err, fmt.Sprintf("error getting usage from object for CustomQuota %s: %v", cq.Name, err))

				continue
			}

			if oldUsage == newUsage {
				continue
			}

			newUsed := cq.Status.Used.DeepCopy()
			newUsed.Sub(resource.MustParse(oldUsage))
			newUsed.Add(resource.MustParse(newUsage))

			if newUsed.Cmp(cq.Spec.Limit) == 1 {
				response := admission.Denied(fmt.Sprintf("updating resource exceeds limit for CustomQuota %s", cq.Name))

				return &response
			}

			cq.Status.Used.Sub(resource.MustParse(oldUsage))
			cq.Status.Available.Add(resource.MustParse(oldUsage))
			cq.Status.Used.Add(resource.MustParse(newUsage))
			cq.Status.Available.Sub(resource.MustParse(newUsage))

			if err := h.client.Status().Update(ctx, &cq); err != nil {
				h.log.Error(err, fmt.Sprintf("error updating CustomQuota %s status: %v", cq.Name, err))
			}
		}

		return nil
	}
}

func (h *customquotasHandler) updateCustomQuota(ctx context.Context, req admission.Request, err error, u unstructured.Unstructured) *admission.Response {
	customQuotasMatched, err := h.getCustomQuotaMatched(ctx, req, u)
	if err != nil {
		h.log.Error(err, fmt.Sprintf("error getting matched CustomQuotas: %v", err))
		return nil
	}

	clusterCustomQuotasMatched, err := h.getClusterCustomQuotaMatched(ctx, req, u)
	if err != nil {
		h.log.Error(err, fmt.Sprintf("error getting matched CustomQuotas: %v", err))
		return nil
	}

	cqs := make(map[string]pair)
	for _, cq := range customQuotasMatched {
		cqs[cq.Name+"."+cq.Namespace] = pair{
			a: &cq.Status,
			b: cq.Spec,
		}
	}
	for _, ccq := range clusterCustomQuotasMatched {
		cqs[ccq.Name+"."+ccq.Namespace] = pair{
			a: &ccq.Status,
			b: ccq.Spec.CustomQuotaSpec,
		}
	}

	var modifiedCq []*capsulev1beta2.CustomQuotaStatus

	for name, cq := range cqs {
		status := cq.a
		spec := cq.b
		claimList := status.Claims
		claimList = append(claimList, fmt.Sprintf("%s.%s", req.Namespace, req.Name))
		status.Claims = claimList

		usage, err := customquotas.GetUsageFromUnstructured(u, spec.Source.Path)
		if err != nil {
			h.log.Error(err, fmt.Sprintf("error getting usage from object for CustomQuota %s: %v", name, err))

			continue
		}

		newUsed := status.Used.DeepCopy()
		newUsed.Add(resource.MustParse(usage))

		if newUsed.Cmp(spec.Limit) == 1 {
			response := admission.Denied(fmt.Sprintf("updating resource exceeds limit for CustomQuota %s", name))

			return &response
		}

		status.Used.Add(resource.MustParse(usage))
		status.Available.Sub(resource.MustParse(usage))

		modifiedCq = append(modifiedCq, status)
	}

	for _, cq := range customQuotasMatched {
		for _, modified := range modifiedCq {
			if &cq.Status == modified {
				if err := h.client.Status().Update(ctx, &cq); err != nil {
					h.log.Error(err, fmt.Sprintf("error updating CustomQuota %s status: %v", cq.Name, err))
				}
			}
		}
	}

	for _, ccq := range clusterCustomQuotasMatched {
		for _, modified := range modifiedCq {
			if &ccq.Status == modified {
				if err := h.client.Status().Update(ctx, &ccq); err != nil {
					h.log.Error(err, fmt.Sprintf("error updating CustomQuota %s status: %v", ccq.Name, err))
				}
			}
		}
	}
	return nil
}

func (h *customquotasHandler) updateClusterCustomQuota(ctx context.Context, req admission.Request, err error, u unstructured.Unstructured) *admission.Response {
	customQuotasMatched, err := h.getClusterCustomQuotaMatched(ctx, req, u)
	if err != nil {
		h.log.Error(err, fmt.Sprintf("error getting matched CustomQuotas: %v", err))
		return nil
	}

	for _, cq := range customQuotasMatched {
		claimList := cq.Status.Claims
		claimList = append(claimList, fmt.Sprintf("%s.%s", req.Namespace, req.Name))
		cq.Status.Claims = claimList

		usage, err := customquotas.GetUsageFromUnstructured(u, cq.Spec.Source.Path)
		if err != nil {
			h.log.Error(err, fmt.Sprintf("error getting usage from object for CustomQuota %s: %v", cq.Name, err))

			continue
		}

		newUsed := cq.Status.Used.DeepCopy()
		newUsed.Add(resource.MustParse(usage))

		if newUsed.Cmp(cq.Spec.Limit) == 1 {
			response := admission.Denied(fmt.Sprintf("updating resource exceeds limit for CustomQuota %s", cq.Name))

			return &response
		}

		cq.Status.Used.Add(resource.MustParse(usage))
		cq.Status.Available.Sub(resource.MustParse(usage))

		if err := h.client.Status().Update(ctx, &cq); err != nil {
			h.log.Error(err, fmt.Sprintf("error updating CustomQuota %s status: %v", cq.Name, err))
		}
	}
	return nil
}

func (h *customquotasHandler) deleteResourceFromCustomQuota(ctx context.Context, obj unstructured.Unstructured, cq capsulev1beta2.CustomQuota) error {
	claim := fmt.Sprintf("%s.%s", obj.GetNamespace(), obj.GetName())
	claimList := cq.Status.Claims
	claimList = slices.Delete(claimList, slices.Index(claimList, claim), slices.Index(claimList, claim)+1)
	cq.Status.Claims = claimList

	usage, err := customquotas.GetUsageFromUnstructured(obj, cq.Spec.Source.Path)
	if err != nil {
		return fmt.Errorf("error getting usage from object for CustomQuota %s: %w", cq.Name, err)
	}

	cq.Status.Used.Sub(resource.MustParse(usage))
	cq.Status.Available.Add(resource.MustParse(usage))

	if err := h.client.Status().Update(ctx, &cq); err != nil {
		return fmt.Errorf("error updating CustomQuota %s status: %w", cq.Name, err)
	}

	return nil
}

func (h *customquotasHandler) deleteResourceFromClusterCustomQuota(ctx context.Context, obj unstructured.Unstructured, cq capsulev1beta2.ClusterCustomQuota) error {
	claim := fmt.Sprintf("%s.%s", obj.GetNamespace(), obj.GetName())
	claimList := cq.Status.Claims
	claimList = slices.Delete(claimList, slices.Index(claimList, claim), slices.Index(claimList, claim)+1)
	cq.Status.Claims = claimList

	usage, err := customquotas.GetUsageFromUnstructured(obj, cq.Spec.Source.Path)
	if err != nil {
		return fmt.Errorf("error getting usage from object for CustomQuota %s: %w", cq.Name, err)
	}

	cq.Status.Used.Sub(resource.MustParse(usage))
	cq.Status.Available.Add(resource.MustParse(usage))

	if err := h.client.Status().Update(ctx, &cq); err != nil {
		return fmt.Errorf("error updating CustomQuota %s status: %w", cq.Name, err)
	}

	return nil
}

func (h *customquotasHandler) getCustomQuotaMatched(ctx context.Context, req admission.Request, u unstructured.Unstructured) ([]capsulev1beta2.CustomQuota, error) {
	list := &capsulev1beta2.CustomQuotaList{}
	if err := h.client.List(ctx, list, client.InNamespace(req.Namespace)); err != nil {
		return nil, err
	}

	var customQuotasMatched []capsulev1beta2.CustomQuota

	for _, cq := range list.Items {
		if cq.Spec.Source.Kind != req.Kind.Kind && cq.Spec.Source.Version != req.Kind.Version {
			continue
		}

		for _, selector := range cq.Spec.ScopeSelectors {
			sel, err := metav1.LabelSelectorAsSelector(&selector)
			if err != nil {
				h.log.Error(err, fmt.Sprintf("error converting custom selector: %v", err))

				continue
			}

			matches := sel.Matches(labels.Set(u.GetLabels()))
			if matches {
				customQuotasMatched = append(customQuotasMatched, cq)
			}
		}
	}

	return customQuotasMatched, nil
}

func (h *customquotasHandler) getClusterCustomQuotaMatched(ctx context.Context, req admission.Request, u unstructured.Unstructured) ([]capsulev1beta2.ClusterCustomQuota, error) {
	list := &capsulev1beta2.ClusterCustomQuotaList{}
	if err := h.client.List(ctx, list); err != nil {
		return nil, err
	}

	var customQuotasMatched []capsulev1beta2.ClusterCustomQuota

	for _, cq := range list.Items {
		if cq.Spec.Source.Kind != req.Kind.Kind && cq.Spec.Source.Version != req.Kind.Version {
			continue
		}

		namespaces := []string{}
		var err error

		if namespaces, err = customquotas.GetNamespacesMatchingSelectors(cq.Spec.Selectors, h.client); err != nil {
			h.log.Error(err, fmt.Sprintf("error getting namespaces matching selectors for ClusterCustomQuota %s: %v", cq.Name, err))
			continue
		}

		if !slices.Contains(namespaces, req.Namespace) {
			continue
		}

		for _, selector := range cq.Spec.ScopeSelectors {
			sel, err := metav1.LabelSelectorAsSelector(&selector)
			if err != nil {
				h.log.Error(err, fmt.Sprintf("error converting custom selector: %v", err))

				continue
			}

			matches := sel.Matches(labels.Set(u.GetLabels()))
			if matches {
				customQuotasMatched = append(customQuotasMatched, cq)
			}
		}
	}

	return customQuotasMatched, nil
}

func getUnstructured(rawExt runtime.RawExtension) (unstructured.Unstructured, error) {
	var (
		obj   runtime.Object
		scope conversion.Scope
	)

	err := runtime.Convert_runtime_RawExtension_To_runtime_Object(&rawExt, &obj, scope)
	if err != nil {
		return unstructured.Unstructured{}, err
	}

	innerObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return unstructured.Unstructured{}, err
	}

	u := unstructured.Unstructured{Object: innerObj}

	return u, err
}
