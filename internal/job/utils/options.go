package utils

import (
	"slices"
	"time"
)

type JobOptions struct {
	TenantLabelKey   string
	TargetLabelKeys  []string
	ExtraLabels      map[string]string
	ExcludeResources []string
	Qps              float32
	Burst            int
	Timeout          time.Duration
}

func (cfg JobOptions) Compact() {
	slices.Sort(cfg.TargetLabelKeys)
	cfg.TargetLabelKeys = slices.Compact(cfg.TargetLabelKeys)

	slices.Sort(cfg.ExcludeResources)
	cfg.ExcludeResources = slices.Compact(cfg.ExcludeResources)
}
