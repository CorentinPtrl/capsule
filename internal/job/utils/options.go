package utils

import (
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
