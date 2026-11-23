// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apivalidation

import (
	"context"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func validForkConfig(mutations ...func(*ateapi.ForkConfig)) *ateapi.ForkConfig {
	cfg := &ateapi.ForkConfig{
		Mode:               ateapi.ForkMode_FORK_MODE_SEED_AND_FORK,
		SeedResources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 1<<30)},
		SingleUse:          true,
		MaxLifetimeSeconds: 60,
		PidsLimit:          64,
		LogBytesLimit:      1 << 20,
	}
	for _, m := range mutations {
		m(cfg)
	}
	return cfg
}

// validForkTemplate is a template that passes every fork rule.
func validForkActorTemplate(mutations ...func(*ateapi.ActorTemplate)) *ateapi.ActorTemplate {
	return validActorTemplate(append([]func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
		tmpl.ForkConfig = validForkConfig()
		tmpl.SnapshotConfig.GoldenSnapshotConfig = &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED}
		tmpl.Containers[0].WakeupProbe = &ateapi.WakeupProbe{HttpGet: &ateapi.HttpGetAction{Path: "/healthz", Port: 8080}, TimeoutSeconds: 10}
	}}, mutations...)...)
}

func TestValidateCreateActorTemplateRequest_ForkConfig(t *testing.T) {
	fork := field.NewPath("actor_template", "fork_config")
	tests := []struct {
		name   string
		gates  string
		optOut bool
		mutate []func(*ateapi.ActorTemplate)
		want   field.ErrorList
	}{{
		name:  "valid",
		gates: "ForkSeed",
	}, {
		name:  "valid with explicit no egress",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.ForkConfig.Egress = ateapi.ForkEgress_FORK_EGRESS_NONE
		}},
	}, {
		name: "gate off",
		want: field.ErrorList{field.Forbidden(fork, "")},
	}, {
		name:   "gate on, opted out",
		gates:  "ForkSeed",
		optOut: true,
		want:   field.ErrorList{field.Forbidden(fork, "")},
	}, {
		name:  "single_use false",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.ForkConfig.SingleUse = false
		}},
		want: field.ErrorList{field.Required(fork.Child("single_use"), "")},
	}, {
		name:  "gateway egress",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.ForkConfig.Egress = ateapi.ForkEgress_FORK_EGRESS_GATEWAY
		}},
		want: field.ErrorList{field.NotSupported[string](fork.Child("egress"), "", nil)},
	}, {
		name:  "missing mode",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.ForkConfig.Mode = ateapi.ForkMode_FORK_MODE_UNSPECIFIED
		}},
		want: field.ErrorList{field.Required(fork.Child("mode"), "")},
	}, {
		name:  "missing seed resources",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.ForkConfig.SeedResources = nil
		}},
		want: field.ErrorList{field.Required(fork.Child("seed_resources"), "")},
	}, {
		name:  "seed resources without memory",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.ForkConfig.SeedResources = &ateapi.ResourceRequirements{Limits: &ateapi.Resources{CpuMillicores: 1000}}
		}},
		want: field.ErrorList{field.Required(fork.Child("seed_resources", "limits", "memory_bytes"), "")},
	}, {
		name:  "max lifetime too long",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.ForkConfig.MaxLifetimeSeconds = 3601
		}},
		want: field.ErrorList{field.Invalid(fork.Child("max_lifetime_seconds"), 3601, "").WithOrigin("maximum")},
	}, {
		name:  "pids limit too low",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.ForkConfig.PidsLimit = 8
		}},
		want: field.ErrorList{field.Invalid(fork.Child("pids_limit"), 8, "").WithOrigin("minimum")},
	}, {
		name:  "missing log bytes limit",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.ForkConfig.LogBytesLimit = 0
		}},
		want: field.ErrorList{field.Required(fork.Child("log_bytes_limit"), "")},
	}, {
		name:  "two containers",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			second := &ateapi.Container{Name: "side", Image: tmpl.Containers[0].Image, WakeupProbe: tmpl.Containers[0].WakeupProbe}
			tmpl.Containers = append(tmpl.Containers, second)
		}},
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template", "containers"), 2, "")},
	}, {
		name:  "no wakeup probe",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = nil
		}},
		want: field.ErrorList{field.Required(field.NewPath("actor_template", "containers").Index(0).Child("wakeup_probe"), "")},
	}, {
		name:  "volumes",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{Name: "scratch", DurableDir: &ateapi.DurableDirVolumeSource{}}}
		}},
		want: field.ErrorList{field.Forbidden(field.NewPath("actor_template", "volumes"), "")},
	}, {
		name:  "golden snapshots on",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.GoldenSnapshotConfig = nil
		}},
		want: field.ErrorList{field.Required(field.NewPath("actor_template", "snapshot_config", "golden_snapshot_config", "mode"), "")},
	}, {
		name:  "secret env name",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Env = []*ateapi.EnvVar{{Name: "Api_Token", Value: "x"}}
		}},
		want: field.ErrorList{field.Forbidden(field.NewPath("actor_template", "containers").Index(0).Child("env").Index(0).Child("name"), "")},
	}, {
		name:  "added capabilities",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].SecurityContext = &ateapi.SecurityContext{Capabilities: &ateapi.Capabilities{Add: []string{"CHOWN"}}}
		}},
		want: field.ErrorList{field.Forbidden(field.NewPath("actor_template", "containers").Index(0).Child("security_context", "capabilities", "add"), "")},
	}, {
		name:  "plain env name",
		gates: "ForkSeed",
		mutate: []func(*ateapi.ActorTemplate){func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Env = []*ateapi.EnvVar{{Name: "LOG_LEVEL", Value: "debug"}}
		}},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setPreviewForTest(t, tt.gates)
			ctx := context.Background()
			if tt.optOut {
				ctx = preview.ContextWithOptOut(ctx)
			}
			req := &ateapi.CreateActorTemplateRequest{ActorTemplate: validForkActorTemplate(tt.mutate...)}
			assertValidateErr(t, ValidateCreateActorTemplateRequest(ctx, req), tt.want)
		})
	}
}

// TestValidateCreateActorTemplateRequest_NoForkConfig shows a template
// without fork_config is not subject to the fork rules, with the gate on or off.
func TestValidateCreateActorTemplateRequest_NoForkConfig(t *testing.T) {
	for _, gates := range []string{"", "ForkSeed", "ALL"} {
		t.Run("gates="+gates, func(t *testing.T) {
			setPreviewForTest(t, gates)
			req := &ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
				tmpl.Containers = append(tmpl.Containers, &ateapi.Container{Name: "side", Image: tmpl.Containers[0].Image})
				tmpl.Volumes = []*ateapi.Volume{{Name: "scratch", DurableDir: &ateapi.DurableDirVolumeSource{}}}
			})}
			assertValidateErr(t, ValidateCreateActorTemplateRequest(context.Background(), req), nil)
		})
	}
}
