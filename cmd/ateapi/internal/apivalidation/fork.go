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
	"strings"

	"go.ate.dev/substrate/api/ateapi/v1"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// secretEnvMarkers are substrings of the env names a fork template may not
// use. The environment of the template container is in the seed's memory, so
// every forked actor can read it: it must hold nothing secret. The check only
// catches the obvious names.
var secretEnvMarkers = []string{"TOKEN", "KEY", "SECRET", "PASSWORD"}

// ValidateCustom_ForkConfig_SeedResources requires both a cpu and a memory
// limit, which the scheduler books for the seed.
func ValidateCustom_ForkConfig_SeedResources(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateapi.ResourceRequirements) field.ErrorList {
	var errs field.ErrorList
	if value.GetLimits().GetCpuMillicores() == 0 {
		errs = append(errs, field.Required(fldPath.Child("limits", "cpu_millicores"), ""))
	}
	if value.GetLimits().GetMemoryBytes() == 0 {
		errs = append(errs, field.Required(fldPath.Child("limits", "memory_bytes"), ""))
	}
	return errs
}

// validateForkTemplate holds the rules that span the fork_config of a
// template and the rest of it. It does nothing for a template without
// fork_config.
//
// A seed boots from one root filesystem image and has no volumes, so the
// template has exactly one container and none. A forked actor is ready when its
// wakeup probe passes, and has no snapshots to restore, so golden snapshots
// are off.
func validateForkTemplate(fldPath *field.Path, tmpl *ateapi.ActorTemplate) field.ErrorList {
	fork := tmpl.GetForkConfig()
	if fork == nil {
		return nil
	}
	var errs field.ErrorList
	if len(tmpl.GetContainers()) != 1 {
		errs = append(errs, field.Invalid(fldPath.Child("containers"), len(tmpl.GetContainers()), "a template with fork_config needs exactly one container"))
	}
	for i, ctr := range tmpl.GetContainers() {
		if ctr.GetWakeupProbe() == nil {
			errs = append(errs, field.Required(fldPath.Child("containers").Index(i).Child("wakeup_probe"), "a template with fork_config needs a wakeup probe"))
		}
		// A seed runs with an empty capability set, so the template cannot add to it.
		if len(ctr.GetSecurityContext().GetCapabilities().GetAdd()) > 0 {
			errs = append(errs, field.Forbidden(fldPath.Child("containers").Index(i).Child("security_context", "capabilities", "add"),
				"a seed runs without capabilities"))
		}
		for j, env := range ctr.GetEnv() {
			if name := env.GetName(); hasSecretMarker(name) {
				errs = append(errs, field.Forbidden(fldPath.Child("containers").Index(i).Child("env").Index(j).Child("name"),
					"the template environment is visible to every forked actor and must hold no secret"))
			}
		}
	}
	if len(tmpl.GetVolumes()) > 0 {
		errs = append(errs, field.Forbidden(fldPath.Child("volumes"), "a template with fork_config has no volumes"))
	}
	if tmpl.GetSnapshotConfig().GetGoldenSnapshotConfig().GetMode() != ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED {
		errs = append(errs, field.Required(fldPath.Child("snapshot_config", "golden_snapshot_config", "mode"), "a template with fork_config must disable golden snapshots"))
	}
	return errs
}

func hasSecretMarker(name string) bool {
	upper := strings.ToUpper(name)
	for _, marker := range secretEnvMarkers {
		if strings.Contains(upper, marker) {
			return true
		}
	}
	return false
}
