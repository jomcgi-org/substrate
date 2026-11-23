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

package controlapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
)

// A template with fork_config starts its actors by forking a seed sandbox that
// each worker holds. A seed is a system Actor with the SEED role, pinned to one
// worker. A forked actor is an ordinary Actor with the FORK role, whose start
// step forks the seed on its worker (see ensureAteletForked).

type systemPrincipalKey struct{}

// withSystemPrincipal marks ctx as the control plane acting for itself. Only
// code in this package sets it, so a caller of the gRPC API cannot.
func withSystemPrincipal(ctx context.Context) context.Context {
	return context.WithValue(ctx, systemPrincipalKey{}, true)
}

func isSystemPrincipal(ctx context.Context) bool {
	v, _ := ctx.Value(systemPrincipalKey{}).(bool)
	return v
}

func forkRole(actor *ateapi.Actor) ateapi.ForkRole {
	return actor.GetStatus().GetFork().GetRole()
}

func isSeedActor(actor *ateapi.Actor) bool { return forkRole(actor) == ateapi.ForkRole_FORK_ROLE_SEED }
func isForkActor(actor *ateapi.Actor) bool { return forkRole(actor) == ateapi.ForkRole_FORK_ROLE_FORK }

// seedActorName is the name of the seed Actor of a template on a worker. It is
// a function of both, so the reconciler finds the seed without listing Actors.
func seedActorName(templateUID, workerName string) string {
	sum := sha256.Sum256([]byte(templateUID + "/" + workerName))
	return "seed-" + hex.EncodeToString(sum[:8])
}

// forkForCreate returns the fork state a new Actor of tmpl starts with, or nil
// when tmpl has no fork_config. A template with fork_config runs only for
// Actors in its own atespace: a seed serves one atespace, and the check keeps
// every forked actor of a seed in it.
func forkForCreate(ctx context.Context, actor *ateapi.Actor, tmpl *ateapi.ActorTemplate) (*ateapi.ActorFork, error) {
	if tmpl.GetForkConfig() == nil {
		return nil, nil
	}
	if !preview.IsEnabledFor(ctx, preview.GateForkSeed) {
		return nil, apierror.FailedPrecondition("ActorTemplate %s/%s uses fork_config, which needs the ForkSeed preview gate to be enabled on the server and the client to opt in to preview features",
			tmpl.GetMetadata().GetAtespace(), tmpl.GetMetadata().GetName())
	}
	if actor.GetMetadata().GetAtespace() != tmpl.GetMetadata().GetAtespace() {
		return nil, apierror.PermissionDenied("ActorTemplate %s/%s uses fork_config, so only Actors in atespace %q may use it",
			tmpl.GetMetadata().GetAtespace(), tmpl.GetMetadata().GetName(), tmpl.GetMetadata().GetAtespace())
	}
	if actor.GetSourceTag() != nil {
		return nil, apierror.InvalidArgument("an Actor of a template with fork_config has no snapshots, so it cannot be created from a source tag")
	}
	return &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_FORK}, nil
}

// refuseForkSuspendOrRevert refuses a suspend or revert of a seed or a forked
// actor. A forked actor has no snapshots, so there is nothing to suspend or
// revert to. A seed lives and dies with its worker process.
func refuseForkSuspendOrRevert(actor *ateapi.Actor, op string) error {
	switch forkRole(actor) {
	case ateapi.ForkRole_FORK_ROLE_SEED:
		return apierror.FailedPrecondition("Actor %s/%s is a seed, which cannot be %s", actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName(), op)
	case ateapi.ForkRole_FORK_ROLE_FORK:
		return apierror.FailedPrecondition("Actor %s/%s is a forked actor, which runs once and cannot be %s", actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName(), op)
	}
	return nil
}

// refuseSeedUserOp refuses a seed Actor's operation for anyone but the control
// plane.
func refuseSeedUserOp(ctx context.Context, actor *ateapi.Actor, op string) error {
	if isSeedActor(actor) && !isSystemPrincipal(ctx) {
		return apierror.PermissionDenied("Actor %s/%s is a seed, which only the control plane may %s", actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName(), op)
	}
	return nil
}

// checkForkResumable checks that actor may start from tmpl. A forked actor
// starts only once, from SUSPENDED, and only while the ForkSeed gate is on.
func checkForkResumable(ctx context.Context, actor *ateapi.Actor, tmpl *ateapi.ActorTemplate) error {
	if err := refuseSeedUserOp(ctx, actor, "resume"); err != nil {
		return err
	}
	role := forkRole(actor)
	if role == ateapi.ForkRole_FORK_ROLE_UNSPECIFIED {
		if tmpl.GetForkConfig() != nil {
			return apierror.FailedPrecondition("ActorTemplate %s/%s uses fork_config, so Actor %s/%s is not one it can start",
				tmpl.GetMetadata().GetAtespace(), tmpl.GetMetadata().GetName(), actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName())
		}
		return nil
	}
	if tmpl.GetForkConfig() == nil {
		return apierror.FailedPrecondition("ActorTemplate %s/%s has no fork_config", tmpl.GetMetadata().GetAtespace(), tmpl.GetMetadata().GetName())
	}
	if !preview.IsEnabled(preview.GateForkSeed) {
		return apierror.FailedPrecondition("starting Actor %s/%s needs the ForkSeed preview gate", actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName())
	}
	if state := actor.GetStatus().GetState(); state != ateapi.ActorState_ACTOR_STATE_SUSPENDED && state != ateapi.ActorState_ACTOR_STATE_RESUMING {
		return apierror.FailedPrecondition("Actor %s/%s is %s, and a forked actor starts only once", actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName(), state)
	}
	return nil
}

// checkForkUpdate checks an update of the stored actor. Nobody but the control
// plane updates a seed, and a forked actor keeps its template: a seed and its
// forks are built for one.
func checkForkUpdate(ctx context.Context, stored, updated *ateapi.Actor) error {
	if err := refuseSeedUserOp(ctx, stored, "update"); err != nil {
		return err
	}
	if forkRole(stored) == ateapi.ForkRole_FORK_ROLE_UNSPECIFIED {
		return nil
	}
	if stored.GetActorTemplate().GetAtespace() != updated.GetActorTemplate().GetAtespace() || stored.GetActorTemplate().GetName() != updated.GetActorTemplate().GetName() {
		return apierror.FailedPrecondition("Actor %s/%s is a forked actor, so its template cannot change", stored.GetMetadata().GetAtespace(), stored.GetMetadata().GetName())
	}
	return nil
}
