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
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/scheduling"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
)

// updateWorkerAttempts bounds the retries of a write to a Worker that loses a
// race with another writer.
const updateWorkerAttempts = 5

// forkSchedulingConstraints adds what a seed or a forked actor needs from its
// worker to c, which is the constraints of an ordinary actor.
//
// A seed holds its template's seed_resources on the Worker it is pinned to. A
// forked actor needs a ready seed of its template, in its atespace, on the
// Worker.
func forkSchedulingConstraints(c scheduling.Constraints, actor *ateapi.Actor, tmpl *ateapi.ActorTemplate) scheduling.Constraints {
	switch forkRole(actor) {
	case ateapi.ForkRole_FORK_ROLE_SEED:
		c.Limits = tmpl.GetForkConfig().GetSeedResources().GetLimits()
		c.RequiredWorker = actor.GetStatus().GetFork().GetPinnedWorker().GetName()
	case ateapi.ForkRole_FORK_ROLE_FORK:
		c.RequiredSeed = &scheduling.SeedRequirement{
			Atespace:    actor.GetMetadata().GetAtespace(),
			TemplateUID: tmpl.GetMetadata().GetUid(),
		}
	}
	return c
}

// ensureAteletSeeded boots the seed sandbox of the template on the assigned
// worker. The seed is a system Actor, so there is no egress and no request
// ever reaches it.
func (w *ActorWorkflow) ensureAteletSeeded(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor, actorTemplate *ateapi.ActorTemplate) (tele restoreTelemetry, workerEpoch int64, err error) {
	ctx, done := stepSpan(ctx, "CallAteletSeed")
	defer func() { err = done(err) }()

	tele.SnapshotKind = ateattr.SnapshotKindBoot
	assignment := actor.GetStatus().GetWorkerAssignment()
	conn, err := w.dialer.DialForAteletOnNode(assignment.GetNodeName())
	if err != nil {
		return tele, 0, err
	}
	spec, err := workloadSpecFromActorTemplate(actorTemplate, actor, nil)
	if err != nil {
		return tele, 0, err
	}
	resp, err := ateletpb.NewAteletClient(conn).Seed(ctx, &ateletpb.SeedRequest{
		WorkerPodUid:          assignment.GetWorkerPodUid(),
		Atespace:              actor.GetMetadata().GetAtespace(),
		ActorName:             actor.GetMetadata().GetName(),
		ActorUid:              actor.GetMetadata().GetUid(),
		ActorTemplateAtespace: actor.GetActorTemplate().GetAtespace(),
		ActorTemplateName:     actor.GetActorTemplate().GetName(),
		TemplateUid:           actorTemplate.GetMetadata().GetUid(),
		Spec:                  spec,
		Resources:             toAteletResources(actorTemplate.GetForkConfig().GetSeedResources()),
		SandboxRuntime:        toAteletSandboxRuntime(assignment.GetSandboxRuntime()),
	})
	if err != nil {
		return tele, 0, handleAteletError(ctx, w.store, actorRef, ateattr.OperationResume, "Seed", false, err)
	}
	return tele, resp.GetWorkerEpoch(), nil
}

// forkEnv is the environment of a forked actor. It names the actor and
// carries nothing from a request: the sandbox parses it before its seccomp
// filter is on.
func forkEnv(actor *ateapi.Actor) []*ateletpb.EnvVar {
	return []*ateletpb.EnvVar{
		{Name: "ATE_ACTOR_ATESPACE", Value: actor.GetMetadata().GetAtespace()},
		{Name: "ATE_ACTOR_NAME", Value: actor.GetMetadata().GetName()},
		{Name: "ATE_ACTOR_UID", Value: actor.GetMetadata().GetUid()},
	}
}

// ensureAteletForked starts the forked actor from the seed on its worker. The
// seed is the one the Worker lists for the actor's template and atespace, so
// atelet and the worker see the seed this workflow was scheduled against.
func (w *ActorWorkflow) ensureAteletForked(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor, actorTemplate *ateapi.ActorTemplate) (tele restoreTelemetry, workerEpoch int64, err error) {
	ctx, done := stepSpan(ctx, "CallAteletFork")
	defer func() { err = done(err) }()

	tele.SnapshotKind = ateattr.SnapshotKindBoot
	assignment := actor.GetStatus().GetWorkerAssignment()
	worker, err := w.store.GetWorker(ctx, assignment.GetWorker().GetName())
	if err != nil {
		return tele, 0, fmt.Errorf("while getting worker %s: %w", assignment.GetWorker().GetName(), err)
	}
	seed := scheduling.SeedFor(worker, &scheduling.SeedRequirement{Atespace: actor.GetMetadata().GetAtespace(), TemplateUID: actorTemplate.GetMetadata().GetUid()})
	if seed == nil {
		// The seed went away after the placement. The retried resume finds the
		// worker ineligible and crashes the actor, which the reaper deletes.
		return tele, 0, apierror.Unavailable("worker %s has no ready seed of ActorTemplate %s for Actor %s", worker.GetMetadata().GetName(), actor.GetActorTemplate().GetName(), actorRef)
	}
	conn, err := w.dialer.DialForAteletOnNode(assignment.GetNodeName())
	if err != nil {
		return tele, 0, err
	}
	spec, err := workloadSpecFromActorTemplate(actorTemplate, actor, nil)
	if err != nil {
		return tele, 0, err
	}
	fork := actorTemplate.GetForkConfig()
	resp, err := ateletpb.NewAteletClient(conn).Fork(ctx, &ateletpb.ForkRequest{
		WorkerPodUid:          assignment.GetWorkerPodUid(),
		Atespace:              actor.GetMetadata().GetAtespace(),
		ActorName:             actor.GetMetadata().GetName(),
		ActorUid:              actor.GetMetadata().GetUid(),
		ActorTemplateAtespace: actor.GetActorTemplate().GetAtespace(),
		ActorTemplateName:     actor.GetActorTemplate().GetName(),
		TemplateUid:           actorTemplate.GetMetadata().GetUid(),
		SeedActorUid:          seed.GetSeedActorUid(),
		SeedWorkerEpoch:       seed.GetWorkerEpoch(),
		Spec:                  spec,
		Resources:             toAteletResources(actorTemplate.GetResources()),
		Env:                   forkEnv(actor),
		SandboxRuntime:        toAteletSandboxRuntime(assignment.GetSandboxRuntime()),
		PidsLimit:             fork.GetPidsLimit(),
		LogBytesLimit:         fork.GetLogBytesLimit(),
		MaxLifetimeSeconds:    fork.GetMaxLifetimeSeconds(),
		EgressGateway:         w.forkEgressGateway(fork),
	})
	if err != nil {
		return tele, 0, handleAteletError(ctx, w.store, actorRef, ateattr.OperationResume, "Fork", false, err)
	}
	return tele, resp.GetWorkerEpoch(), nil
}

// forkEgressGateway is the gateway a forked actor's egress goes through: the
// default gateway when the template asks for FORK_EGRESS_GATEWAY, else none, so
// the actor's TCP is captured and refused.
func (w *ActorWorkflow) forkEgressGateway(fork *ateapi.ForkConfig) *ateletpb.EgressGateway {
	if fork.GetEgress() != ateapi.ForkEgress_FORK_EGRESS_GATEWAY {
		return nil
	}
	return w.egressGateway()
}

// updateWorkerSeeds applies mutate to the seeds of the named Worker and
// writes the result when mutate reports a change. A write that loses a race
// reads the Worker again.
func updateWorkerSeeds(ctx context.Context, st interface {
	GetWorker(ctx context.Context, name string) (*ateapi.Worker, error)
	UpdateWorker(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapi.Worker) error) (*ateapi.Worker, error)
}, workerName string, mutate func(seeds []*ateapi.WorkerSeed) ([]*ateapi.WorkerSeed, bool)) error {
	var err error
	for range updateWorkerAttempts {
		var worker *ateapi.Worker
		if worker, err = st.GetWorker(ctx, workerName); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil // the Worker is gone, and its seeds with it
			}
			return fmt.Errorf("while getting worker %s: %w", workerName, err)
		}
		seeds, changed := mutate(slices.Clone(worker.GetStatus().GetSeeds()))
		if !changed {
			return nil
		}
		_, err = st.UpdateWorker(ctx, workerName, store.PreconditionFrom(worker), func(toUpdate *ateapi.Worker) error {
			if toUpdate.Status == nil {
				toUpdate.Status = &ateapi.WorkerStatus{}
			}
			toUpdate.Status.Seeds = seeds
			return nil
		})
		if err == nil || !errors.Is(err, store.ErrVersionConflict) {
			break
		}
	}
	if err != nil {
		return fmt.Errorf("while updating the seeds of worker %s: %w", workerName, err)
	}
	return nil
}

// recordSeedReady lists a RUNNING seed Actor on its Worker, so the scheduler
// places forked actors there. The Worker lists one seed per template, so an
// entry for the template is replaced.
func recordSeedReady(ctx context.Context, st interface {
	GetWorker(ctx context.Context, name string) (*ateapi.Worker, error)
	UpdateWorker(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapi.Worker) error) (*ateapi.Worker, error)
}, workerName, templateUID string, seedActor *ateapi.Actor, workerEpoch int64) error {
	want := &ateapi.WorkerSeed{
		SeedActor:    &ateapi.ObjectRef{Atespace: seedActor.GetMetadata().GetAtespace(), Name: seedActor.GetMetadata().GetName()},
		SeedActorUid: seedActor.GetMetadata().GetUid(),
		TemplateUid:  templateUID,
		WorkerEpoch:  workerEpoch,
	}
	return updateWorkerSeeds(ctx, st, workerName, func(seeds []*ateapi.WorkerSeed) ([]*ateapi.WorkerSeed, bool) {
		for _, seed := range seeds {
			if seed.GetTemplateUid() == templateUID {
				if proto.Equal(seed, want) {
					return seeds, false
				}
			}
		}
		seeds = slices.DeleteFunc(seeds, func(seed *ateapi.WorkerSeed) bool { return seed.GetTemplateUid() == templateUID })
		return append(seeds, want), true
	})
}

// removeSeed drops the entry of a seed Actor from its Worker.
func removeSeed(ctx context.Context, st interface {
	GetWorker(ctx context.Context, name string) (*ateapi.Worker, error)
	UpdateWorker(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapi.Worker) error) (*ateapi.Worker, error)
}, workerName, seedActorUID string) error {
	return updateWorkerSeeds(ctx, st, workerName, func(seeds []*ateapi.WorkerSeed) ([]*ateapi.WorkerSeed, bool) {
		kept := slices.DeleteFunc(seeds, func(seed *ateapi.WorkerSeed) bool { return seed.GetSeedActorUid() == seedActorUID })
		return kept, len(kept) != len(seeds)
	})
}

// afterRunning does the fork work that follows an actor reaching RUNNING: a
// seed becomes visible to the scheduler. A failure is logged, since the seed
// reconciler lists every RUNNING seed on its next pass.
func (w *ActorWorkflow) afterRunning(ctx context.Context, running *ateapi.Actor, workerEpoch int64) {
	if !isSeedActor(running) {
		return
	}
	template, err := resolveActorTemplate(ctx, w.store, running)
	if err != nil {
		slog.WarnContext(ctx, "Cannot list a ready seed: its template does not resolve", slog.String("actor", running.GetMetadata().GetName()), slog.Any("err", err))
		return
	}
	workerName := running.GetStatus().GetWorkerAssignment().GetWorker().GetName()
	if err := recordSeedReady(ctx, w.store, workerName, template.GetMetadata().GetUid(), running, workerEpoch); err != nil {
		slog.WarnContext(ctx, "Cannot list a ready seed on its worker", slog.String("actor", running.GetMetadata().GetName()), slog.String("worker", workerName), slog.Any("err", err))
	}
}
