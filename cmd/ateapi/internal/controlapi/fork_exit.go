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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ExitActorReportedByWorker records the exit of the workload of a RUNNING
// forked actor, on behalf of the worker hosting it, and deletes the actor: a
// forked actor runs once. It holds the actor's lease while it records the exit,
// so an operation in flight keeps the actor.
//
// The delete takes the lease itself, so it runs once this releases it. A delete
// that fails leaves the actor RUNNING with its exit recorded, and the reaper
// finishes it.
func (w *ActorWorkflow) ExitActorReportedByWorker(ctx context.Context, actorRef resources.ActorRef, actorUID, workerName string, exitCode int32) (*ateapi.Actor, error) {
	ctx = preview.ContextWithOptIn(ctx)
	if err := w.recordForkExit(ctx, actorRef, actorUID, workerName, exitCode); err != nil {
		return nil, err
	}
	deleted, err := w.DeleteActor(ctx, actorRef, store.DeletePreconditions{UID: actorUID})
	if err != nil {
		return nil, fmt.Errorf("while deleting the exited actor: %w", err)
	}
	return deleted, nil
}

func (w *ActorWorkflow) recordForkExit(ctx context.Context, actorRef resources.ActorRef, actorUID, workerName string, exitCode int32) error {
	leaseCtx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return err
	}
	defer lease.Close()

	actor, err := w.store.GetActor(leaseCtx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return apierror.NotFound("Actor %s not found", actorRef)
		}
		return fmt.Errorf("while loading actor %s: %w", actorRef, err)
	}
	// rechecked under the lease, the caller's checks raced other operations
	if actor.GetMetadata().GetUid() != actorUID || actor.GetStatus().GetWorkerAssignment().GetWorker().GetName() != workerName {
		return apierror.NotFound("Actor %s not found", actorRef)
	}
	if !isForkActor(actor) {
		return apierror.FailedPrecondition("actor %s is not a forked actor, so its exit is not reported", actorRef)
	}
	if state := actor.GetStatus().GetState(); state != ateapi.ActorState_ACTOR_STATE_RUNNING {
		return apierror.FailedPrecondition("actor %s is %s, not RUNNING", actorRef, state)
	}
	if _, err := w.store.UpdateActor(leaseCtx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapi.Actor) error {
		toUpdate.Status.Fork.ExitCode = exitCode
		toUpdate.Status.Fork.ExitTime = timestamppb.Now()
		return nil
	}); err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return apierror.Aborted("concurrent update conflict, please retry")
		}
		return fmt.Errorf("while recording the exit of actor %s: %w", actorRef, err)
	}
	slog.InfoContext(ctx, "Forked actor exited", slog.String("actor", actorRef.String()), slog.String("actor_uid", actorUID), slog.Int("exit_code", int(exitCode)))
	return nil
}

// ExitActorReportedByWorker deletes a forked actor whose workload exited, on
// behalf of the worker hosting it. The caller authorizes the worker.
func (s *RPCService) ExitActorReportedByWorker(ctx context.Context, actorRef resources.ActorRef, actorUID, workerName string, exitCode int32) (*ateapi.Actor, error) {
	setSpanActorRefAttributes(ctx, actorRef)
	actor, err := s.actorWorkflow.ExitActorReportedByWorker(ctx, actorRef, actorUID, workerName, exitCode)
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, err
	}
	setSpanActorAttributes(ctx, actor)
	return actor, nil
}
