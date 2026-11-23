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
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestForkReaper(t *testing.T) {
	// forkLifetime is the template's max_lifetime_seconds.
	const forkLifetime = 60 * time.Second
	tests := []struct {
		name string
		// state and fork seed the Actor, whose template is tmpl.
		state ateapi.ActorState
		fork  *ateapi.ActorFork
		tmpl  string
		// later is how far past the time the Actor was written the reaper runs.
		later    time.Duration
		wantGone bool
	}{{
		name:     "a crashed forked actor",
		state:    ateapi.ActorState_ACTOR_STATE_CRASHED,
		fork:     &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_FORK},
		wantGone: true,
	}, {
		name:     "a forked actor whose delete did not finish",
		state:    ateapi.ActorState_ACTOR_STATE_DELETING,
		fork:     &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_FORK},
		wantGone: true,
	}, {
		name:  "a running forked actor within its deadline",
		state: ateapi.ActorState_ACTOR_STATE_RUNNING,
		fork:  &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_FORK, StartTime: timestamppb.Now()},
		later: forkLifetime,
	}, {
		name:     "a running forked actor past its deadline",
		state:    ateapi.ActorState_ACTOR_STATE_RUNNING,
		fork:     &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_FORK, StartTime: timestamppb.Now()},
		later:    forkLifetime + forkDeadlineGrace + time.Second,
		wantGone: true,
	}, {
		name:  "a running forked actor with no start time",
		state: ateapi.ActorState_ACTOR_STATE_RUNNING,
		fork:  &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_FORK},
		later: time.Hour,
	}, {
		name:  "a forked actor that is waiting to start",
		state: ateapi.ActorState_ACTOR_STATE_SUSPENDED,
		fork:  &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_FORK},
		later: forkUnstartedAge - time.Second,
	}, {
		name:     "a forked actor that never started",
		state:    ateapi.ActorState_ACTOR_STATE_SUSPENDED,
		fork:     &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_FORK},
		later:    forkUnstartedAge + time.Second,
		wantGone: true,
	}, {
		name:     "a forked actor whose resume was lost",
		state:    ateapi.ActorState_ACTOR_STATE_RESUMING,
		fork:     &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_FORK},
		later:    forkUnstartedAge + time.Second,
		wantGone: true,
	}, {
		name:  "a crashed seed is the reconciler's",
		state: ateapi.ActorState_ACTOR_STATE_CRASHED,
		fork:  &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_SEED},
	}, {
		name:     "a seed whose template is gone",
		state:    ateapi.ActorState_ACTOR_STATE_CRASHED,
		fork:     &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_SEED},
		tmpl:     "deleted",
		wantGone: true,
	}, {
		name:  "an actor that is not forked",
		state: ateapi.ActorState_ACTOR_STATE_CRASHED,
		later: time.Hour,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newForkEnv(t)
			ctx := context.Background()
			tmpl := tt.tmpl
			if tmpl == "" {
				tmpl = forkTemplate
			}
			actor := storetest.MustCreateActor(t, ctx, e.st, &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: forkAtespace, Name: "subject"},
				ActorTemplate: &ateapi.ObjectRef{Atespace: forkAtespace, Name: tmpl},
				Status:        &ateapi.ActorStatus{State: tt.state, Fork: tt.fork},
			})
			r := NewForkReaper(e.st, workflowControl{e.workflow}, time.Minute)
			written := actor.GetMetadata().GetUpdateTime().AsTime()
			r.now = func() time.Time { return written.Add(tt.later) }
			if err := r.Reap(ctx); err != nil {
				t.Fatalf("Reap() = %v", err)
			}
			_, err := e.st.GetActor(ctx, resources.ActorRefFromActor(actor))
			if gone := errors.Is(err, store.ErrNotFound); gone != tt.wantGone {
				t.Errorf("after Reap() the actor is gone = %v (GetActor: %v), want %v", gone, err, tt.wantGone)
			}
		})
	}
}

func TestForkReaper_OnlyListsForkAtespaces(t *testing.T) {
	e := newForkEnv(t)
	ctx := context.Background()
	storetest.MustCreateAtespace(t, ctx, e.st, "team-b")
	// A forked actor can only live in the atespace of its template, so a crashed
	// Actor elsewhere is none of the reaper's business.
	elsewhere := storetest.MustCreateActor(t, ctx, e.st, &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: "team-b", Name: "subject"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: forkAtespace, Name: forkTemplate},
		Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_CRASHED, Fork: &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_FORK}},
	})
	if err := NewForkReaper(e.st, workflowControl{e.workflow}, time.Minute).Reap(ctx); err != nil {
		t.Fatalf("Reap() = %v", err)
	}
	if _, err := e.st.GetActor(ctx, resources.ActorRefFromActor(elsewhere)); err != nil {
		t.Errorf("the reaper touched an Actor outside the fork atespaces: %v", err)
	}
}

func TestForkReaper_GateOff(t *testing.T) {
	e := newForkEnv(t)
	ctx := context.Background()
	crashed := storetest.MustCreateActor(t, ctx, e.st, &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: forkAtespace, Name: "subject"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: forkAtespace, Name: forkTemplate},
		Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_CRASHED, Fork: &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_FORK}},
	})
	preview.InitForTest(t)
	if err := NewForkReaper(e.st, workflowControl{e.workflow}, time.Minute).Reap(ctx); err != nil {
		t.Fatalf("Reap() = %v", err)
	}
	if _, err := e.st.GetActor(ctx, resources.ActorRefFromActor(crashed)); err != nil {
		t.Errorf("the reaper ran with the gate off: %v", err)
	}
}
