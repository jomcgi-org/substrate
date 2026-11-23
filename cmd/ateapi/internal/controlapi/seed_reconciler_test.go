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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// workflowControl drives the reconciler through the workflow the Control
// service would, without the gRPC layer.
type workflowControl struct{ w *ActorWorkflow }

func (c workflowControl) ResumeActor(ctx context.Context, req *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error) {
	actor, resumed, err := c.w.ResumeActor(ctx, resources.ActorRefFromObjectRef(req.GetActor()))
	if err != nil {
		return nil, err
	}
	return &ateapi.ResumeActorResponse{Actor: actor, Resumed: resumed}, nil
}

func (c workflowControl) DeleteActor(ctx context.Context, req *ateapi.DeleteActorRequest) (*ateapi.Actor, error) {
	return c.w.DeleteActor(ctx, resources.ActorRefFromObjectRef(req.GetActor()), toDeletePreconditions(req.GetOptions()))
}

func (e *forkFixture) reconciler() *SeedReconciler {
	return NewSeedReconciler(e.st, workflowControl{e.workflow}, time.Minute)
}

// seedOf returns the seed of the fixture's template on pod, or nil.
func (e *forkFixture) seedOf(atespace, templateUID, pod string) *ateapi.Actor {
	e.t.Helper()
	seed, err := e.st.GetActor(context.Background(), resources.ActorRef{Atespace: atespace, Name: seedActorName(templateUID, testWorkerUID(pod))})
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		e.t.Fatalf("GetActor(seed on %s): %v", pod, err)
	}
	return seed
}

func TestSeedReconciler_BuildsSeedsOnEligibleWorkers(t *testing.T) {
	e := newForkEnv(t, func(tmpl *ateapi.ActorTemplate) {
		tmpl.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"pool": "fork"}}
	})
	ctx := context.Background()
	fork := map[string]string{"pool": "fork"}
	e.addWorker("pod-1", withLabels(fork))
	e.addWorker("pod-2", withLabels(fork))
	e.addWorker("pod-other-pool", withLabels(map[string]string{"pool": "other"}))
	e.addWorker("pod-no-fork", withLabels(fork), func(w *ateapi.Worker) {
		w.Status.SandboxClasses[0].DefaultRuntime.ForkOverhead = nil
	})
	e.addWorker("pod-draining", withLabels(fork), func(w *ateapi.Worker) {
		w.Status.State = ateapi.WorkerState_WORKER_STATE_DRAINING
	})
	uid := e.template.GetMetadata().GetUid()

	if err := e.reconciler().Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	for _, pod := range []string{"pod-1", "pod-2"} {
		seed := e.seedOf(forkAtespace, uid, pod)
		if seed == nil {
			t.Fatalf("no seed on %s", pod)
		}
		if got := seed.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_RUNNING {
			t.Errorf("seed on %s is %v, want RUNNING", pod, got)
		}
		if got := seed.GetStatus().GetFork().GetPinnedWorker().GetName(); got != testWorkerUID(pod) {
			t.Errorf("seed on %s is pinned to %q", pod, got)
		}
		if got := seed.GetStatus().GetFork().GetRole(); got != ateapi.ForkRole_FORK_ROLE_SEED {
			t.Errorf("seed on %s has role %v", pod, got)
		}
		seeds := e.worker1(pod).GetStatus().GetSeeds()
		if len(seeds) != 1 || seeds[0].GetSeedActorUid() != seed.GetMetadata().GetUid() || seeds[0].GetTemplateUid() != uid || seeds[0].GetWorkerEpoch() != registeredEpoch {
			t.Errorf("worker %s lists seeds %v, want the seed %s of template %s at epoch %d", pod, seeds, seed.GetMetadata().GetUid(), uid, registeredEpoch)
		}
	}
	for _, pod := range []string{"pod-other-pool", "pod-no-fork", "pod-draining"} {
		if seed := e.seedOf(forkAtespace, uid, pod); seed != nil {
			t.Errorf("a seed was built on ineligible worker %s", pod)
		}
	}
	if got, _, _, _ := e.atelet.counts(); got != 2 {
		t.Fatalf("atelet saw %d seed calls, want 2", got)
	}
	req := e.atelet.seeds[0]
	if got := req.GetResources().GetLimits().GetMemoryBytes(); got != 1<<30 {
		t.Errorf("a seed holds %d bytes, want seed_resources %d", got, 1<<30)
	}
	if req.GetTemplateUid() != uid || req.GetAtespace() != forkAtespace {
		t.Errorf("seed request is for template %q in %q", req.GetTemplateUid(), req.GetAtespace())
	}

	// A second pass changes nothing.
	if err := e.reconciler().Reconcile(ctx); err != nil {
		t.Fatalf("second Reconcile() = %v", err)
	}
	if got, _, _, _ := e.atelet.counts(); got != 2 {
		t.Errorf("a second pass built %d more seeds", got-2)
	}
}

func TestSeedReconciler_SeedsServeOneAtespace(t *testing.T) {
	e := newForkEnv(t)
	ctx := context.Background()
	storetest.MustCreateAtespace(t, ctx, e.st, "team-b")
	other, err := e.st.CreateActorTemplate(ctx, forkTemplateSpec(func(tmpl *ateapi.ActorTemplate) {
		tmpl.Metadata = &ateapi.ResourceMetadata{Atespace: "team-b", Name: "scan"}
	}))
	if err != nil {
		t.Fatal(err)
	}
	e.addWorker("pod-1")
	e.addWorker("pod-2")

	for range 2 {
		if err := e.reconciler().Reconcile(ctx); err != nil {
			t.Fatalf("Reconcile() = %v", err)
		}
	}
	for _, pod := range []string{"pod-1", "pod-2"} {
		inA := e.seedOf(forkAtespace, e.template.GetMetadata().GetUid(), pod)
		inB := e.seedOf("team-b", other.GetMetadata().GetUid(), pod)
		if (inA == nil) == (inB == nil) {
			t.Errorf("worker %s holds seeds of both atespaces or neither: team-a %v, team-b %v", pod, inA != nil, inB != nil)
		}
		if got := len(e.worker1(pod).GetStatus().GetSeeds()); got != 1 {
			t.Errorf("worker %s lists %d seeds, want 1", pod, got)
		}
	}
}

func TestSeedReconciler_CrashedSeedIsRebuiltAfterBackoff(t *testing.T) {
	e := newForkEnv(t)
	ctx := context.Background()
	e.addWorker("pod-1")
	e.atelet.seedErr = status.Error(codes.Internal, "mkfs.erofs: not found")
	r := e.reconciler()
	now := time.Now()
	r.now = func() time.Time { return now }

	if err := r.Reconcile(ctx); err == nil {
		t.Fatal("Reconcile() = nil, want the failed seed build")
	}
	uid := e.template.GetMetadata().GetUid()
	seed := e.seedOf(forkAtespace, uid, "pod-1")
	if seed.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_CRASHED {
		t.Fatalf("seed is %v after a failed build, want CRASHED", seed.GetStatus().GetState())
	}
	if len(e.worker1("pod-1").GetStatus().GetSeeds()) != 0 {
		t.Error("a seed that failed to build is listed on its worker")
	}

	// Within the backoff nothing is rebuilt, so a seed that cannot start does
	// not loop.
	now = seed.GetStatus().GetCrash().GetCrashTime().AsTime().Add(seedRetryBackoff / 2)
	if err := r.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile() within the backoff = %v", err)
	}
	if got, _, _, _ := e.atelet.counts(); got != 1 {
		t.Errorf("seed builds within the backoff = %d, want 1", got)
	}

	// After it the crashed seed goes and a new one is built.
	e.atelet.seedErr = nil
	now = seed.GetStatus().GetCrash().GetCrashTime().AsTime().Add(seedRetryBackoff + time.Second)
	if err := r.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile() after the backoff = %v", err)
	}
	if err := r.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	rebuilt := e.seedOf(forkAtespace, uid, "pod-1")
	if rebuilt == nil || rebuilt.GetMetadata().GetUid() == seed.GetMetadata().GetUid() || rebuilt.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("seed after the backoff = %v, want a new RUNNING one", rebuilt)
	}
}

func TestSeedReconciler_DropsSeedsThatNoLongerBelong(t *testing.T) {
	e := newForkEnv(t)
	ctx := context.Background()
	worker := e.addWorker("pod-1")
	seed := e.seedReady(worker, forkAtespace)
	uid := e.template.GetMetadata().GetUid()

	// The Worker leaves the pool of the template.
	if _, err := e.st.UpdateWorker(ctx, worker.GetMetadata().GetName(), store.PreconditionFrom(e.worker1("pod-1")), func(w *ateapi.Worker) error {
		w.Status.State = ateapi.WorkerState_WORKER_STATE_DRAINING
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.reconciler().Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	if e.seedOf(forkAtespace, uid, "pod-1") != nil {
		t.Errorf("the seed %s of a draining worker was kept", seed.GetMetadata().GetName())
	}
	if _, _, drops, _ := e.atelet.counts(); drops != 1 {
		t.Errorf("DropSeed calls = %d, want 1", drops)
	}
	if got := e.worker1("pod-1").GetStatus().GetSeeds(); len(got) != 0 {
		t.Errorf("the dropped seed is still listed: %v", got)
	}
}

func TestSeedReconciler_RepairsWorkerSeedList(t *testing.T) {
	e := newForkEnv(t)
	ctx := context.Background()
	worker := e.addWorker("pod-1")
	seed := e.seedReady(worker, forkAtespace)

	t.Run("a RUNNING seed missing from its worker is listed again", func(t *testing.T) {
		if err := removeSeed(ctx, e.st, worker.GetMetadata().GetName(), seed.GetMetadata().GetUid()); err != nil {
			t.Fatal(err)
		}
		if err := e.reconciler().Reconcile(ctx); err != nil {
			t.Fatalf("Reconcile() = %v", err)
		}
		if got := e.worker1("pod-1").GetStatus().GetSeeds(); len(got) != 1 || got[0].GetSeedActorUid() != seed.GetMetadata().GetUid() {
			t.Errorf("seeds after the pass = %v, want the RUNNING seed", got)
		}
	})

	t.Run("an entry whose seed is gone is removed", func(t *testing.T) {
		err := updateWorkerSeeds(ctx, e.st, worker.GetMetadata().GetName(), func(seeds []*ateapi.WorkerSeed) ([]*ateapi.WorkerSeed, bool) {
			return append(seeds, &ateapi.WorkerSeed{
				SeedActor:    &ateapi.ObjectRef{Atespace: forkAtespace, Name: "seed-gone"},
				SeedActorUid: "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21",
				TemplateUid:  "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
				WorkerEpoch:  registeredEpoch,
			}), true
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.reconciler().Reconcile(ctx); err != nil {
			t.Fatalf("Reconcile() = %v", err)
		}
		if got := e.worker1("pod-1").GetStatus().GetSeeds(); len(got) != 1 || got[0].GetSeedActorUid() != seed.GetMetadata().GetUid() {
			t.Errorf("seeds after the pass = %v, want only the RUNNING seed", got)
		}
	})

	t.Run("an entry whose seed crashed is removed", func(t *testing.T) {
		mustUpdateActorStatus(t, ctx, e.st, e.actorIn(forkAtespace, seed.GetMetadata().GetName()), func(s *ateapi.ActorStatus) {
			s.State = ateapi.ActorState_ACTOR_STATE_CRASHED
			s.Crash = &ateapi.ActorCrash{Message: "gVisor sandbox exited", CrashTime: timestamppb.Now()}
		})
		if err := e.reconciler().Reconcile(ctx); err != nil {
			t.Fatalf("Reconcile() = %v", err)
		}
		if got := e.worker1("pod-1").GetStatus().GetSeeds(); len(got) != 0 {
			t.Errorf("seeds after the pass = %v, want none", got)
		}
	})
}

func (e *forkFixture) actorIn(atespace, name string) *ateapi.Actor {
	e.t.Helper()
	actor, err := e.st.GetActor(context.Background(), resources.ActorRef{Atespace: atespace, Name: name})
	if err != nil {
		e.t.Fatalf("GetActor(%s/%s): %v", atespace, name, err)
	}
	return actor
}

func TestSeedReconciler_GateOff(t *testing.T) {
	e := newForkEnv(t)
	e.addWorker("pod-1")
	preview.InitForTest(t)
	if err := e.reconciler().Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	if seeds, _, _, _ := e.atelet.counts(); seeds != 0 {
		t.Errorf("atelet saw %d seed calls with the gate off", seeds)
	}
	if e.seedOf(forkAtespace, e.template.GetMetadata().GetUid(), "pod-1") != nil {
		t.Error("a seed was created with the gate off")
	}
}

func TestSeedReconciler_TemplateBeingDeleted(t *testing.T) {
	e := newForkEnv(t)
	ctx := context.Background()
	e.addWorker("pod-1")
	if _, err := e.st.MarkActorTemplateForDeletion(ctx, resources.ActorTemplateRefFromActorTemplate(e.template), store.DeletePreconditions{}, func(*ateapi.ActorTemplate) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := e.reconciler().Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	if e.seedOf(forkAtespace, e.template.GetMetadata().GetUid(), "pod-1") != nil {
		t.Error("a seed was created for a template being deleted")
	}
}
