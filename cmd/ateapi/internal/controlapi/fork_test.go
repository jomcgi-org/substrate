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
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/scheduling"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	forkAtespace = "team-a"
	forkTemplate = "scan"
	forkNode     = "node-1"
)

// forkAtelet is an atelet that records the seed and fork calls it receives.
type forkAtelet struct {
	ateletpb.UnimplementedAteletServer

	mu         sync.Mutex
	seeds      []*ateletpb.SeedRequest
	forks      []*ateletpb.ForkRequest
	drops      []*ateletpb.DropSeedRequest
	terminates []*ateletpb.TerminateRequest
	seedErr    error
	forkErr    error
	// onTerminate, when set, runs as Terminate is called.
	onTerminate func(*ateletpb.TerminateRequest)
}

func (f *forkAtelet) Seed(_ context.Context, req *ateletpb.SeedRequest) (*ateletpb.SeedResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seeds = append(f.seeds, proto.CloneOf(req))
	if f.seedErr != nil {
		return nil, f.seedErr
	}
	return &ateletpb.SeedResponse{WorkerEpoch: registeredEpoch}, nil
}

func (f *forkAtelet) Fork(_ context.Context, req *ateletpb.ForkRequest) (*ateletpb.ForkResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forks = append(f.forks, proto.CloneOf(req))
	if f.forkErr != nil {
		return nil, f.forkErr
	}
	return &ateletpb.ForkResponse{WorkerEpoch: registeredEpoch}, nil
}

func (f *forkAtelet) DropSeed(_ context.Context, req *ateletpb.DropSeedRequest) (*ateletpb.DropSeedResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.drops = append(f.drops, proto.CloneOf(req))
	return &ateletpb.DropSeedResponse{}, nil
}

func (f *forkAtelet) Terminate(_ context.Context, req *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	f.mu.Lock()
	hook := f.onTerminate
	f.terminates = append(f.terminates, proto.CloneOf(req))
	f.mu.Unlock()
	if hook != nil {
		hook(req)
	}
	return &ateletpb.TerminateResponse{}, nil
}

func (f *forkAtelet) counts() (seeds, forks, drops, terminates int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.seeds), len(f.forks), len(f.drops), len(f.terminates)
}

// storeWorkers is a scheduling.WorkerSource that reads the store, so a test
// sees a Worker's seeds as soon as they are written.
type storeWorkers struct{ st store.Interface }

func (s storeWorkers) Workers() ([]*ateapi.Worker, error) {
	page, err := s.st.ListWorkers(context.Background(), store.ListOptions{})
	return page.Items, err
}

// forkFixture is a store with a fork template and a fork-capable worker, and an
// ActorWorkflow that reaches a fake atelet.
type forkFixture struct {
	t        *testing.T
	st       store.Interface
	atelet   *forkAtelet
	workflow *ActorWorkflow
	impl     *ServiceImpl
	template *ateapi.ActorTemplate
	worker   *ateapi.Worker
}

func forkTemplateSpec(mutations ...func(*ateapi.ActorTemplate)) *ateapi.ActorTemplate {
	tmpl := &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{Atespace: forkAtespace, Name: forkTemplate},
		Containers: []*ateapi.Container{{
			Name:        "main",
			Image:       "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			WakeupProbe: &ateapi.WakeupProbe{HttpGet: &ateapi.HttpGetAction{Path: "/healthz", Port: 8080}, TimeoutSeconds: 10},
		}},
		SnapshotConfig: &ateapi.SnapshotConfig{
			StorageLocation:      testStorageLocation,
			GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED},
		},
		Resources: &ateapi.ResourceRequirements{Limits: resources.CPUMemory(500, 256<<20)},
		ForkConfig: &ateapi.ForkConfig{
			Mode:               ateapi.ForkMode_FORK_MODE_SEED_AND_FORK,
			SeedResources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 1<<30)},
			SingleUse:          true,
			MaxLifetimeSeconds: 60,
			PidsLimit:          64,
			LogBytesLimit:      1 << 20,
		},
	}
	for _, m := range mutations {
		m(tmpl)
	}
	return tmpl
}

func newForkEnv(t *testing.T, mutations ...func(*ateapi.ActorTemplate)) *forkFixture {
	t.Helper()
	preview.InitForTest(t, string(preview.GateForkSeed), string(preview.GatePerTemplateSnapshotLocation))
	ctx := context.Background()
	st := newTestPersistence(t)
	template, err := st.CreateActorTemplate(ctx, forkTemplateSpec(mutations...))
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	atelet := &forkAtelet{}
	w := &ActorWorkflow{
		store:            st,
		scheduler:        scheduling.New(storeWorkers{st}),
		dialer:           newNodeAteletDialer(t, atelet),
		workflowDeadline: time.Minute,
	}
	return &forkFixture{t: t, st: st, atelet: atelet, workflow: w, impl: newServiceImpl(st, nil), template: template}
}

// forkWorkerSpec is a Worker that can hold seeds: a runtime that reports a fork
// overhead, and room for a few actors.
func forkWorkerSpec(pod string, mutations ...func(*ateapi.Worker)) *ateapi.Worker {
	classes := workerSandboxClasses(testSandboxRuntime())
	classes[0].DefaultRuntime.ActorOverhead = &ateapi.RuntimeOverhead{MemoryBytes: 64 << 20}
	classes[0].DefaultRuntime.ForkOverhead = &ateapi.RuntimeOverhead{MemoryBytes: 16 << 20}
	worker := &ateapi.Worker{
		Metadata:        &ateapi.ResourceMetadata{Name: testWorkerUID(pod)},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       pod,
		WorkerPodUid:    testWorkerUID(pod),
		NodeName:        forkNode,
		Status: &ateapi.WorkerStatus{
			State:           ateapi.WorkerState_WORKER_STATE_ACTIVE,
			RegisteredEpoch: registeredEpoch,
			Capacity:        &ateapi.WorkerResources{Actors: 10, CpuMillicores: 16000, MemoryBytes: 64 << 30},
			SandboxClasses:  classes,
		},
	}
	for _, m := range mutations {
		m(worker)
	}
	return worker
}

func (e *forkFixture) addWorker(pod string, mutations ...func(*ateapi.Worker)) *ateapi.Worker {
	e.t.Helper()
	worker, err := e.st.CreateWorker(context.Background(), forkWorkerSpec(pod, mutations...))
	if err != nil {
		e.t.Fatalf("CreateWorker(%s): %v", pod, err)
	}
	return worker
}

func withLabels(l map[string]string) func(*ateapi.Worker) {
	return func(w *ateapi.Worker) { w.Labels = l }
}

// seedReady stores a RUNNING seed Actor of the template on worker and lists it
// on the Worker, as a finished seed build leaves them.
func (e *forkFixture) seedReady(worker *ateapi.Worker, atespace string) *ateapi.Actor {
	e.t.Helper()
	ctx := context.Background()
	storetest.MustCreateAtespace(e.t, ctx, e.st, atespace)
	seed := storetest.MustCreateActor(e.t, ctx, e.st, &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: atespace, Name: seedActorName(e.template.GetMetadata().GetUid(), worker.GetMetadata().GetName())},
		ActorTemplate: &ateapi.ObjectRef{Atespace: forkAtespace, Name: forkTemplate},
		Status: &ateapi.ActorStatus{
			State: ateapi.ActorState_ACTOR_STATE_RUNNING,
			Fork:  &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_SEED, PinnedWorker: &ateapi.ObjectRef{Name: worker.GetMetadata().GetName()}},
		},
	})
	// A running seed holds an assignment on its worker.
	seedAssignment(e.t, e.st, worker.GetMetadata().GetName(), &ateapi.ActorAssignment{
		Actor:     &ateapi.ObjectRef{Atespace: atespace, Name: seed.GetMetadata().GetName()},
		ActorUid:  seed.GetMetadata().GetUid(),
		Resources: resources.CPUMemory(1000, 1<<30),
	})
	seed = mustUpdateActorStatus(e.t, ctx, e.st, seed, func(s *ateapi.ActorStatus) {
		s.AssignedNode = forkNode
		s.WorkerAssignment = &ateapi.WorkerAssignment{
			Worker:          &ateapi.ObjectRef{Name: worker.GetMetadata().GetName()},
			WorkerNamespace: worker.GetWorkerNamespace(),
			WorkerPool:      worker.GetWorkerPool(),
			WorkerPod:       worker.GetWorkerPod(),
			WorkerPodUid:    worker.GetWorkerPodUid(),
			NodeName:        forkNode,
			WorkerEpoch:     registeredEpoch,
		}
	})
	if err := recordSeedReady(ctx, e.st, worker.GetMetadata().GetName(), e.template.GetMetadata().GetUid(), seed, registeredEpoch); err != nil {
		e.t.Fatalf("recordSeedReady: %v", err)
	}
	return seed
}

// newForkActor creates a forked Actor the way CreateActor does.
func (e *forkFixture) newForkActor(name string) *ateapi.Actor {
	e.t.Helper()
	actor, err := e.impl.CreateActor(context.Background(), &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: forkAtespace, Name: name},
		ActorTemplate: &ateapi.ObjectRef{Atespace: forkAtespace, Name: forkTemplate},
	})
	if err != nil {
		e.t.Fatalf("CreateActor(%s): %v", name, err)
	}
	return actor
}

func (e *forkFixture) actor(name string) *ateapi.Actor {
	e.t.Helper()
	actor, err := e.st.GetActor(context.Background(), resources.ActorRef{Atespace: forkAtespace, Name: name})
	if err != nil {
		e.t.Fatalf("GetActor(%s): %v", name, err)
	}
	return actor
}

func (e *forkFixture) worker1(pod string) *ateapi.Worker {
	e.t.Helper()
	worker, err := e.st.GetWorker(context.Background(), testWorkerUID(pod))
	if err != nil {
		e.t.Fatalf("GetWorker(%s): %v", pod, err)
	}
	return worker
}

func TestCreateActor_ForkTemplate(t *testing.T) {
	otherTemplate := func(tmpl *ateapi.ActorTemplate) {
		tmpl.Metadata.Name = "plain"
		tmpl.ForkConfig = nil
		tmpl.SnapshotConfig.GoldenSnapshotConfig = nil
	}
	tests := []struct {
		name     string
		gates    []string
		actor    *ateapi.Actor
		want     codes.Code
		wantFork bool
	}{{
		name:     "same atespace",
		gates:    []string{"ForkSeed"},
		actor:    &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: forkAtespace, Name: "a"}, ActorTemplate: &ateapi.ObjectRef{Atespace: forkAtespace, Name: forkTemplate}},
		wantFork: true,
	}, {
		name:  "another atespace",
		gates: []string{"ForkSeed"},
		actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "team-b", Name: "a"}, ActorTemplate: &ateapi.ObjectRef{Atespace: forkAtespace, Name: forkTemplate}},
		want:  codes.PermissionDenied,
	}, {
		name:  "gate off",
		actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: forkAtespace, Name: "a"}, ActorTemplate: &ateapi.ObjectRef{Atespace: forkAtespace, Name: forkTemplate}},
		want:  codes.FailedPrecondition,
	}, {
		name:  "from a source tag",
		gates: []string{"ForkSeed"},
		actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: forkAtespace, Name: "a"}, ActorTemplate: &ateapi.ObjectRef{Atespace: forkAtespace, Name: forkTemplate}, SourceTag: &ateapi.ObjectRef{Atespace: forkAtespace, Name: "t"}},
		want:  codes.InvalidArgument,
	}, {
		name:  "a template without fork_config, gate on",
		gates: []string{"ForkSeed"},
		actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "team-b", Name: "a"}, ActorTemplate: &ateapi.ObjectRef{Atespace: forkAtespace, Name: "plain"}},
	}, {
		name:  "a template without fork_config, gate off",
		actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "team-b", Name: "a"}, ActorTemplate: &ateapi.ObjectRef{Atespace: forkAtespace, Name: "plain"}},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newForkEnv(t)
			preview.InitForTest(t, tt.gates...)
			ctx := context.Background()
			plain := forkTemplateSpec(otherTemplate)
			if _, err := e.st.CreateActorTemplate(ctx, plain); err != nil {
				t.Fatalf("CreateActorTemplate: %v", err)
			}
			storetest.MustCreateAtespace(t, ctx, e.st, "team-b")
			actor, err := e.impl.CreateActor(ctx, tt.actor)
			if got := apierror.Code(err); got != tt.want {
				t.Fatalf("CreateActor() = %v, code %v, want %v", err, got, tt.want)
			}
			if err != nil {
				return
			}
			if gotFork := actor.GetStatus().GetFork() != nil; gotFork != tt.wantFork {
				t.Errorf("CreateActor() fork status = %v, want a fork role: %v", actor.GetStatus().GetFork(), tt.wantFork)
			}
			if tt.wantFork && actor.GetStatus().GetFork().GetRole() != ateapi.ForkRole_FORK_ROLE_FORK {
				t.Errorf("CreateActor() role = %v, want FORK", actor.GetStatus().GetFork().GetRole())
			}
		})
	}
}

func TestForkLifecycleRefusals(t *testing.T) {
	e := newForkEnv(t)
	ctx := context.Background()
	worker := e.addWorker("pod-1")
	seed := e.seedReady(worker, forkAtespace)
	forked := e.newForkActor("forked")
	seedRef := resources.ActorRefFromActor(seed)
	forkRef := resources.ActorRefFromActor(forked)

	if _, err := e.workflow.SuspendActor(ctx, forkRef, suspendModeFromOptions(nil)); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("SuspendActor(forked) = %v, want FailedPrecondition", err)
	}
	if _, err := e.workflow.RevertActor(ctx, forkRef); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("RevertActor(forked) = %v, want FailedPrecondition", err)
	}
	if _, err := e.workflow.SuspendActor(ctx, seedRef, suspendModeFromOptions(nil)); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("SuspendActor(seed) = %v, want FailedPrecondition", err)
	}
	if _, err := e.workflow.RevertActor(ctx, seedRef); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("RevertActor(seed) = %v, want FailedPrecondition", err)
	}

	t.Run("a seed is the control plane's", func(t *testing.T) {
		if _, err := e.workflow.DeleteActor(ctx, seedRef, store.DeletePreconditions{}); apierror.Code(err) != codes.PermissionDenied {
			t.Errorf("DeleteActor(seed) = %v, want PermissionDenied", err)
		}
		suspended := e.seedUser(worker, "other-seed")
		if _, _, err := e.workflow.ResumeActor(ctx, resources.ActorRefFromActor(suspended)); apierror.Code(err) != codes.PermissionDenied {
			t.Errorf("ResumeActor(seed) = %v, want PermissionDenied", err)
		}
		if got := e.actor(seed.GetMetadata().GetName()).GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_RUNNING {
			t.Errorf("a refused delete changed the seed to %v", got)
		}
	})

	t.Run("a forked actor keeps its template", func(t *testing.T) {
		svc := &RPCService{impl: e.impl, persistence: e.st, actorWorkflow: e.workflow}
		moved := proto.CloneOf(forked)
		moved.ActorTemplate = &ateapi.ObjectRef{Atespace: forkAtespace, Name: "elsewhere"}
		moved.Status = nil
		if _, err := svc.UpdateActor(preview.ContextWithOptIn(ctx), &ateapi.UpdateActorRequest{Actor: moved}); apierror.Code(err) != codes.FailedPrecondition {
			t.Errorf("UpdateActor(forked, new template) = %v, want FailedPrecondition", err)
		}
	})

	t.Run("a seed cannot be updated by a user", func(t *testing.T) {
		svc := &RPCService{impl: e.impl, persistence: e.st, actorWorkflow: e.workflow}
		same := proto.CloneOf(seed)
		same.Status = nil
		if _, err := svc.UpdateActor(preview.ContextWithOptIn(ctx), &ateapi.UpdateActorRequest{Actor: same}); apierror.Code(err) != codes.PermissionDenied {
			t.Errorf("UpdateActor(seed) = %v, want PermissionDenied", err)
		}
	})

	t.Run("the control plane may delete a seed", func(t *testing.T) {
		if _, err := e.workflow.DeleteActor(withSystemPrincipal(ctx), seedRef, store.DeletePreconditions{}); err != nil {
			t.Fatalf("DeleteActor(seed) as the control plane = %v", err)
		}
		if _, _, drops, _ := e.atelet.counts(); drops != 1 {
			t.Errorf("DropSeed calls = %d, want 1", drops)
		}
		if got := e.worker1("pod-1").GetStatus().GetSeeds(); len(got) != 0 {
			t.Errorf("the deleted seed is still listed on its worker: %v", got)
		}
	})
}

// seedUser stores a SUSPENDED seed Actor the way the reconciler creates one.
func (e *forkFixture) seedUser(worker *ateapi.Worker, name string) *ateapi.Actor {
	e.t.Helper()
	return storetest.MustCreateActor(e.t, context.Background(), e.st, &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: forkAtespace, Name: name},
		ActorTemplate: &ateapi.ObjectRef{Atespace: forkAtespace, Name: forkTemplate},
		Status: &ateapi.ActorStatus{
			State: ateapi.ActorState_ACTOR_STATE_SUSPENDED,
			Fork:  &ateapi.ActorFork{Role: ateapi.ForkRole_FORK_ROLE_SEED, PinnedWorker: &ateapi.ObjectRef{Name: worker.GetMetadata().GetName()}},
		},
	})
}

func TestResumeFork(t *testing.T) {
	t.Run("forks from the seed on its worker", func(t *testing.T) {
		e := newForkEnv(t)
		ctx := context.Background()
		worker := e.addWorker("pod-1")
		seed := e.seedReady(worker, forkAtespace)
		forked := e.newForkActor("forked")

		running, resumed, err := e.workflow.ResumeActor(ctx, resources.ActorRefFromActor(forked))
		if err != nil || !resumed {
			t.Fatalf("ResumeActor() = %v, %v, want the actor started", resumed, err)
		}
		if running.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING {
			t.Errorf("state = %v, want RUNNING", running.GetStatus().GetState())
		}
		if running.GetStatus().GetFork().GetStartTime() == nil {
			t.Error("a running forked actor has no start_time")
		}
		seeds, forks, _, _ := e.atelet.counts()
		if seeds != 0 || forks != 1 {
			t.Fatalf("atelet saw %d seed and %d fork calls, want 0 and 1", seeds, forks)
		}
		req := e.atelet.forks[0]
		if req.GetSeedActorUid() != seed.GetMetadata().GetUid() || req.GetSeedWorkerEpoch() != registeredEpoch || req.GetTemplateUid() != e.template.GetMetadata().GetUid() {
			t.Errorf("fork request names seed %q at epoch %d of template %q, want %q at %d of %q",
				req.GetSeedActorUid(), req.GetSeedWorkerEpoch(), req.GetTemplateUid(), seed.GetMetadata().GetUid(), registeredEpoch, e.template.GetMetadata().GetUid())
		}
		if req.GetPidsLimit() != 64 || req.GetLogBytesLimit() != 1<<20 || req.GetMaxLifetimeSeconds() != 60 {
			t.Errorf("fork limits = %d pids, %d log bytes, %ds, want 64, %d, 60", req.GetPidsLimit(), req.GetLogBytesLimit(), req.GetMaxLifetimeSeconds(), 1<<20)
		}
		if got := req.GetResources().GetLimits().GetMemoryBytes(); got != 256<<20 {
			t.Errorf("fork memory limit = %d, want the template's private limit %d", got, 256<<20)
		}
		wantEnv := map[string]string{"ATE_ACTOR_ATESPACE": forkAtespace, "ATE_ACTOR_NAME": "forked", "ATE_ACTOR_UID": forked.GetMetadata().GetUid()}
		gotEnv := map[string]string{}
		for _, env := range req.GetEnv() {
			gotEnv[env.GetName()] = env.GetValue()
		}
		if len(gotEnv) != len(wantEnv) {
			t.Errorf("fork env = %v, want only %v", gotEnv, wantEnv)
		}
		for k, v := range wantEnv {
			if gotEnv[k] != v {
				t.Errorf("fork env %s = %q, want %q", k, gotEnv[k], v)
			}
		}
		// A forked actor books its private limits plus the fork overhead.
		assignment, err := e.st.GetWorkerAssignment(ctx, worker.GetMetadata().GetName(), forked.GetMetadata().GetUid())
		if err != nil {
			t.Fatalf("GetWorkerAssignment: %v", err)
		}
		if got, want := assignment.GetResources().GetMemoryBytes(), int64(256<<20+16<<20); got != want {
			t.Errorf("booked memory = %d, want the limit plus the fork overhead %d", got, want)
		}
	})

	t.Run("a second resume is refused", func(t *testing.T) {
		e := newForkEnv(t)
		worker := e.addWorker("pod-1")
		e.seedReady(worker, forkAtespace)
		forked := e.newForkActor("forked")
		ref := resources.ActorRefFromActor(forked)
		if _, _, err := e.workflow.ResumeActor(context.Background(), ref); err != nil {
			t.Fatal(err)
		}
		// RUNNING is the router's hot path and stays a no-op.
		if _, resumed, err := e.workflow.ResumeActor(context.Background(), ref); err != nil || resumed {
			t.Errorf("ResumeActor() of a RUNNING forked actor = %v, %v, want a no-op", resumed, err)
		}
		mustUpdateActorStatus(t, context.Background(), e.st, e.actor("forked"), func(s *ateapi.ActorStatus) {
			s.State = ateapi.ActorState_ACTOR_STATE_CRASHED
		})
		if _, _, err := e.workflow.ResumeActor(context.Background(), ref); apierror.Code(err) != codes.FailedPrecondition {
			t.Errorf("ResumeActor() of a CRASHED forked actor = %v, want FailedPrecondition", err)
		}
	})

	tests := []struct {
		name  string
		setup func(e *forkFixture)
		want  codes.Code
	}{{
		name:  "no worker holds a seed",
		setup: func(e *forkFixture) { e.addWorker("pod-1") },
		want:  codes.ResourceExhausted,
	}, {
		name: "the seed serves another atespace",
		setup: func(e *forkFixture) {
			worker := e.addWorker("pod-1")
			e.seedReady(worker, "team-b")
		},
		want: codes.ResourceExhausted,
	}, {
		name: "the seed belongs to an earlier worker process",
		setup: func(e *forkFixture) {
			worker := e.addWorker("pod-1")
			e.seedReady(worker, forkAtespace)
			if _, err := e.st.UpdateWorker(context.Background(), worker.GetMetadata().GetName(), store.PreconditionFrom(e.worker1("pod-1")), func(w *ateapi.Worker) error {
				w.Status.RegisteredEpoch = registeredEpoch + 1
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		},
		want: codes.ResourceExhausted,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newForkEnv(t)
			tt.setup(e)
			forked := e.newForkActor("forked")
			_, _, err := e.workflow.ResumeActor(context.Background(), resources.ActorRefFromActor(forked))
			if got := apierror.Code(err); got != tt.want {
				t.Fatalf("ResumeActor() = %v, code %v, want %v", err, got, tt.want)
			}
			if _, forks, _, _ := e.atelet.counts(); forks != 0 {
				t.Errorf("atelet saw %d fork calls, want none", forks)
			}
			if got := e.actor("forked").GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
				t.Errorf("state = %v, want SUSPENDED", got)
			}
		})
	}

	t.Run("atelet cannot find the seed", func(t *testing.T) {
		e := newForkEnv(t)
		worker := e.addWorker("pod-1")
		e.seedReady(worker, forkAtespace)
		e.atelet.forkErr = status.Error(codes.NotFound, "no such seed")
		forked := e.newForkActor("forked")
		if _, _, err := e.workflow.ResumeActor(context.Background(), resources.ActorRefFromActor(forked)); err == nil {
			t.Fatal("ResumeActor() succeeded without a seed at atelet")
		}
		stored := e.actor("forked")
		if got := stored.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_CRASHED {
			t.Errorf("state = %v, want CRASHED, which the reaper deletes", got)
		}
		if stored.GetStatus().GetWorkerAssignment() != nil {
			t.Error("a crashed forked actor keeps its worker")
		}
	})

	t.Run("a template that is no longer a fork template", func(t *testing.T) {
		e := newForkEnv(t)
		worker := e.addWorker("pod-1")
		e.seedReady(worker, forkAtespace)
		forked := e.newForkActor("forked")
		plain := forkTemplateSpec(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Metadata.Name = "plain"
			tmpl.ForkConfig = nil
		})
		if _, err := e.st.CreateActorTemplate(context.Background(), plain); err != nil {
			t.Fatal(err)
		}
		// A normal actor retargeted at a fork template is refused too.
		normal := storetest.MustCreateActor(t, context.Background(), e.st, &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: forkAtespace, Name: "normal"},
			ActorTemplate: &ateapi.ObjectRef{Atespace: forkAtespace, Name: forkTemplate},
			Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
		})
		if _, _, err := e.workflow.ResumeActor(context.Background(), resources.ActorRefFromActor(normal)); apierror.Code(err) != codes.FailedPrecondition {
			t.Errorf("ResumeActor() of a plain actor on a fork template = %v, want FailedPrecondition", err)
		}
		_ = forked
	})

	t.Run("the gate is off", func(t *testing.T) {
		e := newForkEnv(t)
		worker := e.addWorker("pod-1")
		e.seedReady(worker, forkAtespace)
		forked := e.newForkActor("forked")
		preview.InitForTest(t)
		if _, _, err := e.workflow.ResumeActor(context.Background(), resources.ActorRefFromActor(forked)); apierror.Code(err) != codes.FailedPrecondition {
			t.Errorf("ResumeActor() with the gate off = %v, want FailedPrecondition", err)
		}
		if _, forks, _, _ := e.atelet.counts(); forks != 0 {
			t.Errorf("atelet saw %d fork calls with the gate off", forks)
		}
	})
}

func TestExitActorReportedByWorker(t *testing.T) {
	start := func(t *testing.T) (*forkFixture, *ateapi.Actor) {
		e := newForkEnv(t)
		worker := e.addWorker("pod-1")
		e.seedReady(worker, forkAtespace)
		forked := e.newForkActor("forked")
		running, _, err := e.workflow.ResumeActor(context.Background(), resources.ActorRefFromActor(forked))
		if err != nil {
			t.Fatalf("ResumeActor: %v", err)
		}
		return e, running
	}
	workerName := testWorkerUID("pod-1")

	t.Run("a single-use actor is deleted on exit", func(t *testing.T) {
		e, running := start(t)
		ref := resources.ActorRefFromActor(running)
		// The exit is on the record while the delete runs.
		var exitCode int32 = -1
		e.atelet.onTerminate = func(*ateletpb.TerminateRequest) {
			exitCode = e.actor("forked").GetStatus().GetFork().GetExitCode()
		}
		if _, err := e.workflow.ExitActorReportedByWorker(context.Background(), ref, running.GetMetadata().GetUid(), workerName, 3); err != nil {
			t.Fatalf("ExitActorReportedByWorker() = %v", err)
		}
		if exitCode != 3 {
			t.Errorf("exit_code on record during the delete = %d, want 3", exitCode)
		}
		if _, err := e.st.GetActor(context.Background(), ref); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("GetActor() after the exit = %v, want not found", err)
		}
		if _, err := e.st.GetWorkerAssignment(context.Background(), workerName, running.GetMetadata().GetUid()); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("the worker still hosts the exited actor: %v", err)
		}
	})

	refused := []struct {
		name string
		uid  string
		wkr  string
		want codes.Code
	}{
		{"a stale incarnation", "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21", workerName, codes.NotFound},
		{"another worker", "", "some-other-worker", codes.NotFound},
	}
	for _, tt := range refused {
		t.Run(tt.name, func(t *testing.T) {
			e, running := start(t)
			uid := tt.uid
			if uid == "" {
				uid = running.GetMetadata().GetUid()
			}
			_, err := e.workflow.ExitActorReportedByWorker(context.Background(), resources.ActorRefFromActor(running), uid, tt.wkr, 0)
			if got := apierror.Code(err); got != tt.want {
				t.Fatalf("ExitActorReportedByWorker() = %v, code %v, want %v", err, got, tt.want)
			}
			if got := e.actor("forked").GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_RUNNING {
				t.Errorf("a refused report left the actor %v", got)
			}
		})
	}

	t.Run("an exit that is not a forked actor's", func(t *testing.T) {
		e := newForkEnv(t)
		ctx := context.Background()
		worker := e.addWorker("pod-1")
		normal := storetest.MustCreateActor(t, ctx, e.st, &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: forkAtespace, Name: "normal"},
			ActorTemplate: &ateapi.ObjectRef{Atespace: forkAtespace, Name: forkTemplate},
			Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_RUNNING},
		})
		seedAssignment(t, e.st, worker.GetMetadata().GetName(), &ateapi.ActorAssignment{Actor: &ateapi.ObjectRef{Atespace: forkAtespace, Name: "normal"}, ActorUid: normal.GetMetadata().GetUid()})
		mustUpdateActorStatus(t, ctx, e.st, normal, func(s *ateapi.ActorStatus) {
			s.WorkerAssignment = &ateapi.WorkerAssignment{Worker: &ateapi.ObjectRef{Name: worker.GetMetadata().GetName()}}
		})
		_, err := e.workflow.ExitActorReportedByWorker(ctx, resources.ActorRefFromActor(normal), normal.GetMetadata().GetUid(), worker.GetMetadata().GetName(), 0)
		if apierror.Code(err) != codes.FailedPrecondition {
			t.Errorf("ExitActorReportedByWorker() of a plain actor = %v, want FailedPrecondition", err)
		}
	})

	t.Run("a second report finds the actor gone", func(t *testing.T) {
		e, running := start(t)
		ref := resources.ActorRefFromActor(running)
		if _, err := e.workflow.ExitActorReportedByWorker(context.Background(), ref, running.GetMetadata().GetUid(), workerName, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := e.workflow.ExitActorReportedByWorker(context.Background(), ref, running.GetMetadata().GetUid(), workerName, 0); apierror.Code(err) != codes.NotFound {
			t.Errorf("a repeated ExitActorReportedByWorker() = %v, want NotFound", err)
		}
	})
}

func TestCrashActorReportedByWorker_UnlistsSeed(t *testing.T) {
	e := newForkEnv(t)
	worker := e.addWorker("pod-1")
	seed := e.seedReady(worker, forkAtespace)
	if _, err := e.workflow.CrashActorReportedByWorker(context.Background(), resources.ActorRefFromActor(seed), seed.GetMetadata().GetUid(), worker.GetMetadata().GetName(), "sandbox exited"); err != nil {
		t.Fatalf("CrashActorReportedByWorker() = %v", err)
	}
	if got := e.worker1("pod-1").GetStatus().GetSeeds(); len(got) != 0 {
		t.Errorf("a crashed seed is still listed on its worker: %v", got)
	}
}

func TestDeleteActorTemplate_DeletesSeeds(t *testing.T) {
	e := newForkEnv(t)
	ctx := context.Background()
	w1 := e.addWorker("pod-1")
	e.addWorker("pod-2") // no seed here
	seed := e.seedReady(w1, forkAtespace)
	if _, err := e.workflow.DeleteActorTemplate(ctx, resources.ActorTemplateRefFromActorTemplate(e.template), store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteActorTemplate() = %v", err)
	}
	if _, err := e.st.GetActor(ctx, resources.ActorRefFromActor(seed)); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetActor(seed) after the template is deleted = %v, want not found", err)
	}
	if got := e.worker1("pod-1").GetStatus().GetSeeds(); len(got) != 0 {
		t.Errorf("seeds of a deleted template are still listed: %v", got)
	}
	if _, _, drops, _ := e.atelet.counts(); drops != 0 {
		// The seed in this test holds no assignment, so there is no node to call.
		t.Logf("DropSeed calls = %d", drops)
	}
}
