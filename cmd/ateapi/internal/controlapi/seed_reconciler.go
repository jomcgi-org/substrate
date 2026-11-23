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
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/scheduling"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	// seedRetryBackoff is how long a crashed seed stays before the reconciler
	// deletes it and builds another, so a seed that cannot start does not
	// rebuild in a loop.
	seedRetryBackoff = time.Minute

	// seedWorkerConcurrency bounds the seeds the reconciler builds at once.
	seedWorkerConcurrency = 8

	seedListPageSize = 100
)

// seedReconcilerStore enumerates the exact storage methods the SeedReconciler
// needs and nothing more.
type seedReconcilerStore interface {
	ListActorTemplates(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapi.ActorTemplate], error)
	GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapi.ActorTemplate, error)
	ListWorkers(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapi.Worker], error)
	GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapi.Actor, error)
	CreateActor(ctx context.Context, actor *ateapi.Actor) (*ateapi.Actor, error)
	GetWorker(ctx context.Context, name string) (*ateapi.Worker, error)
	UpdateWorker(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapi.Worker) error) (*ateapi.Worker, error)
	AcquireLease(ctx context.Context, key string) (*store.Lease, error)
}

// seedControl is the in-process slice of the Control service the reconciler
// drives seed Actors through. *RPCService satisfies it.
type seedControl interface {
	ResumeActor(ctx context.Context, req *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error)
	DeleteActor(ctx context.Context, req *ateapi.DeleteActorRequest) (*ateapi.Actor, error)
}

// SeedReconciler keeps one seed Actor per ActorTemplate with fork_config on
// every eligible Worker. A seed is a system Actor pinned to its Worker. It
// builds the seed sandbox from the template, and the scheduler places forked
// actors on Workers that list one.
//
// Each pass is level-triggered: it reads every fork template, Worker and seed
// Actor, and takes the action each needs. A pass runs under one lease, so the
// atespace rule below holds across replicas.
//
// A Worker holds seeds of one atespace only. Seeds of two atespaces in one
// worker process would share clean pages and a cache-timing channel, so a Worker
// that holds a seed of one atespace gets none of another.
type SeedReconciler struct {
	persistence    seedReconcilerStore
	control        seedControl
	resyncInterval time.Duration
	// now is replaced in tests.
	now func() time.Time
}

func NewSeedReconciler(persistence seedReconcilerStore, control seedControl, resyncInterval time.Duration) *SeedReconciler {
	return &SeedReconciler{persistence: persistence, control: control, resyncInterval: resyncInterval, now: time.Now}
}

// Start runs a pass every resync interval until ctx ends.
func (r *SeedReconciler) Start(ctx context.Context) {
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := r.Reconcile(ctx); err != nil {
			slog.ErrorContext(ctx, "Seed reconciliation failed", slog.Any("err", err))
		}
	}, r.resyncInterval)
}

type seedActionKind int

const (
	seedCreate seedActionKind = iota // create the seed Actor, then resume it
	seedResume                       // resume a SUSPENDED or RESUMING seed
	seedDelete                       // delete a seed
)

type seedAction struct {
	kind     seedActionKind
	template *ateapi.ActorTemplate
	worker   *ateapi.Worker
	seed     *ateapi.Actor // nil for seedCreate
	reason   string
}

// Reconcile runs one pass.
func (r *SeedReconciler) Reconcile(ctx context.Context) error {
	if !preview.IsEnabled(preview.GateForkSeed) {
		return nil
	}
	lease, err := r.persistence.AcquireLease(ctx, "lease:seeds")
	if err != nil {
		if errors.Is(err, store.ErrLeaseConflict) {
			return nil // another replica runs this pass
		}
		return fmt.Errorf("while acquiring lease: %w", err)
	}
	defer lease.Close()
	ctx = withSystemPrincipal(preview.ContextWithOptIn(lease.Context()))

	templates, err := r.forkTemplates(ctx)
	if err != nil {
		return err
	}
	workers, err := r.workers(ctx)
	if err != nil {
		return err
	}
	actions, err := r.plan(ctx, templates, workers)
	if err != nil {
		return err
	}
	var errs []error
	var group errgroup.Group
	group.SetLimit(seedWorkerConcurrency)
	results := make(chan error, len(actions))
	for _, action := range actions {
		group.Go(func() error {
			results <- r.apply(ctx, action)
			return nil
		})
	}
	_ = group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if err := r.pruneWorkerSeeds(ctx, workers); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (r *SeedReconciler) forkTemplates(ctx context.Context) ([]*ateapi.ActorTemplate, error) {
	var out []*ateapi.ActorTemplate
	for token := ""; ; {
		page, err := r.persistence.ListActorTemplates(ctx, "", store.ListOptions{PageSize: seedListPageSize, PageToken: token})
		if err != nil {
			return nil, fmt.Errorf("while listing actor templates: %w", err)
		}
		for _, tmpl := range page.Items {
			if tmpl.GetForkConfig() != nil && tmpl.GetMetadata().GetDeleteTime() == nil {
				out = append(out, tmpl)
			}
		}
		if page.NextPageToken == "" {
			return out, nil
		}
		token = page.NextPageToken
	}
}

func (r *SeedReconciler) workers(ctx context.Context) ([]*ateapi.Worker, error) {
	var out []*ateapi.Worker
	for token := ""; ; {
		page, err := r.persistence.ListWorkers(ctx, store.ListOptions{PageSize: seedListPageSize, PageToken: token})
		if err != nil {
			return nil, fmt.Errorf("while listing workers: %w", err)
		}
		out = append(out, page.Items...)
		if page.NextPageToken == "" {
			return out, nil
		}
		token = page.NextPageToken
	}
}

// eligible reports whether a seed of tmpl belongs on worker.
func eligible(worker *ateapi.Worker, tmpl *ateapi.ActorTemplate) bool {
	if !scheduling.ForkCapable(worker) || worker.GetStatus().GetState() != ateapi.WorkerState_WORKER_STATE_ACTIVE {
		return false
	}
	if worker.GetMetadata().GetDeleteTime() != nil {
		return false
	}
	if sel := tmpl.GetWorkerSelector(); sel != nil {
		return labels.SelectorFromSet(labels.Set(sel.GetMatchLabels())).Matches(labels.Set(worker.GetLabels()))
	}
	return true
}

// plan reads the seed Actor of every template and worker pair, and decides
// what each needs.
func (r *SeedReconciler) plan(ctx context.Context, templates []*ateapi.ActorTemplate, workers []*ateapi.Worker) ([]seedAction, error) {
	type pair struct {
		template *ateapi.ActorTemplate
		worker   *ateapi.Worker
		seed     *ateapi.Actor
	}
	var pairs []pair
	// claimed is the atespace of the seeds each Worker already holds, whether
	// or not they run yet.
	claimed := map[string]string{}
	for _, tmpl := range templates {
		for _, worker := range workers {
			ref := resources.ActorRef{Atespace: tmpl.GetMetadata().GetAtespace(), Name: seedActorName(tmpl.GetMetadata().GetUid(), worker.GetMetadata().GetName())}
			seed, err := r.persistence.GetActor(ctx, ref)
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("while getting seed %s: %w", ref, err)
			}
			if err == nil {
				claimed[worker.GetMetadata().GetName()] = tmpl.GetMetadata().GetAtespace()
			}
			pairs = append(pairs, pair{tmpl, worker, seed})
		}
	}

	var actions []seedAction
	for _, p := range pairs {
		name := p.worker.GetMetadata().GetName()
		ok := eligible(p.worker, p.template)
		if p.seed == nil {
			if !ok {
				continue
			}
			if owner := claimed[name]; owner != "" && owner != p.template.GetMetadata().GetAtespace() {
				continue // the Worker serves another atespace
			}
			claimed[name] = p.template.GetMetadata().GetAtespace()
			actions = append(actions, seedAction{kind: seedCreate, template: p.template, worker: p.worker, reason: "no seed"})
			continue
		}
		switch state := p.seed.GetStatus().GetState(); {
		case !ok:
			actions = append(actions, seedAction{kind: seedDelete, template: p.template, worker: p.worker, seed: p.seed, reason: "worker is no longer eligible"})
		case state == ateapi.ActorState_ACTOR_STATE_DELETING:
			actions = append(actions, seedAction{kind: seedDelete, template: p.template, worker: p.worker, seed: p.seed, reason: "deletion unfinished"})
		case state == ateapi.ActorState_ACTOR_STATE_CRASHED:
			if r.now().Sub(p.seed.GetStatus().GetCrash().GetCrashTime().AsTime()) >= seedRetryBackoff {
				actions = append(actions, seedAction{kind: seedDelete, template: p.template, worker: p.worker, seed: p.seed, reason: "crashed"})
			}
		case state == ateapi.ActorState_ACTOR_STATE_SUSPENDED || state == ateapi.ActorState_ACTOR_STATE_RESUMING:
			actions = append(actions, seedAction{kind: seedResume, template: p.template, worker: p.worker, seed: p.seed, reason: "not running"})
		case state == ateapi.ActorState_ACTOR_STATE_RUNNING:
			// Listed again if an earlier listing was lost.
			workerName := p.worker.GetMetadata().GetName()
			epoch := p.seed.GetStatus().GetWorkerAssignment().GetWorkerEpoch()
			if epoch != p.worker.GetStatus().GetRegisteredEpoch() {
				continue // the assignment reconciler crashes it
			}
			if err := recordSeedReady(ctx, r.persistence, workerName, p.template.GetMetadata().GetUid(), p.seed, epoch); err != nil {
				return nil, err
			}
		}
	}
	return actions, nil
}

// apply carries out one action. Delete, resume and create each pick up where
// an earlier attempt stopped.
func (r *SeedReconciler) apply(ctx context.Context, a seedAction) error {
	switch a.kind {
	case seedCreate:
		seed, err := r.createSeed(ctx, a.template, a.worker)
		if err != nil {
			return err
		}
		return r.resume(ctx, seed)
	case seedResume:
		return r.resume(ctx, a.seed)
	case seedDelete:
		slog.InfoContext(ctx, "Deleting a seed", slog.String("seed", a.seed.GetMetadata().GetName()), slog.String("reason", a.reason))
		_, err := r.control.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: &ateapi.ObjectRef{Atespace: a.seed.GetMetadata().GetAtespace(), Name: a.seed.GetMetadata().GetName()}})
		return err
	}
	return nil
}

func (r *SeedReconciler) resume(ctx context.Context, seed *ateapi.Actor) error {
	_, err := r.control.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: seed.GetMetadata().GetAtespace(), Name: seed.GetMetadata().GetName()}})
	if err != nil {
		return fmt.Errorf("while resuming seed %s/%s: %w", seed.GetMetadata().GetAtespace(), seed.GetMetadata().GetName(), err)
	}
	return nil
}

func (r *SeedReconciler) createSeed(ctx context.Context, tmpl *ateapi.ActorTemplate, worker *ateapi.Worker) (*ateapi.Actor, error) {
	// DeleteActorTemplate removes the seeds of a template once, so a seed
	// created after it would stay.
	current, err := r.persistence.GetActorTemplate(ctx, resources.ActorTemplateRefFromActorTemplate(tmpl))
	if err != nil {
		return nil, fmt.Errorf("while getting template %s: %w", resources.ActorTemplateRefFromActorTemplate(tmpl), err)
	}
	if current.GetMetadata().GetDeleteTime() != nil || current.GetMetadata().GetUid() != tmpl.GetMetadata().GetUid() {
		return nil, fmt.Errorf("template %s is being deleted", resources.ActorTemplateRefFromActorTemplate(tmpl))
	}
	seed := &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: tmpl.GetMetadata().GetAtespace(), Name: seedActorName(tmpl.GetMetadata().GetUid(), worker.GetMetadata().GetName())},
		ActorTemplate: &ateapi.ObjectRef{Atespace: tmpl.GetMetadata().GetAtespace(), Name: tmpl.GetMetadata().GetName()},
		Status: &ateapi.ActorStatus{
			State: ateapi.ActorState_ACTOR_STATE_SUSPENDED,
			Fork: &ateapi.ActorFork{
				Role:         ateapi.ForkRole_FORK_ROLE_SEED,
				PinnedWorker: &ateapi.ObjectRef{Name: worker.GetMetadata().GetName()},
			},
		},
	}
	if errs := apivalidation.ValidateActorUpdate(ctx, field.NewPath("actor"), seed, seed, true); len(errs) > 0 {
		return nil, resources.ToInternalError(errs)
	}
	stored, err := r.persistence.CreateActor(ctx, seed)
	if errors.Is(err, store.ErrAlreadyExists) {
		return r.persistence.GetActor(ctx, resources.ActorRefFromActor(seed))
	}
	if err != nil {
		return nil, fmt.Errorf("while creating seed %s: %w", resources.ActorRefFromActor(seed), err)
	}
	logActorStateChanged(ctx, stored, "create")
	return stored, nil
}

// pruneWorkerSeeds removes the entries of seeds that no longer run. A seed
// that crashed or was deleted took its sandbox with it.
func (r *SeedReconciler) pruneWorkerSeeds(ctx context.Context, workers []*ateapi.Worker) error {
	var errs []error
	for _, worker := range workers {
		for _, entry := range worker.GetStatus().GetSeeds() {
			seed, err := r.persistence.GetActor(ctx, resources.ActorRefFromObjectRef(entry.GetSeedActor()))
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				errs = append(errs, err)
				continue
			}
			if err == nil && seed.GetMetadata().GetUid() == entry.GetSeedActorUid() && seed.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_RUNNING {
				continue
			}
			if err := removeSeed(ctx, r.persistence, worker.GetMetadata().GetName(), entry.GetSeedActorUid()); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}
