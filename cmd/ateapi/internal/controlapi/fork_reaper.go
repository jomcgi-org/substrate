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
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
	"google.golang.org/grpc/codes"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	// forkDeadlineGrace is how long past max_lifetime_seconds a RUNNING forked
	// actor lives before the reaper deletes it. The worker terminates the actor
	// at its deadline, so the reaper only acts when that did not happen.
	forkDeadlineGrace = 30 * time.Second

	// forkUnstartedAge is how long a forked actor may stay SUSPENDED or
	// RESUMING before the reaper deletes it, at least. A forked actor starts
	// once, right after it is created, so one that waits has been abandoned or
	// its resume was lost.
	forkUnstartedAge = 5 * time.Minute
)

// forkReaperStore enumerates the exact storage methods the ForkReaper needs.
type forkReaperStore interface {
	ListActorTemplates(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapi.ActorTemplate], error)
	ListActors(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapi.Actor], error)
}

// forkReaperControl is the in-process slice of the Control service the reaper
// deletes Actors through. *RPCService satisfies it.
type forkReaperControl interface {
	DeleteActor(ctx context.Context, req *ateapi.DeleteActorRequest) (*ateapi.Actor, error)
}

// ForkReaper deletes the forked actors that did not delete themselves. The
// normal end of a forked actor is its workload exiting, which the worker
// reports. The reaper is the belt and braces for the rest: a worker that died
// before it reported, a worker that missed the deadline, a crash, and an Actor
// that never started.
type ForkReaper struct {
	persistence    forkReaperStore
	control        forkReaperControl
	resyncInterval time.Duration
	// now is replaced in tests.
	now func() time.Time
}

func NewForkReaper(persistence forkReaperStore, control forkReaperControl, resyncInterval time.Duration) *ForkReaper {
	return &ForkReaper{persistence: persistence, control: control, resyncInterval: resyncInterval, now: time.Now}
}

// Start runs a pass every resync interval until ctx ends.
func (r *ForkReaper) Start(ctx context.Context) {
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := r.Reap(ctx); err != nil {
			slog.ErrorContext(ctx, "Fork reaping failed", slog.Any("err", err))
		}
	}, r.resyncInterval)
}

// Reap runs one pass over the atespaces that hold a template with fork_config.
// Forked actors live in the atespace of their template, so no other atespace
// needs listing.
func (r *ForkReaper) Reap(ctx context.Context) error {
	if !preview.IsEnabled(preview.GateForkSeed) {
		return nil
	}
	ctx = withSystemPrincipal(preview.ContextWithOptIn(ctx))
	templates, err := r.forkTemplates(ctx)
	if err != nil {
		return err
	}
	atespaces := map[string]bool{}
	for _, tmpl := range templates {
		atespaces[tmpl.GetMetadata().GetAtespace()] = true
	}
	var errs []error
	for atespace := range atespaces {
		if err := r.reapAtespace(ctx, atespace, templates); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// forkTemplates returns every template with fork_config, deleted ones included:
// a forked actor of a template that is going away still needs its deadline.
func (r *ForkReaper) forkTemplates(ctx context.Context) ([]*ateapi.ActorTemplate, error) {
	var out []*ateapi.ActorTemplate
	for token := ""; ; {
		page, err := r.persistence.ListActorTemplates(ctx, "", store.ListOptions{PageSize: seedListPageSize, PageToken: token})
		if err != nil {
			return nil, fmt.Errorf("while listing actor templates: %w", err)
		}
		for _, tmpl := range page.Items {
			if tmpl.GetForkConfig() != nil {
				out = append(out, tmpl)
			}
		}
		if page.NextPageToken == "" {
			return out, nil
		}
		token = page.NextPageToken
	}
}

func (r *ForkReaper) reapAtespace(ctx context.Context, atespace string, templates []*ateapi.ActorTemplate) error {
	maxLifetime := map[string]time.Duration{}
	for _, tmpl := range templates {
		if tmpl.GetMetadata().GetAtespace() == atespace {
			maxLifetime[tmpl.GetMetadata().GetName()] = time.Duration(tmpl.GetForkConfig().GetMaxLifetimeSeconds()) * time.Second
		}
	}
	var errs []error
	for token := ""; ; {
		page, err := r.persistence.ListActors(ctx, atespace, store.ListOptions{PageSize: seedListPageSize, PageToken: token})
		if err != nil {
			return fmt.Errorf("while listing actors in atespace %s: %w", atespace, err)
		}
		for _, actor := range page.Items {
			lifetime, known := maxLifetime[actor.GetActorTemplate().GetName()]
			reason := r.reason(actor, lifetime, known)
			if reason == "" {
				continue
			}
			slog.InfoContext(ctx, "Reaping a forked actor", slog.String("actor", actor.GetMetadata().GetName()), slog.String("atespace", atespace), slog.String("reason", reason))
			_, err := r.control.DeleteActor(ctx, &ateapi.DeleteActorRequest{
				Actor:   &ateapi.ObjectRef{Atespace: atespace, Name: actor.GetMetadata().GetName()},
				Options: &ateapi.DeleteOptions{Uid: actor.GetMetadata().GetUid()},
			})
			if err != nil && apierror.Code(err) != codes.NotFound {
				errs = append(errs, fmt.Errorf("while deleting %s/%s: %w", atespace, actor.GetMetadata().GetName(), err))
			}
		}
		if page.NextPageToken == "" {
			return errors.Join(errs...)
		}
		token = page.NextPageToken
	}
}

// reason says why actor is due for deletion, or "" when it is not. A seed is
// the reconciler's, except one whose template is gone.
func (r *ForkReaper) reason(actor *ateapi.Actor, lifetime time.Duration, templateKnown bool) string {
	now := r.now()
	switch forkRole(actor) {
	case ateapi.ForkRole_FORK_ROLE_SEED:
		if !templateKnown {
			return "template with fork_config is gone"
		}
		return ""
	case ateapi.ForkRole_FORK_ROLE_FORK:
	default:
		return ""
	}
	switch state := actor.GetStatus().GetState(); state {
	case ateapi.ActorState_ACTOR_STATE_CRASHED:
		return "crashed"
	case ateapi.ActorState_ACTOR_STATE_DELETING:
		return "deletion unfinished"
	case ateapi.ActorState_ACTOR_STATE_RUNNING:
		start := actor.GetStatus().GetFork().GetStartTime()
		if start == nil {
			return ""
		}
		if now.After(start.AsTime().Add(lifetime + forkDeadlineGrace)) {
			return "past max_lifetime_seconds"
		}
	case ateapi.ActorState_ACTOR_STATE_SUSPENDED, ateapi.ActorState_ACTOR_STATE_RESUMING:
		age := max(lifetime, forkUnstartedAge)
		if now.After(actor.GetMetadata().GetUpdateTime().AsTime().Add(age)) {
			return "never started"
		}
	}
	return ""
}
