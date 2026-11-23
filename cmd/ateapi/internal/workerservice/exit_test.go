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

package workerservice

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
)

func exitRequest(actorUID string, exitCode int32) *ateapi.ReportActorExitedRequest {
	return &ateapi.ReportActorExitedRequest{
		Worker:   &ateapi.ObjectRef{Name: testWorkerName},
		Actor:    &ateapi.ObjectRef{Atespace: suspendAtespace, Name: suspendActor},
		ActorUid: actorUID,
		ExitCode: exitCode,
	}
}

func TestReportActorExited(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	seedReportedWorker(t, st, testNode, &ateapi.WorkerResources{Actors: 1})
	actor := seedHostedActor(t, st, testWorkerName)
	uid := actor.GetMetadata().GetUid()
	control := &fakeControl{exitResp: actor}
	s := New(st, control, testAteletSPIFFEID, nil, "", nil)

	resp, err := s.ReportActorExited(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), exitRequest(uid, 3))
	if err != nil {
		t.Fatalf("ReportActorExited() failed: %v", err)
	}
	if resp.GetActor().GetMetadata().GetUid() != uid {
		t.Errorf("ReportActorExited() actor = %v, want the one the control plane returned", resp.GetActor())
	}
	want := []exitCall{{
		ActorRef:   resources.ActorRef{Atespace: suspendAtespace, Name: suspendActor},
		ActorUID:   uid,
		WorkerName: testWorkerName,
		ExitCode:   3,
	}}
	if diff := cmp.Diff(want, control.exitCalls); diff != "" {
		t.Errorf("exit calls mismatch (-want +got):\n%s", diff)
	}
}

func TestReportActorExited_Refused(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	seedReportedWorker(t, st, testNode, &ateapi.WorkerResources{Actors: 1})
	actor := seedHostedActor(t, st, testWorkerName)
	uid := actor.GetMetadata().GetUid()
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	badCode := exitRequest(uid, 300)
	noUID := exitRequest("", 0)

	tests := []struct {
		name string
		ctx  context.Context
		req  *ateapi.ReportActorExitedRequest
		want codes.Code
	}{
		{"unauthenticated", ateletauthtest.ContextWith(nil), exitRequest(uid, 0), codes.Unauthenticated},
		{"worker on another node", ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "some-other-node")), exitRequest(uid, 0), codes.NotFound},
		{"stale actor incarnation", authed, exitRequest("3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21", 0), codes.NotFound},
		{"no actor uid", authed, noUID, codes.InvalidArgument},
		{"exit code out of range", authed, badCode, codes.InvalidArgument},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			control := &fakeControl{}
			s := New(st, control, testAteletSPIFFEID, nil, "", nil)
			_, err := s.ReportActorExited(tc.ctx, tc.req)
			if got := apierror.Code(err); got != tc.want {
				t.Fatalf("code = %v (err %v), want %v", got, err, tc.want)
			}
			if len(control.exitCalls) != 0 {
				t.Errorf("refused report still reached the delete: %v", control.exitCalls)
			}
		})
	}
}
