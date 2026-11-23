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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"go.ate.dev/substrate/cmd/atelet/internal/actordirs"
	"go.ate.dev/substrate/cmd/atelet/internal/apivalidation"
	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/atomicfile"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// seedRecord is what atelet keeps of a seed Actor: whom it was built for.
// Fork checks a request against it, so a forged or stale request cannot fork a
// seed that belongs to another atespace, another template or another worker.
type seedRecord struct {
	Atespace      string `json:"atespace"`
	TemplateUID   string `json:"templateUid"`
	WorkerPodUID  string `json:"workerPodUid"`
	RootfsSHA256  string `json:"rootfsSha256"`
	TemplateName  string `json:"templateName"`
	TemplateSpace string `json:"templateAtespace"`
}

func writeSeedRecord(actorUID string, rec *seedRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return wrapFileSystemErr("while marshaling seed record", err)
	}
	if err := atomicfile.Write(ateletpath.SeedRecordFile(actorUID), data, 0o600); err != nil {
		return wrapFileSystemErr("while writing seed record", err)
	}
	return nil
}

func readSeedRecord(actorUID string) (*seedRecord, error) {
	data, err := os.ReadFile(ateletpath.SeedRecordFile(actorUID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, status.Errorf(codes.NotFound, "seed %s is not on this node", actorUID)
		}
		return nil, wrapFileSystemErr("while reading seed record", err)
	}
	rec := &seedRecord{}
	if err := json.Unmarshal(data, rec); err != nil {
		return nil, wrapFileSystemErr("while parsing seed record", err)
	}
	return rec, nil
}

// checkForkSpec holds the workload of a seed or a forked actor to what the
// seed supports: one container and no volumes. The control plane validates the
// template the same way, so this is the check at atelet's own edge.
func checkForkSpec(spec *ateletpb.WorkloadSpec) error {
	if len(spec.GetContainers()) != 1 {
		return apierror.InvalidArgument("spec: a seed has exactly one container, got %d", len(spec.GetContainers()))
	}
	if len(spec.GetVolumes()) != 0 || len(spec.GetContainers()[0].GetVolumeMounts()) != 0 {
		return apierror.InvalidArgument("spec: a seed has no volumes")
	}
	return nil
}

// requireForkSeedGate refuses fork work while the gate is off, so an atelet
// that was started without it does nothing for a control plane that has it.
func requireForkSeedGate() error {
	if !preview.IsEnabled(preview.GateForkSeed) {
		return apierror.FailedPrecondition("seeds need the ForkSeed preview gate on atelet")
	}
	return nil
}

// stageActivation fetches the sandbox runtime assets, resets the actor's
// directories and records the sandbox, as Run does before it calls the worker.
// The assets go in under a staging name first, so a failed fetch leaves a
// live activation as it was, and commitSandboxAssets moves them and their
// paths to the final directory. The returned func removes what was staged if
// the activation fails.
func (s *Atelet) stageActivation(ctx context.Context, actorUID string, sandboxRec *sandboxAssetsRecord) (assetPaths map[string]string, undo func(), err error) {
	staging := ateletpath.SandboxAssetsStagingDir(actorUID)
	if err := os.RemoveAll(staging); err != nil {
		return nil, nil, wrapFileSystemErr("while removing a stale sandbox assets staging dir", err)
	}
	assetPaths, err = s.ensureSandboxAssets(ctx, sandboxRec, staging)
	if err != nil {
		_ = os.RemoveAll(staging)
		return nil, nil, err
	}
	undo = func() {
		_ = os.RemoveAll(staging)
		_ = os.RemoveAll(ateletpath.SandboxAssetsDir(actorUID))
	}
	if err := actordirs.ResetActorDirsKeepingVolumes(actorUID); err != nil {
		undo()
		return nil, nil, status.Errorf(codes.Canceled, "while resetting actor dirs: %v", err)
	}
	if err := commitSandboxAssets(actorUID, staging, assetPaths); err != nil {
		undo()
		return nil, nil, err
	}
	if err := writeSandboxRecord(actorUID, sandboxRec); err != nil {
		undo()
		return nil, nil, fmt.Errorf("while recording sandbox assets: %w", err)
	}
	return assetPaths, undo, nil
}

// Seed builds the seed sandbox of a template on the worker. The template image
// becomes a read-only root filesystem image that atelet builds and checks, and
// the worker boots the seed from it.
func (s *Atelet) Seed(ctx context.Context, req *ateletpb.SeedRequest) (resp *ateletpb.SeedResponse, err error) {
	if err := requireForkSeedGate(); err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateSeedRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	if err := checkForkSpec(req.GetSpec()); err != nil {
		return nil, err
	}
	// A seed serves one atespace, the template's.
	if req.GetAtespace() != req.GetActorTemplateAtespace() {
		return nil, apierror.PermissionDenied("a seed lives in the atespace of its template: seed %s/%s, template atespace %s", req.GetAtespace(), req.GetActorName(), req.GetActorTemplateAtespace())
	}
	actorUID := req.GetActorUid()
	sandboxRec, err := s.advertisedRuntime(ctx, req.GetSandboxRuntime())
	if err != nil {
		return nil, err
	}
	unlockActor, err := s.lockActor(ctx, actorUID)
	if err != nil {
		return nil, err
	}
	defer unlockActor()

	assetPaths, undo, err := s.stageActivation(ctx, actorUID, sandboxRec)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			undo()
		}
	}()

	ctr := req.GetSpec().GetContainers()[0]
	img, err := s.imageCache.EnsureImage(ctx, ctr.GetImage())
	if err != nil {
		return nil, fmt.Errorf("in imageCache.EnsureImage(%q): %w", ctr.GetImage(), err)
	}
	containerSpec, err := resolveSeedContainer(img, ctr)
	if err != nil {
		return nil, err
	}
	rootfsPath, rootfsSHA, err := s.ensureSeedRootfs(ctx, actorUID, img)
	if err != nil {
		return nil, err
	}
	if err := writeSeedRecord(actorUID, &seedRecord{
		Atespace:      req.GetAtespace(),
		TemplateUID:   req.GetTemplateUid(),
		WorkerPodUID:  req.GetWorkerPodUid(),
		RootfsSHA256:  rootfsSHA,
		TemplateName:  req.GetActorTemplateName(),
		TemplateSpace: req.GetActorTemplateAtespace(),
	}); err != nil {
		return nil, err
	}

	client, err := s.dialWorker(ctx, req.GetWorkerPodUid())
	if err != nil {
		return nil, err
	}
	spec, err := buildWorkerWorkloadSpec(req.GetSpec(), []*ateworkerpb.ContainerSpec{containerSpec})
	if err != nil {
		return nil, apierror.InvalidArgument("invalid workload spec: %v", err)
	}
	workerResp, err := client.SeedWorkload(ctx, &ateworkerpb.SeedWorkloadRequest{
		Atespace:              req.GetAtespace(),
		ActorName:             req.GetActorName(),
		ActorUid:              actorUID,
		ActorTemplateAtespace: req.GetActorTemplateAtespace(),
		ActorTemplateName:     req.GetActorTemplateName(),
		TemplateUid:           req.GetTemplateUid(),
		RunscPath:             runscPathFor(assetPaths),
		Spec:                  spec,
		RootfsImagePath:       rootfsPath,
		RootfsSha256:          rootfsSHA,
		Resources:             toAteworkerResources(req.GetResources()),
		ActorDirs:             ateletpath.ActorDirs(actorUID),
	})
	if err != nil {
		return nil, workerCallError("SeedWorkload", err)
	}
	return &ateletpb.SeedResponse{WorkerEpoch: workerResp.GetWorkerEpoch(), RootfsSha256: rootfsSHA}, nil
}

// resolveSeedContainer is what the worker builds the seed's process from: the
// argv, environment, user and working directory a normal container resolves
// from its image and template.
func resolveSeedContainer(img *imagecache.Image, ctr *ateletpb.Container) (*ateworkerpb.ContainerSpec, error) {
	var envs []string
	for _, env := range ctr.GetEnv() {
		envs = append(envs, fmt.Sprintf("%s=%s", env.GetName(), env.GetValue()))
	}
	cfg := &img.Config
	args, err := resolveProcessArgs(cfg, ctr.GetCommand(), ctr.GetArgs())
	if err != nil {
		return nil, fmt.Errorf("while resolving process args for container %q: %w", ctr.GetName(), err)
	}
	user, err := resolveImageUser(img)
	if err != nil {
		return nil, fmt.Errorf("while resolving user for container %q: %w", ctr.GetName(), err)
	}
	cwd, err := resolveCwd(cfg)
	if err != nil {
		return nil, fmt.Errorf("while resolving the working directory of container %q: %w", ctr.GetName(), err)
	}
	return &ateworkerpb.ContainerSpec{
		Args:         args,
		Env:          resolveActorEnv(cfg, envs),
		Capabilities: resolveCapabilities(ctr.GetSecurityContext().GetCapabilities()),
		Resources:    toAteworkerResources(ctr.GetResources()),
		User:         user,
		Cwd:          cwd,
	}, nil
}

// Fork starts a forked actor from the seed of the template. Both atelet and the
// worker check that the seed belongs to the request's atespace and template, so
// a request for another tenant's seed fails at either.
func (s *Atelet) Fork(ctx context.Context, req *ateletpb.ForkRequest) (*ateletpb.ForkResponse, error) {
	if err := requireForkSeedGate(); err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateForkRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	if err := checkForkSpec(req.GetSpec()); err != nil {
		return nil, err
	}
	if req.GetAtespace() != req.GetActorTemplateAtespace() {
		return nil, apierror.PermissionDenied("a forked actor lives in the atespace of its template: actor %s/%s, template atespace %s", req.GetAtespace(), req.GetActorName(), req.GetActorTemplateAtespace())
	}
	seed, err := readSeedRecord(req.GetSeedActorUid())
	if err != nil {
		return nil, err
	}
	if seed.Atespace != req.GetAtespace() || seed.TemplateUID != req.GetTemplateUid() || seed.WorkerPodUID != req.GetWorkerPodUid() {
		slog.WarnContext(ctx, "Refusing a fork of a seed that was built for another atespace, template or worker",
			slog.String("seed", req.GetSeedActorUid()), slog.String("atespace", req.GetAtespace()), slog.String("template_uid", req.GetTemplateUid()))
		return nil, apierror.PermissionDenied("seed %s was not built for atespace %s, template %s and worker %s", req.GetSeedActorUid(), req.GetAtespace(), req.GetTemplateUid(), req.GetWorkerPodUid())
	}

	actorUID := req.GetActorUid()
	sandboxRec, err := s.advertisedRuntime(ctx, req.GetSandboxRuntime())
	if err != nil {
		return nil, err
	}
	unlockActor, err := s.lockActor(ctx, actorUID)
	if err != nil {
		return nil, err
	}
	defer unlockActor()
	// The assets are staged for Terminate, which needs the sandbox record to
	// reach the worker. The fork itself uses the seed's runtime.
	_, undo, err := s.stageActivation(ctx, actorUID, sandboxRec)
	if err != nil {
		return nil, err
	}

	client, err := s.dialWorker(ctx, req.GetWorkerPodUid())
	if err != nil {
		undo()
		return nil, err
	}
	spec, err := buildWorkerWorkloadSpec(req.GetSpec(), nil)
	if err != nil {
		undo()
		return nil, apierror.InvalidArgument("invalid workload spec: %v", err)
	}
	env := make([]string, 0, len(req.GetEnv()))
	for _, e := range req.GetEnv() {
		env = append(env, fmt.Sprintf("%s=%s", e.GetName(), e.GetValue()))
	}
	workerResp, err := client.ForkWorkload(ctx, &ateworkerpb.ForkWorkloadRequest{
		Atespace:              req.GetAtespace(),
		ActorName:             req.GetActorName(),
		ActorUid:              actorUID,
		ActorTemplateAtespace: req.GetActorTemplateAtespace(),
		ActorTemplateName:     req.GetActorTemplateName(),
		TemplateUid:           req.GetTemplateUid(),
		SeedActorUid:          req.GetSeedActorUid(),
		SeedWorkerEpoch:       req.GetSeedWorkerEpoch(),
		Spec:                  spec,
		Resources:             toAteworkerResources(req.GetResources()),
		Env:                   env,
		PidsLimit:             req.GetPidsLimit(),
		LogBytesLimit:         req.GetLogBytesLimit(),
		MaxLifetimeSeconds:    req.GetMaxLifetimeSeconds(),
		ActorDirs:             ateletpath.ActorDirs(actorUID),
	})
	if err != nil {
		// The sandbox record stays, so a Terminate can find what to tear down.
		return nil, workerCallError("ForkWorkload", err)
	}
	return &ateletpb.ForkResponse{WorkerEpoch: workerResp.GetWorkerEpoch()}, nil
}

// DropSeed discards a seed on its worker and the node state it was built from.
func (s *Atelet) DropSeed(ctx context.Context, req *ateletpb.DropSeedRequest) (*ateletpb.DropSeedResponse, error) {
	if errs := apivalidation.ValidateDropSeedRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorUID := req.GetActorUid()
	unlockActor, err := s.lockActor(ctx, actorUID)
	if err != nil {
		return nil, err
	}
	defer unlockActor()

	if podUID := req.GetWorkerPodUid(); podUID != "" {
		if _, err := os.Stat(workerSocketPath(podUID)); err == nil {
			client, err := s.dialWorker(ctx, podUID)
			if err != nil {
				return nil, fmt.Errorf("failed to dial worker for drop seed (actor %s): %w", actorUID, err)
			}
			if _, err := client.DropSeed(ctx, &ateworkerpb.DropSeedRequest{
				Atespace:  req.GetAtespace(),
				ActorName: req.GetActorName(),
				ActorUid:  actorUID,
			}); err != nil {
				switch status.Code(err) {
				case codes.NotFound, codes.Unavailable:
					// The worker no longer holds the seed, or is gone with it.
				default:
					return nil, fmt.Errorf("failed calling worker DropSeed (actor %s): %w", actorUID, err)
				}
			}
		}
	}
	if err := actordirs.RemoveActorDirs(actorUID); err != nil {
		return nil, fmt.Errorf("failed to remove actor directories during drop seed (actor %s): %w", actorUID, err)
	}
	return &ateletpb.DropSeedResponse{}, nil
}
