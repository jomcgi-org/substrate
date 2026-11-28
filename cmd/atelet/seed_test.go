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
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/snapshottracker"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/utils/clock"
)

// seedWorker is a worker that records the seed and fork calls it gets.
type seedWorker struct {
	ateworkerpb.UnimplementedWorkerServer
	mu     sync.Mutex
	seeds  []*ateworkerpb.SeedWorkloadRequest
	forks  []*ateworkerpb.ForkWorkloadRequest
	drops  []*ateworkerpb.DropSeedRequest
	failed error
}

func (w *seedWorker) SeedWorkload(_ context.Context, req *ateworkerpb.SeedWorkloadRequest) (*ateworkerpb.SeedWorkloadResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seeds = append(w.seeds, proto.CloneOf(req))
	return &ateworkerpb.SeedWorkloadResponse{WorkerEpoch: 7}, w.failed
}

func (w *seedWorker) ForkWorkload(_ context.Context, req *ateworkerpb.ForkWorkloadRequest) (*ateworkerpb.ForkWorkloadResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.forks = append(w.forks, proto.CloneOf(req))
	return &ateworkerpb.ForkWorkloadResponse{WorkerEpoch: 7}, w.failed
}

func (w *seedWorker) DropSeed(_ context.Context, req *ateworkerpb.DropSeedRequest) (*ateworkerpb.DropSeedResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.drops = append(w.drops, proto.CloneOf(req))
	return &ateworkerpb.DropSeedResponse{}, nil
}

func serveSeedWorker(t *testing.T, w *seedWorker) {
	t.Helper()
	dir, err := os.MkdirTemp("", "worker-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "worker.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	ateworkerpb.RegisterWorkerServer(srv, w)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	orig := workerSocketPath
	workerSocketPath = func(string) string { return sock }
	t.Cleanup(func() { workerSocketPath = orig })
}

// tarErofs stands in for mkfs.erofs: the "image" is the merged tar stream, so
// the test sees what would go into the real one.
type tarErofs struct {
	mu     sync.Mutex
	builds int
}

func (b *tarErofs) Build(_ context.Context, img *imagecache.Image, out string) error {
	b.mu.Lock()
	b.builds++
	b.mu.Unlock()
	var buf bytes.Buffer
	if err := img.WriteTar(&buf); err != nil {
		return err
	}
	return os.WriteFile(out, buf.Bytes(), 0o644)
}

type seedFixture struct {
	s       *Atelet
	worker  *seedWorker
	builder *tarErofs
	rec     *sandboxAssetsRecord
	image   string
	cache   string
}

const (
	seedAtespace   = "ate-demo"
	seedUID        = "01234567-89ab-cdef-0123-456789abcdef"
	forkUID        = "11234567-89ab-cdef-0123-456789abcdef"
	seedTemplate   = "scan"
	seedTemplateID = "21234567-89ab-cdef-0123-456789abcdef"
	seedPodUID     = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
)

func newSeedFixture(t *testing.T) *seedFixture {
	t.Helper()
	useTempNodeDirs(t)
	preview.InitForTest(t, string(preview.GateForkSeed))
	worker := &seedWorker{}
	serveSeedWorker(t, worker)
	host := imageVolumeTestRegistry(t)
	pinned := pushTestImageWithConfig(t, host+"/scan:v1", v1.Config{Entrypoint: []string{"/bin/scan"}, Env: []string{"MODE=fast"}}, singleFileLayer(t, "bin/scan", "scanner"))
	release := newGVisorTestRelease(t)
	cacheRoot := t.TempDir()
	builder := &tarErofs{}
	s := &Atelet{
		workerDialer:      newWorkerDialer(1),
		imageCache:        newImageVolumeStore(t),
		sandboxAssetCache: newTestAssetCacheAt(t, cacheRoot),
		anonGCSClient:     fakeObjectStorage{data: release.tarball},
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
		snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
		checkpointTimeout: defaultCheckpointTimeout,
		erofs:             builder,
	}
	rec := testSandboxRecord(release)
	advertiseRuntime(t, s, rec)
	return &seedFixture{s: s, worker: worker, builder: builder, rec: rec, image: pinned, cache: cacheRoot}
}

func (f *seedFixture) spec() *ateletpb.WorkloadSpec {
	return &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{
		Name:        "main",
		Image:       f.image,
		WakeupProbe: &ateletpb.WakeupProbe{HttpGet: &ateletpb.HttpGetAction{Path: "/healthz", Port: 8080}, TimeoutSeconds: 5},
	}}}
}

func (f *seedFixture) seedRequest(t *testing.T) *ateletpb.SeedRequest {
	return &ateletpb.SeedRequest{
		WorkerPodUid:          seedPodUID,
		Atespace:              seedAtespace,
		ActorName:             "seed-1",
		ActorUid:              seedUID,
		ActorTemplateAtespace: seedAtespace,
		ActorTemplateName:     seedTemplate,
		TemplateUid:           seedTemplateID,
		Spec:                  f.spec(),
		Resources:             &ateletpb.ResourceRequirements{Limits: &ateletpb.Resources{CpuMillicores: 1000, MemoryBytes: 1 << 30}},
		SandboxRuntime:        runtimeFor(t, f.rec),
	}
}

func (f *seedFixture) forkRequest(t *testing.T) *ateletpb.ForkRequest {
	return &ateletpb.ForkRequest{
		WorkerPodUid:          seedPodUID,
		Atespace:              seedAtespace,
		ActorName:             "fork-1",
		ActorUid:              forkUID,
		ActorTemplateAtespace: seedAtespace,
		ActorTemplateName:     seedTemplate,
		TemplateUid:           seedTemplateID,
		SeedActorUid:          seedUID,
		SeedWorkerEpoch:       7,
		Spec:                  f.spec(),
		Resources:             &ateletpb.ResourceRequirements{Limits: &ateletpb.Resources{CpuMillicores: 500, MemoryBytes: 256 << 20}},
		Env:                   []*ateletpb.EnvVar{{Name: "ATE_ACTOR_NAME", Value: "fork-1"}},
		SandboxRuntime:        runtimeFor(t, f.rec),
		PidsLimit:             64,
		LogBytesLimit:         1 << 20,
		MaxLifetimeSeconds:    60,
	}
}

func TestSeed(t *testing.T) {
	f := newSeedFixture(t)
	resp, err := f.s.Seed(t.Context(), f.seedRequest(t))
	if err != nil {
		t.Fatalf("Seed() = %v", err)
	}
	if resp.GetWorkerEpoch() != 7 {
		t.Errorf("worker epoch = %d, want the worker's 7", resp.GetWorkerEpoch())
	}
	if len(f.worker.seeds) != 1 {
		t.Fatalf("worker saw %d SeedWorkload calls, want 1", len(f.worker.seeds))
	}
	req := f.worker.seeds[0]

	image, err := os.ReadFile(req.GetRootfsImagePath())
	if err != nil {
		t.Fatalf("reading the rootfs image the worker was given: %v", err)
	}
	if want := fmt.Sprintf("%x", sha256.Sum256(image)); req.GetRootfsSha256() != want || resp.GetRootfsSha256() != want {
		t.Errorf("rootfs hash = %s (response %s), want the image's %s", req.GetRootfsSha256(), resp.GetRootfsSha256(), want)
	}
	if !strings.HasPrefix(req.GetRootfsImagePath(), ateletpath.SeedRootfsDir(seedUID)+"/") {
		t.Errorf("rootfs image %q is not under the seed's directory", req.GetRootfsImagePath())
	}
	if !bytes.Contains(image, []byte("scanner")) {
		t.Error("the rootfs image does not hold the template image's files")
	}
	ctr := req.GetSpec().GetContainers()[0]
	if got := ctr.GetContainerSpec().GetArgs(); len(got) != 1 || got[0] != "/bin/scan" {
		t.Errorf("seed argv = %v, want the image entrypoint", got)
	}
	if env := strings.Join(ctr.GetContainerSpec().GetEnv(), " "); !strings.Contains(env, "MODE=fast") {
		t.Errorf("seed env = %q, want the image env", env)
	}
	if got := req.GetResources().GetLimits().GetMemoryBytes(); got != 1<<30 {
		t.Errorf("seed memory = %d, want %d", got, int64(1<<30))
	}
	if !strings.HasPrefix(req.GetRunscPath(), ateletpath.SandboxAssetsDir(seedUID)) {
		t.Errorf("runsc path %q is not under the seed's sandbox assets", req.GetRunscPath())
	}
	rec, err := readSeedRecord(seedUID)
	if err != nil {
		t.Fatalf("seed record: %v", err)
	}
	if rec.Atespace != seedAtespace || rec.TemplateUID != seedTemplateID || rec.WorkerPodUID != seedPodUID || rec.RootfsSHA256 != req.GetRootfsSha256() {
		t.Errorf("seed record = %+v", rec)
	}

	// A seed of the same image built again comes from the cache.
	if _, err := f.s.Seed(t.Context(), f.seedRequest(t)); err != nil {
		t.Fatalf("second Seed() = %v", err)
	}
	if f.builder.builds != 1 {
		t.Errorf("the image was built %d times, want once", f.builder.builds)
	}
}

func TestSeedRefusals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, f *seedFixture, req *ateletpb.SeedRequest)
		want   codes.Code
	}{{
		name:   "a template in another atespace",
		mutate: func(_ *testing.T, _ *seedFixture, req *ateletpb.SeedRequest) { req.ActorTemplateAtespace = "other" },
		want:   codes.PermissionDenied,
	}, {
		name: "two containers",
		mutate: func(_ *testing.T, _ *seedFixture, req *ateletpb.SeedRequest) {
			req.Spec.Containers = append(req.Spec.Containers, proto.CloneOf(req.Spec.Containers[0]))
			req.Spec.Containers[1].Name = "side"
		},
		want: codes.InvalidArgument,
	}, {
		name: "a volume",
		mutate: func(_ *testing.T, _ *seedFixture, req *ateletpb.SeedRequest) {
			req.Spec.Volumes = []*ateletpb.Volume{{Name: "scratch", DurableDir: &ateletpb.DurableDirVolumeSource{}}}
		},
		want: codes.InvalidArgument,
	}, {
		name:   "no mkfs.erofs",
		mutate: func(_ *testing.T, f *seedFixture, _ *ateletpb.SeedRequest) { f.s.erofs = nil },
		want:   codes.FailedPrecondition,
	}, {
		name:   "the gate is off",
		mutate: func(t *testing.T, _ *seedFixture, _ *ateletpb.SeedRequest) { preview.InitForTest(t) },
		want:   codes.FailedPrecondition,
	}, {
		name:   "a request that is not valid",
		mutate: func(_ *testing.T, _ *seedFixture, req *ateletpb.SeedRequest) { req.TemplateUid = "" },
		want:   codes.InvalidArgument,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSeedFixture(t)
			req := f.seedRequest(t)
			tt.mutate(t, f, req)
			_, err := f.s.Seed(t.Context(), req)
			if got := apierror.Code(err); got != tt.want {
				t.Fatalf("Seed() = %v, code %v, want %v", err, got, tt.want)
			}
			if len(f.worker.seeds) != 0 {
				t.Error("a refused seed reached the worker")
			}
		})
	}
}

// A cached image that changed on disk since it was built does not boot a seed.
func TestSeedRefusesAnImageThatChanged(t *testing.T) {
	f := newSeedFixture(t)
	if _, err := f.s.Seed(t.Context(), f.seedRequest(t)); err != nil {
		t.Fatal(err)
	}
	var cached string
	_ = filepath.WalkDir(filepath.Join(f.cache, "cache", "entries"), func(path string, d os.DirEntry, err error) error {
		if err == nil && d.Name() == rootfsImageName {
			cached = path
		}
		return nil
	})
	if cached == "" {
		t.Fatal("no cached rootfs image")
	}
	if err := os.Chmod(cached, 0o644); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(cached, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("tampered"); err != nil {
		t.Fatal(err)
	}
	file.Close()

	f.worker.seeds = nil
	_, err = f.s.Seed(t.Context(), f.seedRequest(t))
	if apierror.Code(err) != codes.DataLoss {
		t.Fatalf("Seed() with a changed image = %v, want DataLoss", err)
	}
	if len(f.worker.seeds) != 0 {
		t.Error("a changed image reached the worker")
	}
}

func TestFork(t *testing.T) {
	f := newSeedFixture(t)
	if _, err := f.s.Seed(t.Context(), f.seedRequest(t)); err != nil {
		t.Fatal(err)
	}

	resp, err := f.s.Fork(t.Context(), f.forkRequest(t))
	if err != nil {
		t.Fatalf("Fork() = %v", err)
	}
	if resp.GetWorkerEpoch() != 7 {
		t.Errorf("worker epoch = %d, want 7", resp.GetWorkerEpoch())
	}
	if len(f.worker.forks) != 1 {
		t.Fatalf("worker saw %d ForkWorkload calls, want 1", len(f.worker.forks))
	}
	req := f.worker.forks[0]
	if req.GetSeedActorUid() != seedUID || req.GetSeedWorkerEpoch() != 7 || req.GetTemplateUid() != seedTemplateID {
		t.Errorf("fork request names seed %s at epoch %d of template %s", req.GetSeedActorUid(), req.GetSeedWorkerEpoch(), req.GetTemplateUid())
	}
	if got := req.GetEnv(); len(got) != 1 || got[0] != "ATE_ACTOR_NAME=fork-1" {
		t.Errorf("fork env = %v", got)
	}
	if req.GetPidsLimit() != 64 || req.GetLogBytesLimit() != 1<<20 || req.GetMaxLifetimeSeconds() != 60 {
		t.Errorf("fork limits = %d, %d, %d", req.GetPidsLimit(), req.GetLogBytesLimit(), req.GetMaxLifetimeSeconds())
	}
	if req.GetActorDirs().GetRootDir() != ateletpath.ActorPath(forkUID) {
		t.Errorf("fork actor dir = %q, want the forked actor's", req.GetActorDirs().GetRootDir())
	}
	if got := req.GetSpec().GetContainers()[0].GetWakeupProbe().GetHttpGet().GetPort(); got != 8080 {
		t.Errorf("wakeup probe port = %d, want 8080", got)
	}
	if _, err := readSandboxRecord(forkUID); err != nil {
		t.Errorf("a forked actor has no sandbox record for Terminate: %v", err)
	}
	if req.EgressGateway != nil {
		t.Errorf("fork egress gateway = %v, want none when the request has none", req.GetEgressGateway())
	}
}

// Fork passes the request's egress gateway to the worker.
func TestForkPassesTheEgressGateway(t *testing.T) {
	f := newSeedFixture(t)
	if _, err := f.s.Seed(t.Context(), f.seedRequest(t)); err != nil {
		t.Fatal(err)
	}
	req := f.forkRequest(t)
	req.EgressGateway = &ateletpb.EgressGateway{Address: "egress.ate-system.svc:8443"}
	if _, err := f.s.Fork(t.Context(), req); err != nil {
		t.Fatalf("Fork() = %v", err)
	}
	if len(f.worker.forks) != 1 {
		t.Fatalf("worker saw %d ForkWorkload calls, want 1", len(f.worker.forks))
	}
	if got := f.worker.forks[0].GetEgressGateway().GetAddress(); got != "egress.ate-system.svc:8443" {
		t.Errorf("worker egress gateway = %q, want the request's", got)
	}
}

// Fork refuses a seed that was not built for the request, whoever asks.
func TestForkChecksTheSeed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(req *ateletpb.ForkRequest)
		want   codes.Code
	}{
		{"another atespace", func(r *ateletpb.ForkRequest) { r.Atespace = "other"; r.ActorTemplateAtespace = "other" }, codes.PermissionDenied},
		{"a template in another atespace", func(r *ateletpb.ForkRequest) { r.ActorTemplateAtespace = "other" }, codes.PermissionDenied},
		{"another template", func(r *ateletpb.ForkRequest) { r.TemplateUid = "31234567-89ab-cdef-0123-456789abcdef" }, codes.PermissionDenied},
		{"another worker pod", func(r *ateletpb.ForkRequest) { r.WorkerPodUid = "6f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f" }, codes.PermissionDenied},
		{"an unknown seed", func(r *ateletpb.ForkRequest) { r.SeedActorUid = "41234567-89ab-cdef-0123-456789abcdef" }, codes.NotFound},
		{"two containers", func(r *ateletpb.ForkRequest) {
			r.Spec.Containers = append(r.Spec.Containers, proto.CloneOf(r.Spec.Containers[0]))
			r.Spec.Containers[1].Name = "side"
		}, codes.InvalidArgument},
		{"a request that is not valid", func(r *ateletpb.ForkRequest) { r.PidsLimit = 0 }, codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSeedFixture(t)
			if _, err := f.s.Seed(t.Context(), f.seedRequest(t)); err != nil {
				t.Fatal(err)
			}
			req := f.forkRequest(t)
			tt.mutate(req)
			_, err := f.s.Fork(t.Context(), req)
			if got := apierror.Code(err); got != tt.want && status.Code(err) != tt.want {
				t.Fatalf("Fork() = %v, want %v", err, tt.want)
			}
			if len(f.worker.forks) != 0 {
				t.Error("a refused fork reached the worker")
			}
		})
	}
}

func TestForkWorkerFailure(t *testing.T) {
	f := newSeedFixture(t)
	if _, err := f.s.Seed(t.Context(), f.seedRequest(t)); err != nil {
		t.Fatal(err)
	}
	f.worker.failed = status.Error(codes.FailedPrecondition, "forkd is down")
	if _, err := f.s.Fork(t.Context(), f.forkRequest(t)); err == nil {
		t.Fatal("Fork() succeeded though the worker failed")
	}
	// Terminate still needs to find the sandbox.
	if _, err := readSandboxRecord(forkUID); err != nil {
		t.Errorf("a fork that failed at the worker left no sandbox record: %v", err)
	}
}

func TestDropSeed(t *testing.T) {
	f := newSeedFixture(t)
	if _, err := f.s.Seed(t.Context(), f.seedRequest(t)); err != nil {
		t.Fatal(err)
	}
	req := &ateletpb.DropSeedRequest{WorkerPodUid: seedPodUID, Atespace: seedAtespace, ActorName: "seed-1", ActorUid: seedUID}
	for range 2 { // idempotent
		if _, err := f.s.DropSeed(t.Context(), req); err != nil {
			t.Fatalf("DropSeed() = %v", err)
		}
	}
	if len(f.worker.drops) == 0 || f.worker.drops[0].GetActorUid() != seedUID {
		t.Errorf("the worker was not told to drop the seed: %v", f.worker.drops)
	}
	if _, err := os.Stat(ateletpath.ActorPath(seedUID)); !os.IsNotExist(err) {
		t.Errorf("the seed's directory is still there: %v", err)
	}
	if _, err := readSeedRecord(seedUID); status.Code(err) != codes.NotFound {
		t.Errorf("a dropped seed can still be forked: %v", err)
	}
	// Without a worker it still clears the node.
	noWorker := &ateletpb.DropSeedRequest{Atespace: seedAtespace, ActorName: "seed-1", ActorUid: seedUID}
	if _, err := f.s.DropSeed(t.Context(), noWorker); err != nil {
		t.Errorf("DropSeed() without a worker = %v", err)
	}
}

func TestMkfsErofsPath(t *testing.T) {
	data := t.TempDir()
	t.Setenv("KO_DATA_PATH", data)
	if got := mkfsErofsPath(""); got != "" {
		t.Errorf("mkfsErofsPath() with no binary in kodata = %q, want none", got)
	}
	bundled := filepath.Join(data, "mkfs.erofs-"+runtime.GOARCH)
	if err := os.WriteFile(bundled, []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := mkfsErofsPath(""); got != bundled {
		t.Errorf("mkfsErofsPath() = %q, want the kodata binary %q", got, bundled)
	}
	if got := mkfsErofsPath("/opt/mkfs.erofs"); got != "/opt/mkfs.erofs" {
		t.Errorf("mkfsErofsPath(flag) = %q, want the flag", got)
	}
	t.Setenv("KO_DATA_PATH", "")
	if got := mkfsErofsPath(""); got != "" {
		t.Errorf("mkfsErofsPath() outside a ko image = %q, want none", got)
	}
}

// The real mkfs.erofs makes the same image from the same layers. It runs only
// where ATE_TEST_MKFS_EROFS names the binary, since few hosts have one.
func TestMkfsErofsIsReproducible(t *testing.T) {
	binary := os.Getenv("ATE_TEST_MKFS_EROFS")
	if binary == "" {
		t.Skip("ATE_TEST_MKFS_EROFS names no mkfs.erofs")
	}
	if _, err := exec.LookPath(binary); err != nil {
		t.Fatalf("ATE_TEST_MKFS_EROFS=%q: %v", binary, err)
	}
	host := imageVolumeTestRegistry(t)
	pushTestImage(t, host+"/scan:v1", singleFileLayer(t, "bin/scan", "scanner"))
	img, err := newImageVolumeStore(t).EnsureImage(t.Context(), host+"/scan:v1")
	if err != nil {
		t.Fatal(err)
	}
	var sums []string
	for i := range 2 {
		out := filepath.Join(t.TempDir(), fmt.Sprintf("img%d.erofs", i))
		if err := (mkfsErofs{binary: binary}).Build(t.Context(), img, out); err != nil {
			t.Fatalf("Build() = %v", err)
		}
		sum, err := fileSHA256(out)
		if err != nil {
			t.Fatal(err)
		}
		sums = append(sums, sum)
	}
	if sums[0] != sums[1] {
		t.Errorf("two builds of one image differ: %v", sums)
	}
	if err := (mkfsErofs{binary: "/nonexistent/mkfs.erofs"}).Build(t.Context(), img, filepath.Join(t.TempDir(), "x")); err == nil {
		t.Error("Build() succeeded with no mkfs.erofs")
	}
}
