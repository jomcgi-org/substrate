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
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/forkd"
	"go.ate.dev/substrate/internal/forkd/forkdtest"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
)

const (
	forkTestAtespace = "team-a"
	forkTestTemplate = "21234567-89ab-cdef-0123-456789abcdef"
	forkTestSeedUID  = "01234567-89ab-cdef-0123-456789abcdef"
	forkTestChildUID = "11234567-89ab-cdef-0123-456789abcdef"
	forkTestEpoch    = 7
)

// fakeLauncher hands out a client of a forkdtest.Fake.
type fakeLauncher struct {
	socket   string
	launches int
	bins     []forkdBinaries // what each launch was given
	err      error
	exited   chan struct{}
}

func (l *fakeLauncher) Launch(_ context.Context, bins forkdBinaries) (*forkd.Client, <-chan struct{}, error) {
	l.launches++
	l.bins = append(l.bins, bins)
	if l.err != nil {
		return nil, nil, l.err
	}
	return forkd.NewClient(l.socket), l.exited, nil
}

// fakeHost records the host work around a forked actor.
type fakeHost struct {
	mu       sync.Mutex
	hosted   []string
	unhosted []string
	active   []string
	gateways []*ateworkerpb.EgressGateway
	hostErr  error
	probeErr error
	onProbe  func()
	// holdProbe makes Probe wait for its context to end.
	holdProbe bool
}

func (h *fakeHost) Host(_ context.Context, actor resources.ActorAttribution, _ *ateworkerpb.ActorDirs, _ int64, gateway *ateworkerpb.EgressGateway) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.hostErr != nil {
		return "", h.hostErr
	}
	h.hosted = append(h.hosted, actor.UID)
	h.gateways = append(h.gateways, gateway)
	return "/proc/1/fd/9", nil
}

func (h *fakeHost) Probe(ctx context.Context, _ string, _ []*ateworkerpb.Container) error {
	h.mu.Lock()
	hook, err, hold := h.onProbe, h.probeErr, h.holdProbe
	h.mu.Unlock()
	if hook != nil {
		hook()
	}
	if hold {
		<-ctx.Done()
		return ctx.Err()
	}
	return err
}

func (h *fakeHost) Activate(actor resources.ActorAttribution) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.active = append(h.active, actor.UID)
	return nil
}

func (h *fakeHost) Unhost(_ context.Context, actor resources.ActorAttribution) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.unhosted = append(h.unhosted, actor.UID)
	return nil
}

func (h *fakeHost) unhostedUIDs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.unhosted...)
}

// recordingReporter records how each seed and forked actor was reported.
type recordingReporter struct {
	mu      sync.Mutex
	exited  map[string]int32
	crashed map[string]string
	notify  chan string
}

func newRecordingReporter() *recordingReporter {
	return &recordingReporter{exited: map[string]int32{}, crashed: map[string]string{}, notify: make(chan string, 16)}
}

func (r *recordingReporter) ReportExited(_ context.Context, a resources.ActorAttribution, code int32) error {
	r.mu.Lock()
	r.exited[a.UID] = code
	r.mu.Unlock()
	r.notify <- a.UID
	return nil
}

func (r *recordingReporter) ReportCrashed(_ context.Context, a resources.ActorAttribution, msg string) error {
	r.mu.Lock()
	r.crashed[a.UID] = msg
	r.mu.Unlock()
	r.notify <- a.UID
	return nil
}

// next waits for the next report and returns the UID it was for.
func (r *recordingReporter) next(t *testing.T) string {
	t.Helper()
	select {
	case uid := <-r.notify:
		return uid
	case <-time.After(10 * time.Second):
		t.Fatal("no report arrived")
		return ""
	}
}

type forkFixture struct {
	t        *testing.T
	fake     *forkdtest.Fake
	launcher *fakeLauncher
	host     *fakeHost
	reporter *recordingReporter
	m        *forkManager
	dir      string
	rootfs   string
	sha      string
	seedPIDs map[int]chan struct{}
	seedMu   sync.Mutex
}

func newForkFixture(t *testing.T) *forkFixture {
	t.Helper()
	fake, socket := forkdtest.Serve(t)
	f := &forkFixture{
		t: t, fake: fake, host: &fakeHost{}, reporter: newRecordingReporter(),
		launcher: &fakeLauncher{socket: socket, exited: make(chan struct{})},
		dir:      t.TempDir(), seedPIDs: map[int]chan struct{}{},
	}
	f.m = newForkManager(f.launcher, f.host)
	f.m.reporter = f.reporter
	f.m.waitRetry = 5 * time.Millisecond
	f.m.lockActor = func(context.Context, string) (func(), bool) { return func() {}, true }
	f.m.watchPID = func(pid int) (<-chan struct{}, func(), error) {
		f.seedMu.Lock()
		defer f.seedMu.Unlock()
		ch := make(chan struct{})
		f.seedPIDs[pid] = ch
		return ch, func() {}, nil
	}
	f.rootfs = filepath.Join(f.dir, "seed-rootfs", "rootfs.erofs")
	if err := os.MkdirAll(filepath.Dir(f.rootfs), 0o755); err != nil {
		t.Fatal(err)
	}
	content := []byte("erofs image")
	if err := os.WriteFile(f.rootfs, content, 0o444); err != nil {
		t.Fatal(err)
	}
	f.sha = fmt.Sprintf("%x", sha256.Sum256(content))
	return f
}

func (f *forkFixture) seedRequest() *ateworkerpb.SeedWorkloadRequest {
	return &ateworkerpb.SeedWorkloadRequest{
		Atespace: forkTestAtespace, ActorName: "seed-1", ActorUid: forkTestSeedUID,
		ActorTemplateAtespace: forkTestAtespace, ActorTemplateName: "scan", TemplateUid: forkTestTemplate,
		RunscPath:       "/sandbox-assets/gvisor/runsc",
		RootfsImagePath: f.rootfs, RootfsSha256: f.sha,
		Resources: &ateworkerpb.ResourceRequirements{Limits: &ateworkerpb.Resources{CpuMillicores: 1500, MemoryBytes: 1 << 30}},
		ActorDirs: &ateworkerpb.ActorDirs{RootDir: f.dir},
		Spec: &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{{
			Name: "main",
			ContainerSpec: &ateworkerpb.ContainerSpec{
				Args: []string{"/bin/scan", "--serve"}, Env: []string{"MODE=fast"}, Cwd: "/srv",
				User: &ateworkerpb.ProcessUser{Uid: 1000, Gid: 1000},
			},
		}}},
	}
}

func (f *forkFixture) forkRequest() *ateworkerpb.ForkWorkloadRequest {
	return &ateworkerpb.ForkWorkloadRequest{
		Atespace: forkTestAtespace, ActorName: "fork-1", ActorUid: forkTestChildUID,
		ActorTemplateAtespace: forkTestAtespace, ActorTemplateName: "scan", TemplateUid: forkTestTemplate,
		SeedActorUid: forkTestSeedUID, SeedWorkerEpoch: forkTestEpoch,
		Resources: &ateworkerpb.ResourceRequirements{Limits: &ateworkerpb.Resources{CpuMillicores: 500, MemoryBytes: 256 << 20}},
		Env:       []string{"ATE_ACTOR_NAME=fork-1"},
		PidsLimit: 64, LogBytesLimit: 1 << 20, MaxLifetimeSeconds: 60,
		ActorDirs: &ateworkerpb.ActorDirs{RootDir: f.dir},
		Spec: &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{{
			Name: "main", WakeupProbe: &ateworkerpb.WakeupProbe{HttpGet: &ateworkerpb.HttpGetAction{Path: "/healthz", Port: 8080}, TimeoutSeconds: 5},
		}}},
	}
}

func (f *forkFixture) seed() {
	f.t.Helper()
	if err := f.m.seed(f.t.Context(), f.seedRequest(), forkTestEpoch); err != nil {
		f.t.Fatalf("seed() = %v", err)
	}
}

func (f *forkFixture) fork() {
	f.t.Helper()
	if err := f.m.fork(f.t.Context(), f.forkRequest(), forkTestEpoch); err != nil {
		f.t.Fatalf("fork() = %v", err)
	}
}

func TestForkManagerSeedAndFork(t *testing.T) {
	f := newForkFixture(t)
	f.seed()
	seeds := f.fake.SeedRequests()
	if len(seeds) != 1 {
		t.Fatalf("forkd saw %d seed requests, want 1", len(seeds))
	}
	s := seeds[0]
	if s.SeedID != forkTestSeedUID || s.Image != f.rootfs || s.ImageSHA256 != f.sha {
		t.Errorf("seed names %q, image %q (%s)", s.SeedID, s.Image, s.ImageSHA256)
	}
	if strings.Join(s.Args, " ") != "/bin/scan --serve" || strings.Join(s.Env, " ") != "MODE=fast" || s.Cwd != "/srv" || s.UID != 1000 || s.GID != 1000 {
		t.Errorf("seed process = %v %v %q %d:%d", s.Args, s.Env, s.Cwd, s.UID, s.GID)
	}
	if !s.DropCapabilities {
		t.Error("a seed keeps capabilities")
	}
	if s.Cgroup == nil || s.Cgroup.MemoryMax != 1<<30 || s.Cgroup.MemoryHigh != 0 {
		t.Errorf("seed cgroup = %+v, want memory.max of the seed resources (1Gi)", s.Cgroup)
	}
	if len(s.Tmpfs) != 1 || s.Tmpfs[0].Path != "/tmp" || s.Tmpfs[0].SizeBytes != 512<<20 {
		t.Errorf("seed tmpfs = %+v, want /tmp at half the memory", s.Tmpfs)
	}
	if s.CPUs != 2 || s.Workers != 1 {
		t.Errorf("seed has %d cpus and %d workers, want 2 and 1", s.CPUs, s.Workers)
	}
	if f.launcher.launches != 1 {
		t.Errorf("forkd was started %d times, want once", f.launcher.launches)
	}
	// A retried SeedWorkload that already landed does nothing more.
	f.seed()
	if got := len(f.fake.SeedRequests()); got != 1 {
		t.Errorf("a repeated seed reached forkd: %d requests", got)
	}

	f.fork()
	forks := f.fake.ForkRequests()
	if len(forks) != 1 {
		t.Fatalf("forkd saw %d fork requests, want 1", len(forks))
	}
	r := forks[0]
	if r.SeedID != forkTestSeedUID || r.ChildID != forkTestChildUID {
		t.Errorf("fork names seed %q and child %q", r.SeedID, r.ChildID)
	}
	if len(r.Env) != 1 || r.Env[0] != "ATE_ACTOR_NAME=fork-1" {
		t.Errorf("fork env = %v", r.Env)
	}
	if r.Cgroup == nil || r.Cgroup.CPUMax != "50000 100000" || r.Cgroup.MemoryMax != 256<<20+forkOverhead.GetMemoryBytes() || r.Cgroup.MemoryHigh != 0 || r.Cgroup.PidsMax != 64 {
		t.Errorf("fork cgroup = %+v, want 0.5 cpu, memory.max of 256Mi plus the fork overhead, no memory.high and 64 pids", r.Cgroup)
	}
	if r.Net == nil || r.Net.NetNSPath != "/proc/1/fd/9" {
		t.Errorf("fork net = %+v, want the actor's namespace", r.Net)
	}
	if r.MaxLifetimeMS != 60000 || r.LogBytesLimit != 1<<20 {
		t.Errorf("fork limits = %dms, %d bytes", r.MaxLifetimeMS, r.LogBytesLimit)
	}
	if len(f.host.hosted) != 1 || len(f.host.active) != 1 {
		t.Errorf("host calls: hosted %v, active %v", f.host.hosted, f.host.active)
	}
	if !f.m.owns(forkTestChildUID) || !f.m.owns(forkTestSeedUID) {
		t.Error("the manager does not own the seed and child it started")
	}
}

// The manager hosts a forked actor with the request's egress gateway, and with
// none when the request has none.
func TestForkManagerHostsWithTheEgressGateway(t *testing.T) {
	for _, tc := range []struct {
		name    string
		gateway *ateworkerpb.EgressGateway
	}{
		{name: "none"},
		{name: "gateway", gateway: &ateworkerpb.EgressGateway{Address: "egress.ate-system.svc:8443"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newForkFixture(t)
			f.seed()
			req := f.forkRequest()
			req.EgressGateway = tc.gateway
			if err := f.m.fork(t.Context(), req, forkTestEpoch); err != nil {
				t.Fatalf("fork() = %v", err)
			}
			if len(f.host.gateways) != 1 {
				t.Fatalf("host was called %d times, want once", len(f.host.gateways))
			}
			if got := f.host.gateways[0]; got.GetAddress() != tc.gateway.GetAddress() || (got == nil) != (tc.gateway == nil) {
				t.Errorf("host got gateway %v, want %v", got, tc.gateway)
			}
		})
	}
}

func TestForkManagerReportsExits(t *testing.T) {
	signal := func(n int) forkd.ExitInfo { return forkd.ExitInfo{Reason: forkd.ReasonExited, Signal: n} }
	tests := []struct {
		name        string
		exit        forkd.ExitInfo
		wantExited  bool
		wantCode    int32
		wantCrashed string
	}{
		{name: "exit code", exit: forkdtest.ExitCode(3), wantExited: true, wantCode: 3},
		{name: "zero exit", exit: forkdtest.ExitCode(0), wantExited: true},
		{name: "killed by a signal", exit: signal(9), wantExited: true, wantCode: 137},
		{name: "deleted", exit: forkd.ExitInfo{Reason: forkd.ReasonDeleted}, wantExited: true},
		{name: "oom", exit: forkd.ExitInfo{Reason: forkd.ReasonOOM, Detail: "oom_kill 1"}, wantCrashed: "forked sandbox ended: oom: oom_kill 1"},
		{name: "deadline", exit: forkd.ExitInfo{Reason: forkd.ReasonMaxLifetime}, wantCrashed: "forked sandbox ended: max_lifetime"},
		{name: "log limit", exit: forkd.ExitInfo{Reason: forkd.ReasonLogLimit}, wantCrashed: "forked sandbox ended: log_limit"},
		{name: "sandbox died", exit: forkd.ExitInfo{Reason: forkd.ReasonKilled, Signal: 11}, wantCrashed: "forked sandbox ended: killed"},
		{name: "seed gone", exit: forkd.ExitInfo{Reason: forkd.ReasonSeedGone}, wantCrashed: "forked sandbox ended: seed_gone"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newForkFixture(t)
			f.seed()
			f.fork()
			f.fake.Exit(forkTestChildUID, tt.exit)
			if got := f.reporter.next(t); got != forkTestChildUID {
				t.Fatalf("reported %q, want the forked actor", got)
			}
			f.reporter.mu.Lock()
			defer f.reporter.mu.Unlock()
			if code, ok := f.reporter.exited[forkTestChildUID]; ok != tt.wantExited || code != tt.wantCode {
				t.Errorf("exit report = %d, %v, want %d, %v", code, ok, tt.wantCode, tt.wantExited)
			}
			if msg, ok := f.reporter.crashed[forkTestChildUID]; ok != (tt.wantCrashed != "") || msg != tt.wantCrashed {
				t.Errorf("crash report = %q, %v, want %q", msg, ok, tt.wantCrashed)
			}
		})
	}
}

// A report follows the teardown: the actor is deleted from forkd and unhosted
// by the time the control plane hears of it.
func TestForkManagerTearsDownBeforeReporting(t *testing.T) {
	f := newForkFixture(t)
	f.seed()
	f.fork()
	f.fake.Exit(forkTestChildUID, forkdtest.ExitCode(0))
	f.reporter.next(t)
	if got := f.host.unhostedUIDs(); len(got) != 1 || got[0] != forkTestChildUID {
		t.Errorf("unhosted %v, want the forked actor", got)
	}
	if f.fake.HasChild(forkTestChildUID) {
		t.Error("forkd still holds the child after its exit was reported")
	}
	if f.m.owns(forkTestChildUID) {
		t.Error("the manager still owns the child")
	}
}

// A child can end while the fork RPC still probes it. The exit is reported
// once, after the RPC returns, and the actor is not left hosted.
func TestForkManagerChildExitsBeforeTheForkReturns(t *testing.T) {
	f := newForkFixture(t)
	f.seed()
	f.host.onProbe = func() { f.fake.Exit(forkTestChildUID, forkdtest.ExitCode(1)) }
	f.fork()
	if got := f.reporter.next(t); got != forkTestChildUID {
		t.Fatalf("reported %q", got)
	}
	f.reporter.mu.Lock()
	code, ok := f.reporter.exited[forkTestChildUID]
	f.reporter.mu.Unlock()
	if !ok || code != 1 {
		t.Errorf("exit report = %d, %v, want code 1", code, ok)
	}
	select {
	case again := <-f.reporter.notify:
		t.Errorf("the exit was reported twice (%q)", again)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestForkManagerForkRefusals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(f *forkFixture, req *ateworkerpb.ForkWorkloadRequest)
		want   codes.Code
	}{
		{"an unknown seed", func(_ *forkFixture, r *ateworkerpb.ForkWorkloadRequest) {
			r.SeedActorUid = "31234567-89ab-cdef-0123-456789abcdef"
		}, codes.NotFound},
		{"a seed of an earlier worker process", func(_ *forkFixture, r *ateworkerpb.ForkWorkloadRequest) { r.SeedWorkerEpoch = forkTestEpoch - 1 }, codes.FailedPrecondition},
		{"another atespace", func(_ *forkFixture, r *ateworkerpb.ForkWorkloadRequest) { r.Atespace = "team-b" }, codes.PermissionDenied},
		{"another template", func(_ *forkFixture, r *ateworkerpb.ForkWorkloadRequest) {
			r.TemplateUid = "41234567-89ab-cdef-0123-456789abcdef"
		}, codes.PermissionDenied},
		{"no room in forkd", func(f *forkFixture, _ *ateworkerpb.ForkWorkloadRequest) {
			f.fake.ForkErr = &forkd.Error{Code: forkd.CodeResourceExhausted, Message: "256 children"}
		}, codes.ResourceExhausted},
		{"the seed is not ready in forkd", func(f *forkFixture, _ *ateworkerpb.ForkWorkloadRequest) {
			f.fake.ForkErr = &forkd.Error{Code: forkd.CodeFailedPrecondition, Message: "seed is dead"}
		}, codes.FailedPrecondition},
		{"forkd is down", func(f *forkFixture, _ *ateworkerpb.ForkWorkloadRequest) { f.fake.Crash() }, codes.Unavailable},
		{"the network cannot be built", func(f *forkFixture, _ *ateworkerpb.ForkWorkloadRequest) {
			f.host.hostErr = apierror.ResourceExhausted("worker is full")
		}, codes.ResourceExhausted},
		{"the wakeup probe fails", func(f *forkFixture, _ *ateworkerpb.ForkWorkloadRequest) {
			f.host.probeErr = errors.New("probe timed out")
		}, codes.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newForkFixture(t)
			f.seed()
			req := f.forkRequest()
			tt.mutate(f, req)
			err := f.m.fork(t.Context(), req, forkTestEpoch)
			if err == nil {
				t.Fatal("fork() succeeded")
			}
			if got := apierror.Code(err); got != tt.want {
				t.Errorf("fork() = %v, code %v, want %v", err, got, tt.want)
			}
			if f.m.owns(forkTestChildUID) {
				t.Error("a failed fork left the actor registered")
			}
			// Whatever the fork built is unwound, once, and nothing is reported:
			// the RPC's own error is how the control plane learns.
			hosted, unhosted := len(f.host.hosted), len(f.host.unhostedUIDs())
			if hosted != unhosted {
				t.Errorf("the host built %d actor networks and removed %d", hosted, unhosted)
			}
			select {
			case uid := <-f.reporter.notify:
				t.Errorf("a failed fork was reported for %q", uid)
			case <-time.After(30 * time.Millisecond):
			}
		})
	}
}

// forkd refuses a cgroup parent that is not an absolute path below
// /sys/fs/cgroup, so the launcher passes the path as it is.
func TestForkdLauncherArgs(t *testing.T) {
	l := execForkdLauncher{stateDir: "/run/ate/forkd", cgroupParent: "/sys/fs/cgroup/forkd", uid: 1234}
	want := []string{
		"--forkd-state-dir=/run/ate/forkd",
		"--forkd-cgroup-parent=/sys/fs/cgroup/forkd",
		"--forkd-env-prefixes=ATE_ACTOR_",
		"--forkd-allow-uids=1234",
	}
	if got := l.args(); !slices.Equal(got, want) {
		t.Errorf("args = %v, want %v", got, want)
	}
}

// A child that ends while the wakeup probe polls fails the start at once.
func TestForkManagerChildEndsDuringTheProbe(t *testing.T) {
	f := newForkFixture(t)
	f.seed()
	f.host.mu.Lock()
	f.host.holdProbe = true
	f.host.onProbe = func() { f.fake.Exit(forkTestChildUID, forkdtest.ExitCode(3)) }
	f.host.mu.Unlock()
	start := time.Now()
	err := f.m.fork(t.Context(), f.forkRequest(), forkTestEpoch)
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("fork() = %v, want FailedPrecondition", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("fork() took %v to notice that the child ended", elapsed)
	}
	// The child's watcher or the failed fork removes it, whichever comes first.
	deadline := time.Now().Add(5 * time.Second)
	for len(f.host.unhostedUIDs()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := f.host.unhostedUIDs(); len(got) != 1 {
		t.Errorf("unhosted %v, want the child once", got)
	}
}

func TestForkManagerSeedRefusals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(f *forkFixture, req *ateworkerpb.SeedWorkloadRequest)
		want   codes.Code
	}{
		{"an image that changed", func(f *forkFixture, r *ateworkerpb.SeedWorkloadRequest) {
			if err := os.Chmod(f.rootfs, 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(f.rootfs, []byte("tampered"), 0o644); err != nil {
				t.Fatal(err)
			}
		}, codes.DataLoss},
		{"an image outside the seed's directory", func(f *forkFixture, r *ateworkerpb.SeedWorkloadRequest) {
			r.RootfsImagePath = "/etc/passwd"
		}, codes.InvalidArgument},
		{"an image that is gone", func(f *forkFixture, _ *ateworkerpb.SeedWorkloadRequest) { _ = os.Remove(f.rootfs) }, codes.FailedPrecondition},
		{"no memory limit to cap the seed with", func(_ *forkFixture, r *ateworkerpb.SeedWorkloadRequest) {
			r.Resources.Limits.MemoryBytes = 0
		}, codes.InvalidArgument},
		{"no process", func(_ *forkFixture, r *ateworkerpb.SeedWorkloadRequest) {
			r.Spec.Containers[0].ContainerSpec.Args = nil
		}, codes.InvalidArgument},
		{"forkd refuses the image", func(f *forkFixture, _ *ateworkerpb.SeedWorkloadRequest) {
			f.fake.SeedErr = &forkd.Error{Code: forkd.CodePermissionDenied, Message: "digest mismatch"}
		}, codes.PermissionDenied},
		{"forkd cannot start", func(f *forkFixture, _ *ateworkerpb.SeedWorkloadRequest) { f.launcher.err = errors.New("no such file") }, codes.FailedPrecondition},
		{"a forkd on another platform", func(f *forkFixture, _ *ateworkerpb.SeedWorkloadRequest) { f.fake.Hello.Platform = "systrap" }, codes.FailedPrecondition},
		{"a forkd of another protocol", func(f *forkFixture, _ *ateworkerpb.SeedWorkloadRequest) {
			f.fake.Hello.ProtocolVersion = forkd.ProtocolVersion + 1
		}, codes.FailedPrecondition},
		{"a forkd that failed an assertion", func(f *forkFixture, _ *ateworkerpb.SeedWorkloadRequest) {
			f.fake.Hello.Assertions = append(f.fake.Hello.Assertions, forkd.Assertion{Name: "sentry_seccomp", OK: false, Detail: "disabled"})
		}, codes.FailedPrecondition},
		{"a forkd that reports no assertions", func(f *forkFixture, _ *ateworkerpb.SeedWorkloadRequest) { f.fake.Hello.Assertions = nil }, codes.FailedPrecondition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newForkFixture(t)
			req := f.seedRequest()
			tt.mutate(f, req)
			err := f.m.seed(t.Context(), req, forkTestEpoch)
			if got := apierror.Code(err); err == nil || got != tt.want {
				t.Fatalf("seed() = %v, code %v, want %v", err, got, tt.want)
			}
			if f.m.owns(forkTestSeedUID) {
				t.Error("a refused seed is registered")
			}
		})
	}
}

// Two atespaces never share a worker process: the first seed fixes it.
func TestForkManagerSeedsServeOneAtespace(t *testing.T) {
	f := newForkFixture(t)
	f.seed()
	other := f.seedRequest()
	other.Atespace, other.ActorTemplateAtespace = "team-b", "team-b"
	other.ActorUid = "51234567-89ab-cdef-0123-456789abcdef"
	if err := f.m.seed(t.Context(), other, forkTestEpoch); apierror.Code(err) != codes.PermissionDenied {
		t.Fatalf("seed() of another atespace = %v, want PermissionDenied", err)
	}
	if got := len(f.fake.SeedRequests()); got != 1 {
		t.Errorf("forkd saw %d seed requests, want only the first", got)
	}
	// The same atespace may hold more templates.
	same := f.seedRequest()
	same.ActorUid = "61234567-89ab-cdef-0123-456789abcdef"
	same.TemplateUid = "71234567-89ab-cdef-0123-456789abcdef"
	if err := f.m.seed(t.Context(), same, forkTestEpoch); err != nil {
		t.Errorf("a second template in the same atespace = %v", err)
	}
}

func TestForkManagerSeedDiesOnItsOwn(t *testing.T) {
	f := newForkFixture(t)
	f.seed()
	f.seedMu.Lock()
	var pid int
	for p := range f.seedPIDs {
		pid = p
	}
	close(f.seedPIDs[pid])
	f.seedMu.Unlock()

	if got := f.reporter.next(t); got != forkTestSeedUID {
		t.Fatalf("reported %q, want the seed", got)
	}
	f.reporter.mu.Lock()
	msg := f.reporter.crashed[forkTestSeedUID]
	f.reporter.mu.Unlock()
	if msg != "seed sandbox exited" {
		t.Errorf("crash message = %q", msg)
	}
	if err := f.m.fork(t.Context(), f.forkRequest(), forkTestEpoch); apierror.Code(err) != codes.NotFound {
		t.Errorf("fork() from a dead seed = %v, want NotFound", err)
	}
}

func TestForkManagerForkdExits(t *testing.T) {
	f := newForkFixture(t)
	f.seed()
	f.fork()
	f.fake.Crash()
	close(f.launcher.exited)

	got := map[string]bool{}
	for range 2 {
		got[f.reporter.next(t)] = true
	}
	if !got[forkTestSeedUID] || !got[forkTestChildUID] {
		t.Errorf("reports for %v, want the seed and the forked actor", got)
	}
	f.reporter.mu.Lock()
	defer f.reporter.mu.Unlock()
	if f.reporter.crashed[forkTestSeedUID] != "forkd exited" {
		t.Errorf("seed crash message = %q", f.reporter.crashed[forkTestSeedUID])
	}
	if len(f.reporter.exited) != 0 {
		t.Errorf("a forkd that died produced clean exits: %v", f.reporter.exited)
	}
}

func TestForkManagerTerminate(t *testing.T) {
	f := newForkFixture(t)
	f.seed()
	f.fork()

	if err := f.m.terminate(t.Context(), forkTestChildUID); err != nil {
		t.Fatalf("terminate(child) = %v", err)
	}
	if f.m.owns(forkTestChildUID) || f.fake.HasChild(forkTestChildUID) {
		t.Error("the child survived its Terminate")
	}
	select {
	case uid := <-f.reporter.notify:
		t.Errorf("a Terminate was reported back to the control plane (%q)", uid)
	case <-time.After(100 * time.Millisecond):
	}
	if got := f.host.unhostedUIDs(); len(got) != 1 {
		t.Errorf("unhosted %v, want the child once", got)
	}
	if err := f.m.terminate(t.Context(), forkTestChildUID); err != nil {
		t.Errorf("a second terminate(child) = %v", err)
	}

	if err := f.m.terminate(t.Context(), forkTestSeedUID); err != nil {
		t.Fatalf("terminate(seed) = %v", err)
	}
	if f.m.owns(forkTestSeedUID) || f.fake.HasSeed(forkTestSeedUID) {
		t.Error("the seed survived its Terminate")
	}
	if err := f.m.dropSeed(t.Context(), forkTestSeedUID); err != nil {
		t.Errorf("dropSeed() of a dropped seed = %v", err)
	}
}

func TestCpuMax(t *testing.T) {
	for _, tc := range []struct {
		millicores int64
		want       string
	}{{0, "max"}, {-5, "max"}, {500, "50000 100000"}, {1000, "100000 100000"}, {2500, "250000 100000"}, {1, "1000 100000"}} {
		if got := cpuMax(tc.millicores); got != tc.want {
			t.Errorf("cpuMax(%d) = %q, want %q", tc.millicores, got, tc.want)
		}
	}
}

func TestExitCode(t *testing.T) {
	code := func(n int) *int { return &n }
	for _, tc := range []struct {
		name string
		exit forkd.ExitInfo
		want int32
	}{
		{"status", forkd.ExitInfo{ExitCode: code(7)}, 7},
		{"status wraps to a byte", forkd.ExitInfo{ExitCode: code(256 + 5)}, 5},
		{"signal", forkd.ExitInfo{Signal: 15}, 143},
		{"neither", forkd.ExitInfo{}, 0},
	} {
		if got := exitCode(&tc.exit); got != tc.want {
			t.Errorf("%s: exitCode() = %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestForkdPath(t *testing.T) {
	if got, want := forkdPath("/a/b/gvisor/runsc"), "/a/b/gvisor/gvisor-bin/forkd"; got != want {
		t.Errorf("forkdPath() = %q, want %q", got, want)
	}
}

// The RPCs refuse work a worker was not started for, before they look at the
// request.
func TestForkRPCsRefuseWithoutForkSupport(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		gates []string
		fork  bool
	}{
		{"worker without --fork-platform", []string{"ForkSeed"}, false},
		{"gate off", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preview.InitForTest(t, tt.gates...)
			s := &AteWorkerService{}
			s.epoch.Store(forkTestEpoch)
			if tt.fork {
				s.fork = newForkManager(&fakeLauncher{}, &fakeHost{})
			}
			for name, call := range map[string]func() error{
				"SeedWorkload": func() error { _, err := s.SeedWorkload(ctx, &ateworkerpb.SeedWorkloadRequest{}); return err },
				"ForkWorkload": func() error { _, err := s.ForkWorkload(ctx, &ateworkerpb.ForkWorkloadRequest{}); return err },
				"DropSeed":     func() error { _, err := s.DropSeed(ctx, &ateworkerpb.DropSeedRequest{}); return err },
			} {
				if got := apierror.Code(call()); got != codes.FailedPrecondition {
					t.Errorf("%s() = %v, want FailedPrecondition", name, got)
				}
			}
		})
	}
}

// A worker that can fork still validates and waits for registration.
func TestForkRPCsValidateAndNeedRegistration(t *testing.T) {
	preview.InitForTest(t, "ForkSeed")
	s := &AteWorkerService{fork: newForkManager(&fakeLauncher{}, &fakeHost{})}
	ctx := context.Background()
	if _, err := s.SeedWorkload(ctx, &ateworkerpb.SeedWorkloadRequest{}); apierror.Code(err) != codes.InvalidArgument {
		t.Errorf("SeedWorkload() of an empty request = %v, want InvalidArgument", err)
	}
	if _, err := s.ForkWorkload(ctx, &ateworkerpb.ForkWorkloadRequest{}); apierror.Code(err) != codes.InvalidArgument {
		t.Errorf("ForkWorkload() of an empty request = %v, want InvalidArgument", err)
	}
	if _, err := s.DropSeed(ctx, &ateworkerpb.DropSeedRequest{}); apierror.Code(err) != codes.InvalidArgument {
		t.Errorf("DropSeed() of an empty request = %v, want InvalidArgument", err)
	}
}
