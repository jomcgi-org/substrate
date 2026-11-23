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
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/forkd"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/roottest"
	"go.ate.dev/substrate/internal/wakeupprobe"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
)

// These tests run the fork manager against the real forkd and real runsc on a
// host with KVM, without a cluster. The worker's network is real (actornet
// builds the namespaces, veth pair and egress rules of a worker pod), forkd
// makes the cgroups, and the guests are real KVM sandboxes. Only atunnel and
// the control plane are absent.
//
// ATE_TEST_FORKD_DIR names a directory with runsc and gvisor-bin/forkd (the
// layout of the SandboxConfig tarball of the gVisor fork). mkfs.erofs and a
// static busybox must be on PATH for the busybox image. ATE_TEST_ENGINE_IMAGE
// optionally names a digest-pinned EROFS image of a scan engine that serves
// one request on :8080 (the FORKSRV_NET shim) for the engine test.

func forkdHostDir(t *testing.T) string {
	t.Helper()
	roottest.Require(t, "forkd, cgroups and network namespaces")
	dir := os.Getenv("ATE_TEST_FORKD_DIR")
	if dir == "" {
		t.Skip("ATE_TEST_FORKD_DIR names no directory with runsc and gvisor-bin/forkd")
	}
	for _, p := range []string{"runsc", "gvisor-bin/forkd", "gvisor-bin/gvisor_sentry"} {
		if _, err := os.Stat(filepath.Join(dir, p)); err != nil {
			t.Skipf("ATE_TEST_FORKD_DIR lacks %s: %v", p, err)
		}
	}
	if _, err := os.Stat("/dev/kvm"); err != nil {
		t.Skip("needs /dev/kvm")
	}
	var st unix.Statfs_t
	if err := unix.Statfs("/sys/fs/cgroup", &st); err != nil || st.Type != 0x63677270 {
		t.Skip("needs cgroup v2")
	}
	return dir
}

// guestScript is the guest of the busybox image. It initializes, asks for the
// fork checkpoint, and then follows the ATE_ACTOR_T_* entries of the spec
// environment that the test put into the fork request:
//
//	ATE_ACTOR_T_EXIT_NOW=<n>  exit with n before it serves anything
//	ATE_ACTOR_T_SERVE=<n>     serve n connections, then exit with ATE_ACTOR_T_EXIT (0 serves forever)
//	ATE_ACTOR_T_ALLOC_MB=<n>  write n MB to /tmp first
//	ATE_ACTOR_T_EGRESS=1      try an outbound TCP connection and print the result
const guestScript = `/busybox echo TEMPLATE_INITIALIZED
exec 3<>/proc/gvisor/checkpoint
echo 1 >&3
/busybox cat <&3 | /busybox grep -qx restore || exit 99
E=$(/busybox tr '\0' '\n' </proc/gvisor/spec_environ)
v() { echo "$E" | /busybox sed -n "s/^$1=//p"; }
/busybox echo READY
[ -n "$(v ATE_ACTOR_T_EXIT_NOW)" ] && exit $(v ATE_ACTOR_T_EXIT_NOW)
[ -n "$(v ATE_ACTOR_T_ALLOC_MB)" ] && /busybox dd if=/dev/zero of=/tmp/big bs=1M count=$(v ATE_ACTOR_T_ALLOC_MB) 2>/dev/null
if [ -n "$(v ATE_ACTOR_T_EGRESS)" ]; then
  /busybox nc -w 3 93.184.216.34 80 </dev/null >/dev/null 2>&1
  echo EGRESS_RC=$?
fi
N=$(v ATE_ACTOR_T_SERVE); N=${N:-0}; i=0
echo SERVING
while [ "$N" = 0 ] || [ $i -lt "$N" ]; do
  /busybox nc -l -p 8080 -w 5 </reply >/dev/null 2>&1
  i=$((i+1))
done
X=$(v ATE_ACTOR_T_EXIT)
exit ${X:-0}
`

// buildBusyboxImage returns the path and SHA-256 of a read-only EROFS image
// with a static busybox and the canned HTTP reply the guest serves.
func buildBusyboxImage(t *testing.T, dir string) (path, digest string) {
	t.Helper()
	root := filepath.Join(dir, "rootfs")
	for _, d := range []string{"", "dev", "proc", "sys", "tmp", "etc"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	bb, err := exec.LookPath("busybox")
	if err != nil {
		t.Skipf("needs a static busybox: %v", err)
	}
	if _, err := exec.LookPath("mkfs.erofs"); err != nil {
		t.Skipf("needs mkfs.erofs: %v", err)
	}
	if out, err := exec.Command("cp", bb, filepath.Join(root, "busybox")).CombinedOutput(); err != nil {
		t.Fatalf("cp: %v\n%s", err, out)
	}
	reply := "HTTP/1.0 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
	if err := os.WriteFile(filepath.Join(root, "reply"), []byte(reply), 0o444); err != nil {
		t.Fatal(err)
	}
	// The image is built the way atelet builds it (cmd/atelet/erofs.go): mkfs.erofs
	// reads a tar stream, without inline data, with a fixed timestamp and UUID.
	path = filepath.Join(dir, "busybox.erofs")
	mkfs := exec.Command("mkfs.erofs", "--tar=f", "-E", "noinline_data", "-T", "0", "-U", erofsUUID, "--quiet", path)
	tarCmd := exec.Command("tar", "--sort=name", "--owner=0", "--group=0", "--numeric-owner", "-C", root, "-cf", "-", ".")
	pipe, err := tarCmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	mkfs.Stdin = pipe
	var mkfsOut bytes.Buffer
	mkfs.Stdout, mkfs.Stderr = &mkfsOut, &mkfsOut
	if err := mkfs.Start(); err != nil {
		t.Fatalf("mkfs.erofs: %v", err)
	}
	if err := tarCmd.Run(); err != nil {
		t.Fatalf("tar: %v", err)
	}
	if err := mkfs.Wait(); err != nil {
		t.Fatalf("mkfs.erofs: %v\n%s", err, mkfsOut.String())
	}
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	return path, fileSHA256Must(t, path)
}

// erofsUUID is the filesystem UUID that atelet gives every image.
const erofsUUID = "00000000-0000-0000-0000-000000000001"

func fileSHA256Must(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// testLauncher is the worker's launcher that also remembers forkd's pid.
type testLauncher struct {
	execForkdLauncher
	mu       sync.Mutex
	launches int
	pid      int
}

func (l *testLauncher) Launch(ctx context.Context, binary string) (*forkd.Client, <-chan struct{}, error) {
	c, exited, err := l.execForkdLauncher.Launch(ctx, binary)
	if err != nil {
		return nil, nil, err
	}
	out, _ := exec.Command("pgrep", "-f", "--", "--forkd-state-dir="+l.stateDir).Output()
	pid, _ := strconv.Atoi(strings.Fields(string(out) + " 0")[0])
	l.mu.Lock()
	l.launches++
	l.pid = pid
	l.mu.Unlock()
	return c, exited, nil
}

func (l *testLauncher) forkdPID() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.pid
}

// realHost is the worker's network and probe around a forked actor, with the
// real namespaces and the real wakeup probe. atunnel is not started, so the
// egress and DNS servers are absent, which fails closed.
type realHost struct {
	mu       sync.Mutex
	sessions map[string]*actornet.SandboxSession
	hosted   int
	unhosted int
	// probe replaces the HTTP wakeup probe when set.
	probe func(ctx context.Context, uid string) error
	// hostMS are the times Host took, in milliseconds.
	hostMS []float64
}

func (h *realHost) Host(ctx context.Context, actor resources.ActorAttribution, _ *ateworkerpb.ActorDirs, _ int64) (string, error) {
	start := time.Now()
	session, err := actornet.ServeSandbox(ctx, actornet.SandboxNetworkConfig{ActorUID: actor.UID, Veth: true, EgressPort: 15001}, nil, nil)
	if err != nil {
		return "", err
	}
	h.mu.Lock()
	h.sessions[actor.UID] = session
	h.hosted++
	h.hostMS = append(h.hostMS, float64(time.Since(start).Microseconds())/1000)
	h.mu.Unlock()
	return fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), int(session.Network.RuntimeNetNS)), nil
}

func (h *realHost) dialer(uid string) wakeupprobe.DialFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		h.mu.Lock()
		session := h.sessions[uid]
		h.mu.Unlock()
		if session == nil {
			return nil, fmt.Errorf("actor %s is not hosted here", uid)
		}
		return session.Dialer()(ctx, network, address)
	}
}

func (h *realHost) Probe(ctx context.Context, uid string, containers []*ateworkerpb.Container) error {
	h.mu.Lock()
	probe := h.probe
	h.mu.Unlock()
	if probe != nil {
		return probe(ctx, uid)
	}
	return wakeupprobe.WaitAll(ctx, containers, actornet.ActorVethIP, h.dialer(uid))
}

func (h *realHost) Activate(resources.ActorAttribution) error { return nil }

func (h *realHost) Unhost(ctx context.Context, actor resources.ActorAttribution) error {
	h.mu.Lock()
	session := h.sessions[actor.UID]
	delete(h.sessions, actor.UID)
	if session != nil {
		h.unhosted++
	}
	h.mu.Unlock()
	if session == nil {
		return nil
	}
	return session.Close(ctx)
}

func (h *realHost) counts() (hosted, unhosted int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hosted, h.unhosted
}

// realFixture is a fork manager with the real forkd.
type realFixture struct {
	t          *testing.T
	m          *forkManager
	host       *realHost
	rep        *recordingReporter
	launcher   *testLauncher
	forkdDir   string
	cgroupRoot string
	dir        string
	image      string
	sha        string
	logs       sync.Map // uid -> [2]string
	seq        atomic.Uint64
}

func newRealFixture(t *testing.T) *realFixture {
	t.Helper()
	forkdDir := forkdHostDir(t)
	dir := t.TempDir()
	// A cgroup without processes that delegates the controllers the way the
	// worker's own cgroup does after cgroup.Delegate.
	root := fmt.Sprintf("/sys/fs/cgroup/ate-forkit-%d-%d", os.Getpid(), time.Now().UnixNano()%100000)
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Skipf("cannot create a cgroup: %v", err)
	}
	t.Cleanup(func() { removeCgroupTree(root) })
	for _, c := range []string{"memory", "pids", "cpu"} {
		if err := os.WriteFile(filepath.Join(root, "cgroup.subtree_control"), []byte("+"+c), 0o644); err != nil {
			t.Skipf("cannot delegate %s: %v", c, err)
		}
	}
	f := &realFixture{
		t: t, forkdDir: forkdDir, cgroupRoot: root, dir: dir,
		host: &realHost{sessions: map[string]*actornet.SandboxSession{}},
		rep:  newRecordingReporter(),
	}
	f.launcher = &testLauncher{execForkdLauncher: execForkdLauncher{
		stateDir: filepath.Join(dir, "forkd"), cgroupParent: filepath.Join(root, "forkd"), uid: os.Getuid(),
	}}
	f.m = newForkManager(f.launcher, f.host)
	f.m.reporter = f.rep
	f.m.waitRetry = 50 * time.Millisecond
	f.m.lockActor = func(context.Context, string) (func(), bool) { return func() {}, true }
	f.m.tailLogs = func(a resources.ActorAttribution, stdout, stderr string) func() {
		f.logs.Store(a.UID, [2]string{stdout, stderr})
		return func() {}
	}
	t.Cleanup(func() { f.shutdown() })
	return f
}

// shutdown drops what the test left, stops forkd and checks that nothing of
// this fixture stays on the host.
func (f *realFixture) shutdown() {
	f.m.mu.Lock()
	var seeds []string
	for uid := range f.m.seeds {
		seeds = append(seeds, uid)
	}
	f.m.mu.Unlock()
	f.m.shuttingDown = func() bool { return true }
	for _, uid := range seeds {
		_ = f.m.dropSeed(context.Background(), uid)
	}
	f.m.mu.Lock()
	var kids []string
	for uid := range f.m.children {
		kids = append(kids, uid)
	}
	f.m.mu.Unlock()
	for _, uid := range kids {
		f.m.dropChild(context.Background(), uid)
	}
	if pid := f.launcher.forkdPID(); pid > 0 {
		_ = unix.Kill(pid, unix.SIGTERM)
		waitGone(pid, 30*time.Second)
	}
	for _, name := range f.hostedNetNS() {
		f.t.Errorf("network namespace %s remains after teardown", name)
	}
}

func waitGone(pid int, d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if err := unix.Kill(pid, 0); err != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = unix.Kill(pid, unix.SIGKILL)
}

func removeCgroupTree(dir string) {
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.IsDir() {
			removeCgroupTree(filepath.Join(dir, e.Name()))
		}
	}
	_ = os.WriteFile(filepath.Join(dir, "cgroup.kill"), []byte("1"), 0o644)
	for i := 0; i < 100; i++ {
		if err := unix.Rmdir(dir); err == nil || err == unix.ENOENT {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// hostedNetNS lists the named namespaces of actors that this fixture hosted.
func (f *realFixture) hostedNetNS() []string {
	var out []string
	entries, _ := os.ReadDir("/run/netns")
	prefix := nodepath.ActorNetNSName("")
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) && strings.Contains(e.Name(), f.idPrefix()) {
			out = append(out, e.Name())
		}
	}
	return out
}

// idPrefix is the part of every UID of this fixture.
func (f *realFixture) idPrefix() string { return fmt.Sprintf("%08x", f.t.Name()[0]) + "-" + f.tag() }

func (f *realFixture) tag() string {
	h := sha256.Sum256([]byte(f.dir))
	return fmt.Sprintf("%x", h[:2])
}

// uid makes a UUID-shaped identifier that names this fixture and a role.
func (f *realFixture) uid(role string) string {
	n := f.seq.Add(1)
	return fmt.Sprintf("%08x-%s-4000-8000-%012x", f.t.Name()[0], f.tag(), uint64(len(role))<<40|n)
}

func (f *realFixture) useImage(path, sha string) {
	f.image, f.sha = path, sha
}

func (f *realFixture) seedRequest(atespace, uid string, args, env []string, memory int64) *ateworkerpb.SeedWorkloadRequest {
	return &ateworkerpb.SeedWorkloadRequest{
		Atespace: atespace, ActorName: "seed-" + uid[:8], ActorUid: uid,
		ActorTemplateAtespace: atespace, ActorTemplateName: "scan", TemplateUid: "71234567-89ab-cdef-0123-456789abcdef",
		RunscPath:       filepath.Join(f.forkdDir, "runsc"),
		RootfsImagePath: f.image, RootfsSha256: f.sha,
		Resources: &ateworkerpb.ResourceRequirements{Limits: &ateworkerpb.Resources{CpuMillicores: 1000, MemoryBytes: memory}},
		ActorDirs: &ateworkerpb.ActorDirs{RootDir: filepath.Dir(f.image)},
		Spec: &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{{
			Name:          "main",
			ContainerSpec: &ateworkerpb.ContainerSpec{Args: args, Env: env, Cwd: "/"},
		}}},
	}
}

func (f *realFixture) forkRequest(atespace, seedUID, uid string, memory int64, env ...string) *ateworkerpb.ForkWorkloadRequest {
	return &ateworkerpb.ForkWorkloadRequest{
		Atespace: atespace, ActorName: "fork-" + uid[:8], ActorUid: uid,
		ActorTemplateAtespace: atespace, ActorTemplateName: "scan", TemplateUid: "71234567-89ab-cdef-0123-456789abcdef",
		SeedActorUid: seedUID, SeedWorkerEpoch: forkTestEpoch,
		Resources: &ateworkerpb.ResourceRequirements{Limits: &ateworkerpb.Resources{CpuMillicores: 1000, MemoryBytes: memory}},
		Env:       append([]string{"ATE_ACTOR_NAME=fork-" + uid[:8], "ATE_ACTOR_UID=" + uid}, env...),
		PidsLimit: 64, LogBytesLimit: 1 << 20, MaxLifetimeSeconds: 120,
		ActorDirs: &ateworkerpb.ActorDirs{RootDir: f.dir},
		Spec: &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{{
			Name: "main", WakeupProbe: &ateworkerpb.WakeupProbe{HttpGet: &ateworkerpb.HttpGetAction{Path: "/healthz", Port: 8080}, TimeoutSeconds: 20},
		}}},
	}
}

// busyboxSeed boots the busybox seed of the atespace.
func (f *realFixture) busyboxSeed(atespace string, memory int64) string {
	f.t.Helper()
	if f.image == "" {
		path, sha := buildBusyboxImage(f.t, f.dir)
		f.useImage(path, sha)
	}
	uid := f.uid("seed")
	req := f.seedRequest(atespace, uid, []string{"/busybox", "sh", "-c", guestScript}, []string{"PATH=/"}, memory)
	if err := f.m.seed(f.t.Context(), req, forkTestEpoch); err != nil {
		f.t.Fatalf("seed: %v", err)
	}
	return uid
}

func (f *realFixture) forkChild(atespace, seedUID string, memory int64, env ...string) (string, error) {
	uid := f.uid("child")
	return uid, f.m.fork(f.t.Context(), f.forkRequest(atespace, seedUID, uid, memory, env...), forkTestEpoch)
}

func (f *realFixture) logOf(uid string) (stdout, stderr string) {
	v, ok := f.logs.Load(uid)
	if !ok {
		return "", ""
	}
	paths := v.([2]string)
	a, _ := os.ReadFile(paths[0])
	b, _ := os.ReadFile(paths[1])
	return string(a), string(b)
}

// waitReport waits for the report of uid.
func (f *realFixture) waitReport(uid string) (exited bool, code int32, crash string) {
	f.t.Helper()
	deadline := time.After(60 * time.Second)
	for {
		f.rep.mu.Lock()
		if c, ok := f.rep.exited[uid]; ok {
			f.rep.mu.Unlock()
			return true, c, ""
		}
		if m, ok := f.rep.crashed[uid]; ok {
			f.rep.mu.Unlock()
			return false, 0, m
		}
		f.rep.mu.Unlock()
		select {
		case <-f.rep.notify:
		case <-deadline:
			f.t.Fatalf("no report for %s", uid)
		}
	}
}

// get sends one HTTP request into the actor's namespace. The busybox guest
// listens again between connections, so a refused connection is retried.
func (f *realFixture) get(uid, method, path string, body []byte) (int, []byte, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		code, data, err := f.getOnce(uid, method, path, body)
		if err == nil || !strings.Contains(err.Error(), "connection refused") || time.Now().After(deadline) {
			return code, data, err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (f *realFixture) getOnce(uid, method, path string, body []byte) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(f.t.Context(), 60*time.Second)
	defer cancel()
	client := &http.Client{Transport: &http.Transport{DialContext: f.host.dialer(uid), DisableKeepAlives: true}}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+actornet.ActorVethIP+":8080"+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return resp.StatusCode, data, err
}

func (f *realFixture) forkdChildren() []forkd.ChildInfo {
	f.m.mu.Lock()
	client := f.m.client
	f.m.mu.Unlock()
	if client == nil {
		return nil
	}
	_, kids, err := client.List(f.t.Context())
	if err != nil {
		f.t.Fatalf("forkd list: %v", err)
	}
	return kids
}

func (f *realFixture) forkdSeeds() []forkd.SeedInfo {
	f.m.mu.Lock()
	client := f.m.client
	f.m.mu.Unlock()
	if client == nil {
		return nil
	}
	seeds, _, err := client.List(f.t.Context())
	if err != nil {
		f.t.Fatalf("forkd list: %v", err)
	}
	return seeds
}

func readCgroup(t *testing.T, dir, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

func codeOfErr(err error) codes.Code { return apierror.Code(err) }

// A forked actor runs the whole path: network, forkd, probe, request, exit,
// report and cleanup. A single-use child that exits is deleted by the worker.
func TestRealForkdSingleUseChild(t *testing.T) {
	f := newRealFixture(t)
	seedUID := f.busyboxSeed("team-a", 1<<30)

	// The seed runs in the capped cgroup forkd made for it.
	seeds := f.forkdSeeds()
	if len(seeds) != 1 || seeds[0].CgroupPath != filepath.Join(f.cgroupRoot, "forkd", "_seed."+seedUID) {
		t.Fatalf("forkd seeds = %+v", seeds)
	}
	seedCg := seeds[0].CgroupPath
	if got := readCgroup(t, seedCg, "memory.max"); got != fmt.Sprint(1<<30) {
		t.Errorf("seed memory.max = %s, want the seed resources (1Gi)", got)
	}
	if got := readCgroup(t, seedCg, "memory.high"); got != "max" {
		t.Errorf("seed memory.high = %s, want none", got)
	}

	// One request, then the guest exits with code 7.
	uid, err := f.forkChild("team-a", seedUID, 256<<20, "ATE_ACTOR_T_SERVE=2", "ATE_ACTOR_T_EXIT=7")
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	// The child's cgroup is the template's memory limit plus the fork overhead,
	// with no memory.high and the pids limit of the request.
	kids := f.forkdChildren()
	if len(kids) != 1 {
		t.Fatalf("forkd children = %+v", kids)
	}
	cg := kids[0].CgroupPath
	if got, want := readCgroup(t, cg, "memory.max"), fmt.Sprint(256<<20+forkOverhead.GetMemoryBytes()); got != want {
		t.Errorf("child memory.max = %s, want %s", got, want)
	}
	if got := readCgroup(t, cg, "memory.high"); got != "max" {
		t.Errorf("child memory.high = %s, want none", got)
	}
	if got := readCgroup(t, cg, "pids.max"); got != "64" {
		t.Errorf("child pids.max = %s, want 64", got)
	}
	if got := readCgroup(t, cg, "cpu.max"); got != "100000 100000" {
		t.Errorf("child cpu.max = %q, want one cpu", got)
	}

	waitFor(t, "the guest to serve", func() bool { out, _ := f.logOf(uid); return strings.Contains(out, "SERVING") })
	if out, _ := f.logOf(uid); !strings.Contains(out, "READY") {
		t.Errorf("child stdout = %q", out)
	}
	code, body, err := f.get(uid, "GET", "/", nil)
	if err != nil || code != 200 || string(body) != "ok" {
		t.Fatalf("request = %d %q, %v", code, body, err)
	}
	exited, exitCode, crash := f.waitReport(uid)
	if !exited || exitCode != 7 {
		t.Fatalf("report = exited %v code %d crash %q, want ReportExited with code 7", exited, exitCode, crash)
	}
	// Single use: the child is gone from forkd, its cgroup and namespaces too.
	waitFor(t, "the child to be deleted from forkd", func() bool { return len(f.forkdChildren()) == 0 })
	waitFor(t, "the child's cgroup to go", func() bool { _, err := os.Stat(cg); return os.IsNotExist(err) })
	if h, u := f.host.counts(); h != 1 || u != 1 {
		t.Errorf("host calls: hosted %d unhosted %d, want 1 and 1", h, u)
	}
	if ns := f.hostedNetNS(); len(ns) != 0 {
		t.Errorf("namespaces remain after the child ended: %v", ns)
	}
	if got := readCgroup(t, seedCg, "memory.max"); got != fmt.Sprint(1<<30) {
		t.Errorf("seed cgroup changed to %s", got)
	}
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A child that exits before the wakeup probe passes fails the fork and leaves
// nothing behind.
func TestRealForkdChildExitsBeforeItServes(t *testing.T) {
	f := newRealFixture(t)
	seedUID := f.busyboxSeed("team-a", 1<<30)
	f.host.mu.Lock()
	f.host.probe = func(ctx context.Context, uid string) error {
		// Wait for the guest to end on its own.
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			kids := f.forkdChildren()
			if len(kids) == 0 || kids[0].State == forkd.StateExited {
				return fmt.Errorf("the guest exited before it served")
			}
		}
	}
	f.host.mu.Unlock()
	uid, err := f.forkChild("team-a", seedUID, 256<<20, "ATE_ACTOR_T_EXIT_NOW=3")
	if err == nil {
		t.Fatal("fork of a child that exits at once succeeded")
	}
	t.Logf("fork error: %v", err)
	// The failed fork or the child's watcher cleans up, whichever comes first,
	// and the control plane is told the child ended.
	waitFor(t, "the child to be cleaned up", func() bool { h, u := f.host.counts(); return len(f.forkdChildren()) == 0 && h == u })
	if ns := f.hostedNetNS(); len(ns) != 0 {
		t.Errorf("namespaces remain: %v", ns)
	}
	exited, code, crash := f.waitReport(uid)
	if !exited || code != 3 {
		t.Errorf("report = exited %v code %d crash %q, want exit code 3", exited, code, crash)
	}
}

// A child above its memory limit is killed by the kernel, not throttled, and
// forkd reports why.
func TestRealForkdMemoryLimitKillsTheChild(t *testing.T) {
	f := newRealFixture(t)
	seedUID := f.busyboxSeed("team-a", 1<<30)
	// 200 MB of tmpfs pages under a 64 MiB limit plus the fork overhead.
	uid, err := f.forkChild("team-a", seedUID, 64<<20, "ATE_ACTOR_T_ALLOC_MB=200")
	if err != nil {
		// The kill can land before the probe passes.
		t.Logf("fork ended early: %v", err)
	}
	exited, code, crash := f.waitReport(uid)
	if err == nil {
		t.Logf("report: exited=%v code=%d crash=%q", exited, code, crash)
		if exited && code == 0 {
			t.Errorf("a child that writes 200 MB survived a 64 MiB limit")
		}
	}
	waitFor(t, "the child to be deleted", func() bool { return len(f.forkdChildren()) == 0 })
}

// A child that serves a request keeps running under its limit.
func TestRealForkdChildServesRepeatedRequests(t *testing.T) {
	f := newRealFixture(t)
	seedUID := f.busyboxSeed("team-a", 1<<30)
	uid, err := f.forkChild("team-a", seedUID, 128<<20, "ATE_ACTOR_T_ALLOC_MB=20")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if code, body, err := f.get(uid, "GET", "/", nil); err != nil || code != 200 || string(body) != "ok" {
			t.Fatalf("request %d = %d %q, %v", i, code, body, err)
		}
	}
	// Terminate, as the control plane does: no report is expected, since the
	// control plane asked for it.
	f.m.shuttingDown = func() bool { return false }
	if err := f.m.terminate(t.Context(), uid); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the child to be deleted", func() bool { return len(f.forkdChildren()) == 0 })
	if ns := f.hostedNetNS(); len(ns) != 0 {
		t.Errorf("namespaces remain: %v", ns)
	}
}

// A forked actor has no egress: the redirect to atunnel finds nothing serving.
func TestRealForkdNoEgress(t *testing.T) {
	f := newRealFixture(t)
	seedUID := f.busyboxSeed("team-a", 1<<30)
	uid, err := f.forkChild("team-a", seedUID, 128<<20, "ATE_ACTOR_T_EGRESS=1")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the guest's egress attempt", func() bool {
		out, _ := f.logOf(uid)
		return strings.Contains(out, "EGRESS_RC=")
	})
	out, _ := f.logOf(uid)
	if strings.Contains(out, "EGRESS_RC=0") {
		t.Errorf("the guest connected to the internet: %q", out)
	}
	t.Logf("guest output: %q", out)
}

// A fork asked for by another atespace, template or epoch is refused by the
// worker, and a second atespace gets no seed on a worker that holds one.
func TestRealForkdAtespaceChecks(t *testing.T) {
	f := newRealFixture(t)
	seedUID := f.busyboxSeed("team-a", 1<<30)

	if _, err := f.forkChild("team-b", seedUID, 128<<20); codeOfErr(err) != codes.PermissionDenied {
		t.Errorf("fork from another atespace = %v, want PermissionDenied", err)
	}
	req := f.forkRequest("team-a", seedUID, f.uid("child"), 128<<20)
	req.TemplateUid = "81234567-89ab-cdef-0123-456789abcdef"
	if err := f.m.fork(t.Context(), req, forkTestEpoch); codeOfErr(err) != codes.PermissionDenied {
		t.Errorf("fork of another template = %v, want PermissionDenied", err)
	}
	req = f.forkRequest("team-a", seedUID, f.uid("child"), 128<<20)
	if err := f.m.fork(t.Context(), req, forkTestEpoch+1); codeOfErr(err) != codes.FailedPrecondition {
		t.Errorf("fork on another worker epoch = %v, want FailedPrecondition", err)
	}
	req = f.forkRequest("team-a", f.uid("seed"), f.uid("child"), 128<<20)
	if err := f.m.fork(t.Context(), req, forkTestEpoch); codeOfErr(err) != codes.NotFound {
		t.Errorf("fork of a seed this worker lacks = %v, want NotFound", err)
	}
	other := f.seedRequest("team-b", f.uid("seed"), []string{"/busybox", "sh", "-c", guestScript}, []string{"PATH=/"}, 1<<30)
	if err := f.m.seed(t.Context(), other, forkTestEpoch); codeOfErr(err) != codes.PermissionDenied {
		t.Errorf("seed of a second atespace = %v, want PermissionDenied", err)
	}
	// Nothing leaked from the refused calls.
	if kids := f.forkdChildren(); len(kids) != 0 {
		t.Errorf("forkd holds children after refused forks: %+v", kids)
	}
	if ns := f.hostedNetNS(); len(ns) != 0 {
		t.Errorf("namespaces remain after refused forks: %v", ns)
	}
	// The real forkd refuses a fork of a seed it does not hold, too.
	f.m.mu.Lock()
	client := f.m.client
	f.m.mu.Unlock()
	_, err := client.Fork(t.Context(), &forkd.ForkRequest{SeedID: "no-such-seed", ChildID: "x", Cgroup: &forkd.Cgroup{MemoryMax: 1 << 28, PidsMax: 64}})
	if !forkd.IsCode(err, forkd.CodeNotFound) {
		t.Errorf("forkd fork of an unknown seed = %v, want not_found", err)
	}
}

// The per-seed cap is a real limit: a seed whose template needs more than
// seed_resources does not boot, and leaves no cgroup.
func TestRealForkdSeedCap(t *testing.T) {
	f := newRealFixture(t)
	path, sha := buildBusyboxImage(t, f.dir)
	f.useImage(path, sha)
	uid := f.uid("seed")
	req := f.seedRequest("team-a", uid, []string{"/busybox", "sh", "-c", guestScript}, []string{"PATH=/"}, 6<<20)
	if err := f.m.seed(t.Context(), req, forkTestEpoch); err == nil {
		t.Fatal("a seed with 6 MiB booted")
	} else {
		t.Logf("seed with 6 MiB: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.cgroupRoot, "forkd", "_seed."+uid)); !os.IsNotExist(err) {
		t.Errorf("the failed seed's cgroup remains: %v", err)
	}
	if seeds := f.forkdSeeds(); len(seeds) != 0 {
		t.Errorf("forkd holds seeds after the failed seed: %+v", seeds)
	}
	// No limit at all is refused before forkd sees it.
	req.Resources.Limits.MemoryBytes = 0
	if err := f.m.seed(t.Context(), req, forkTestEpoch); codeOfErr(err) != codes.InvalidArgument {
		t.Errorf("seed with no memory limit = %v, want InvalidArgument", err)
	}
	// A cap that fits boots.
	req.Resources.Limits.MemoryBytes = 256 << 20
	if err := f.m.seed(t.Context(), req, forkTestEpoch); err != nil {
		t.Fatalf("seed with 256 MiB: %v", err)
	}
}

// forkd dies with seeds and children running: the worker reports them, cleans
// up the networks and the next seed starts a new forkd that removes what the
// old one left.
func TestRealForkdKilledMidRun(t *testing.T) {
	f := newRealFixture(t)
	seedUID := f.busyboxSeed("team-a", 1<<30)
	var kids []string
	for i := 0; i < 3; i++ {
		uid, err := f.forkChild("team-a", seedUID, 128<<20)
		if err != nil {
			t.Fatal(err)
		}
		kids = append(kids, uid)
	}
	if h, _ := f.host.counts(); h != 3 {
		t.Fatalf("hosted %d children, want 3", h)
	}
	pid := f.launcher.forkdPID()
	if pid == 0 {
		t.Fatal("forkd pid unknown")
	}
	// The processes that kill -9 leaves running: the template and the children.
	oldPIDs := []int{}
	for _, s := range f.forkdSeeds() {
		oldPIDs = append(oldPIDs, s.PID)
	}
	for _, k := range f.forkdChildren() {
		oldPIDs = append(oldPIDs, k.PID)
	}
	f.m.shuttingDown = func() bool { return false }
	if err := unix.Kill(pid, unix.SIGKILL); err != nil {
		t.Fatal(err)
	}
	// The seed and every child are reported crashed, once each.
	reported := map[string]string{}
	for len(reported) < 4 {
		uid := f.rep.next(t)
		f.rep.mu.Lock()
		reported[uid] = f.rep.crashed[uid]
		f.rep.mu.Unlock()
	}
	for _, uid := range append([]string{seedUID}, kids...) {
		if msg, ok := reported[uid]; !ok || msg == "" {
			t.Errorf("%s was not reported crashed: %q", uid, msg)
		}
	}
	t.Logf("reports: %v", reported)
	waitFor(t, "the children's networks to be removed", func() bool { _, u := f.host.counts(); return u == 3 })
	if ns := f.hostedNetNS(); len(ns) != 0 {
		t.Errorf("namespaces remain: %v", ns)
	}

	// The killed forkd cannot clean up: its template and leaves remain, until the
	// next forkd starts.
	forkdCg := filepath.Join(f.cgroupRoot, "forkd")
	leftovers, _ := os.ReadDir(forkdCg)
	var left []string
	for _, e := range leftovers {
		if e.IsDir() {
			left = append(left, e.Name())
		}
	}
	t.Logf("cgroup leaves after kill -9: %v", left)

	newSeed := f.uid("seed")
	req := f.seedRequest("team-a", newSeed, []string{"/busybox", "sh", "-c", guestScript}, []string{"PATH=/"}, 1<<30)
	if err := f.m.seed(t.Context(), req, forkTestEpoch); err != nil {
		t.Fatalf("seed after forkd was killed: %v", err)
	}
	if got := f.launcher.launches; got != 2 {
		t.Errorf("forkd launched %d times, want 2", got)
	}
	entries, _ := os.ReadDir(forkdCg)
	var now []string
	for _, e := range entries {
		if e.IsDir() {
			now = append(now, e.Name())
		}
	}
	if len(now) != 1 || now[0] != "_seed."+newSeed {
		t.Errorf("cgroup leaves after the restart = %v, want only the new seed's", now)
	}
	for _, old := range oldPIDs {
		if err := unix.Kill(old, 0); err == nil {
			t.Errorf("process %d of the killed forkd is still running after the new forkd started", old)
		}
	}
	// A child of the new seed works.
	uid, err := f.forkChild("team-a", newSeed, 128<<20)
	if err != nil {
		t.Fatalf("fork from the new seed: %v", err)
	}
	if code, body, err := f.get(uid, "GET", "/", nil); err != nil || code != 200 || string(body) != "ok" {
		t.Fatalf("request = %d %q, %v", code, body, err)
	}
}

// The public scan engine behind forkd and the worker's network: N children at
// once, one POST each, with the timings the forkd numbers of the gVisor side
// can be compared with.
func TestRealForkdEngine(t *testing.T) {
	image := os.Getenv("ATE_TEST_ENGINE_IMAGE")
	if image == "" {
		t.Skip("ATE_TEST_ENGINE_IMAGE names no engine image")
	}
	bodyPath := os.Getenv("ATE_TEST_ENGINE_BODY")
	body, err := os.ReadFile(bodyPath)
	if err != nil {
		t.Skipf("ATE_TEST_ENGINE_BODY: %v", err)
	}
	f := newRealFixture(t)
	f.useImage(image, fileSHA256Must(t, image))
	// The engine's one-shot server must not see the probe: wait until it
	// listens, which its stderr says.
	f.host.mu.Lock()
	f.host.probe = func(ctx context.Context, uid string) error {
		for {
			if _, errOut := f.logOf(uid); strings.Contains(errOut, "SHIM listen=") {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Microsecond):
			}
		}
	}
	f.host.mu.Unlock()
	env := []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "LD_LIBRARY_PATH=/semgrep/libs", "LD_PRELOAD=/semgrep/shim.so",
		"FORKSRV_TARGET=/scan/target.json", "FORKSRV_NET=8080", "FORKSRV_HOLD=1", "FORKSRV_CHECKPOINT=1", "FORKSRV_BALLAST_MB=64",
	}
	uid := f.uid("seed")
	req := f.seedRequest("team-a", uid, strings.Fields("/semgrep/semgrep-core -rules /semgrep/rules.json -targets /semgrep/targets.json -json -j 1"), env, 2<<30)
	start := time.Now()
	if err := f.m.seed(t.Context(), req, forkTestEpoch); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Logf("FULLPATH seed_ms=%d", time.Since(start).Milliseconds())
	seedCg := f.forkdSeeds()[0].CgroupPath
	t.Logf("FULLPATH seed_cgroup memory.current=%s peak=%s", readCgroup(t, seedCg, "memory.current"), readCgroup(t, seedCg, "memory.peak"))

	reps := 3
	if v := os.Getenv("ATE_TEST_ENGINE_REPS"); v != "" {
		reps, _ = strconv.Atoi(v)
	}
	for _, n := range []int{1, 4, 16} {
		var forkMS, resultMS []float64
		var failed int
		f.host.mu.Lock()
		f.host.hostMS = nil
		f.host.mu.Unlock()
		for rep := 0; rep < reps; rep++ {
			var wg sync.WaitGroup
			var mu sync.Mutex
			begin := time.Now()
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					id := f.uid("child")
					t0 := time.Now()
					freq := f.forkRequest("team-a", uid, id, 256<<20)
					freq.Spec.Containers[0].WakeupProbe = nil
					if err := f.m.fork(t.Context(), freq, forkTestEpoch); err != nil {
						t.Errorf("fork %s: %v", id, err)
						mu.Lock()
						failed++
						mu.Unlock()
						return
					}
					forked := time.Since(t0)
					code, out, err := f.get(id, "POST", "/scan", body)
					if err != nil || code != 200 || !bytes.Contains(out, []byte(`"results"`)) {
						t.Errorf("scan %s = %d %.100q, %v", id, code, out, err)
						mu.Lock()
						failed++
						mu.Unlock()
						return
					}
					mu.Lock()
					forkMS = append(forkMS, float64(forked.Microseconds())/1000)
					resultMS = append(resultMS, float64(time.Since(begin).Microseconds())/1000)
					mu.Unlock()
					_ = f.m.terminate(t.Context(), id)
				}()
			}
			wg.Wait()
			waitFor(t, "children to be deleted", func() bool { return len(f.forkdChildren()) == 0 })
		}
		sort.Float64s(forkMS)
		sort.Float64s(resultMS)
		if len(forkMS) > 0 {
			f.host.mu.Lock()
			hostMS := append([]float64(nil), f.host.hostMS...)
			f.host.mu.Unlock()
			sort.Float64s(hostMS)
			t.Logf("FULLPATH n=%d children=%d failed=%d host_ms_med=%.1f manager_fork_ms_med=%.1f p95=%.1f fork_to_result_ms_med=%.1f p95=%.1f",
				n, len(forkMS), failed, hostMS[len(hostMS)/2], forkMS[len(forkMS)/2], forkMS[len(forkMS)*95/100], resultMS[len(resultMS)/2], resultMS[len(resultMS)*95/100])
		}
	}
}

// TestRealForkdHoldChildren forks children of a probe guest through the worker
// path and holds them for host-side isolation checks. It writes the host pids
// of the seed and the children to $ATE_TEST_ISO_DIR/beacon, then waits for the
// file $ATE_TEST_ISO_DIR/done. When $ATE_TEST_ISO_GO is set it writes
// seeded and waits for go before it forks, so a tracer can attach first.
// The image is built from the guest of the gVisor fork's isolation probes.
func TestRealForkdHoldChildren(t *testing.T) {
	dir := os.Getenv("ATE_TEST_ISO_DIR")
	image := os.Getenv("ATE_TEST_ISO_IMAGE")
	if dir == "" || image == "" {
		t.Skip("ATE_TEST_ISO_DIR and ATE_TEST_ISO_IMAGE name no probe image and directory")
	}
	n, _ := strconv.Atoi(os.Getenv("ATE_TEST_ISO_N"))
	if n == 0 {
		n = 4
	}
	f := newRealFixture(t)
	f.useImage(image, fileSHA256Must(t, image))
	f.host.mu.Lock()
	f.host.probe = func(ctx context.Context, uid string) error {
		for {
			if out, _ := f.logOf(uid); strings.Contains(out, `"results"`) && strings.HasSuffix(strings.TrimSpace(out), "}") {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}
	}
	f.host.mu.Unlock()
	uid := f.uid("seed")
	// The canary is in the seed's environment, like a secret that a template
	// must not hold: every child can read it.
	req := f.seedRequest("team-a", uid, []string{"/guest"}, []string{"SECRET_CANARY=canary-seed-env"}, 512<<20)
	if err := f.m.seed(t.Context(), req, forkTestEpoch); err != nil {
		t.Fatalf("seed: %v", err)
	}
	seeds := f.forkdSeeds()
	if len(seeds) != 1 {
		t.Fatalf("forkd seeds: %+v", seeds)
	}
	waitFile := func(name string, d time.Duration) {
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("no %s file", name)
	}
	if os.Getenv("ATE_TEST_ISO_GO") != "" {
		if err := os.WriteFile(filepath.Join(dir, "seeded"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		waitFile("go", 120*time.Second)
	}
	var uids []string
	for i := 0; i < n; i++ {
		id := f.uid("child")
		freq := f.forkRequest("team-a", uid, id, 256<<20, "ATE_ACTOR_MODE=ident+procgvisor+net", fmt.Sprintf("ATE_ACTOR_MARK=child-%d", i))
		freq.Spec.Containers[0].WakeupProbe = nil
		if err := f.m.fork(t.Context(), freq, forkTestEpoch); err != nil {
			t.Fatalf("fork %d: %v", i, err)
		}
		uids = append(uids, id)
	}
	line := fmt.Sprintf("template=%d", seeds[0].PID)
	for _, k := range f.forkdChildren() {
		line += fmt.Sprintf(" child=%d", k.PID)
	}
	if err := os.WriteFile(filepath.Join(dir, "beacon"), []byte(line+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for i, id := range uids {
		out, _ := f.logOf(id)
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("child%d.stdout", i)), []byte(out), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	waitFile("done", 300*time.Second)
	for _, id := range uids {
		_ = f.m.terminate(t.Context(), id)
	}
}
