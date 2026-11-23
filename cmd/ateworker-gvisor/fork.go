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
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/ateworkervalidation"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/forkd"
	"go.ate.dev/substrate/internal/pidfd"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/sizing"
	"go.ate.dev/substrate/internal/wakeupprobe"
	"go.ate.dev/substrate/internal/workerstats"
	"golang.org/x/sys/unix"
)

// A seed is a sandbox that holds the booted template of an ActorTemplate. A
// forked actor is a child sandbox cloned from it. Both live in forkd, a daemon
// of the gVisor fork that ateworker starts. See the forkd section of
// docs/dev/fork-seed.md for the protocol and for what each side owns.

var forkPlatform = pflag.String("fork-platform", "", "Set to kvm on a worker pod that can hold seed sandboxes and fork actors from them (preview gate ForkSeed). The pod needs /dev/kvm. Empty serves no seeds, and the worker reports no fork overhead, so no seed is placed on it.")

// forkOverhead is what a forked actor costs the worker outside its own limits:
// the sentry process and the host state of its sandbox. A child's private
// pages are charged to its cgroup and booked as its limit.
var forkOverhead = &ateletpb.RuntimeOverhead{MemoryBytes: 16 << 20}

const (
	forkdHelloTimeout  = 10 * time.Second
	forkdStartTimeout  = 15 * time.Second
	forkdSeedTimeout   = 2 * time.Minute
	forkdForkTimeout   = 30 * time.Second
	forkdDeleteTimeout = 30 * time.Second
	forkdCgroupName    = "forkd"
	forkdBinaryRel     = "gvisor-bin/forkd"
)

// defaultForkdWaitRetry is how long a child's watcher waits before it asks forkd
// again after a failed wait.
const defaultForkdWaitRetry = time.Second

// forkdLauncher starts forkd and reports when it exits.
type forkdLauncher interface {
	Launch(ctx context.Context, bins forkdBinaries) (*forkd.Client, <-chan struct{}, error)
}

// forkHost is what a forked actor needs from the worker around forkd: its
// network, its ingress and its wakeup probe.
type forkHost interface {
	// Host builds the actor's network namespaces and returns the path forkd
	// reads the child's links from.
	Host(ctx context.Context, actor resources.ActorAttribution, dirs *ateworkerpb.ActorDirs, memoryBytes int64) (netnsPath string, err error)
	// Probe waits until the actor's containers pass their wakeup probes.
	Probe(ctx context.Context, actorUID string, containers []*ateworkerpb.Container) error
	// Activate makes the actor reachable through ingress.
	Activate(actor resources.ActorAttribution) error
	// Unhost removes the actor's ingress and network.
	Unhost(ctx context.Context, actor resources.ActorAttribution) error
}

// forkReporter tells the control plane how a forked actor or a seed ended.
type forkReporter interface {
	ReportExited(ctx context.Context, actor resources.ActorAttribution, exitCode int32) error
	ReportCrashed(ctx context.Context, actor resources.ActorAttribution, message string) error
}

// forkSeed is a seed this worker process holds.
type forkSeed struct {
	attribution resources.ActorAttribution
	atespace    string
	templateUID string
	epoch       int64
	stopWatch   func()
}

// forkChild is a forked actor this worker process hosts.
type forkChild struct {
	attribution resources.ActorAttribution
	seedUID     string
	stopLogs    func()
	// ended closes when forkd reports the child over. exit is set before it.
	ended chan struct{}
	exit  *forkd.ExitInfo
}

// forkManager holds the seeds and forked actors of one worker process.
type forkManager struct {
	launcher forkdLauncher
	host     forkHost
	reporter forkReporter
	// lockActor takes the actor's lifecycle lock, as the lifecycle RPCs do.
	lockActor func(ctx context.Context, uid string) (unlock func(), ok bool)
	// tailLogs copies a child's stdout and stderr files into the actor log
	// until the returned func stops it, after a last read of both.
	tailLogs func(actor resources.ActorAttribution, stdout, stderr string) (stop func())
	// watchPID closes the returned channel when the process exits.
	watchPID func(pid int) (<-chan struct{}, func(), error)
	// shuttingDown is true once the worker drains.
	shuttingDown func() bool
	// waitRetry is the pause before a child's watcher retries a failed wait.
	waitRetry time.Duration

	// binRoot is where forkd's binaries are staged (see forkdBinRoot). Empty
	// runs forkd from the assets of the seed that starts it, which tests with
	// a fake launcher do.
	binRoot string

	mu       sync.Mutex
	client   *forkd.Client
	bins     *stagedForkdBins // what forkd runs, once staged
	atespace string           // the atespace this process serves, once it has a seed
	seeds    map[string]*forkSeed
	children map[string]*forkChild
}

func newForkManager(launcher forkdLauncher, host forkHost) *forkManager {
	return &forkManager{
		launcher: launcher, host: host,
		watchPID:     watchProcess,
		tailLogs:     func(resources.ActorAttribution, string, string) func() { return func() {} },
		shuttingDown: func() bool { return false },
		waitRetry:    defaultForkdWaitRetry,
		seeds:        map[string]*forkSeed{},
		children:     map[string]*forkChild{},
	}
}

// watchProcess closes the channel when the process pid exits.
func watchProcess(pid int) (<-chan struct{}, func(), error) {
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("opening pidfd for %d: %w", pid, err)
	}
	exited, err := pidfd.ExitedFD(fd)
	if err != nil {
		_ = unix.Close(fd)
		return nil, nil, err
	}
	return exited, func() { _ = unix.Close(fd) }, nil
}

// forkdPath is where the SandboxConfig puts forkd, next to runsc.
func forkdPath(runscPath string) string {
	return filepath.Join(filepath.Dir(runscPath), forkdBinaryRel)
}

// forkdBinaries returns the binaries forkd runs, staging them from the seed's
// runsc the first time. Later seeds have to name the same release. The caller
// holds m.mu.
func (m *forkManager) forkdBinaries(runscPath string) (forkdBinaries, error) {
	if m.binRoot == "" {
		return forkdBinaries{Forkd: forkdPath(runscPath)}, nil
	}
	if m.bins != nil {
		return m.bins.binaries(), m.bins.matches(runscPath)
	}
	staged, err := stageForkdBins(m.binRoot, runscPath)
	if err != nil {
		return forkdBinaries{}, err
	}
	m.bins = staged
	return staged.binaries(), nil
}

// ensureForkd starts forkd if this process has not, and checks what it says it
// is. The caller holds m.mu.
func (m *forkManager) ensureForkd(ctx context.Context, runscPath string) (*forkd.Client, error) {
	bins, err := m.forkdBinaries(runscPath)
	if err != nil {
		return nil, err
	}
	if m.client != nil {
		return m.client, nil
	}
	client, exited, err := m.launcher.Launch(ctx, bins)
	if err != nil {
		m.bins = nil // stage again: the directory may be what failed
		return nil, apierror.FailedPrecondition("starting forkd: %v", err)
	}
	helloCtx, cancel := context.WithTimeout(ctx, forkdHelloTimeout)
	defer cancel()
	hello, err := client.Hello(helloCtx)
	if err == nil {
		err = checkHello(hello)
	}
	if err != nil {
		return nil, apierror.FailedPrecondition("forkd is not fit to hold seeds: %v", err)
	}
	slog.InfoContext(ctx, "forkd started", slog.String("version", hello.ForkdVersion),
		slog.String("runsc_sha256", hello.RunscSHA256), slog.String("sentry_sha256", hello.SentrySHA256))
	m.client = client
	go m.watchForkd(exited)
	return client, nil
}

// checkHello refuses a forkd that speaks another protocol, runs another
// platform or failed one of its own startup assertions.
func checkHello(h *forkd.HelloInfo) error {
	if h.ProtocolVersion != forkd.ProtocolVersion {
		return fmt.Errorf("it speaks protocol %d, not %d", h.ProtocolVersion, forkd.ProtocolVersion)
	}
	if h.Platform != "kvm" {
		return fmt.Errorf("its platform is %q, not kvm", h.Platform)
	}
	if len(h.Assertions) == 0 {
		return errors.New("it reports no startup assertions")
	}
	for _, a := range h.Assertions {
		if !a.OK {
			return fmt.Errorf("startup assertion %s failed: %s", a.Name, a.Detail)
		}
	}
	return nil
}

// watchForkd handles forkd exiting. Every seed and child went with it, so each
// seed is reported crashed and the next seed starts a new forkd. The children's
// own watchers see their waits fail.
func (m *forkManager) watchForkd(exited <-chan struct{}) {
	<-exited
	m.mu.Lock()
	m.client = nil
	seeds := m.seeds
	m.seeds = map[string]*forkSeed{}
	m.mu.Unlock()
	if m.shuttingDown() {
		return
	}
	slog.Error("forkd exited; its seeds and forked actors are gone")
	for _, seed := range seeds {
		seed.stopWatch()
		m.report(seed.attribution, func(ctx context.Context) error {
			return m.reporter.ReportCrashed(ctx, seed.attribution, "forkd exited")
		})
	}
}

func (m *forkManager) report(actor resources.ActorAttribution, send func(ctx context.Context) error) {
	if m.reporter == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), crashReportTimeout)
	defer cancel()
	if err := send(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to report a seed or forked actor", slog.String("actorUID", actor.UID), slog.Any("err", err))
	}
}

// seed boots the template of the request. The atespace of a worker process
// is fixed by its first seed: two atespaces in one forkd would share clean
// pages and a cache-timing channel.
func (m *forkManager) seed(ctx context.Context, req *ateworkerpb.SeedWorkloadRequest, epoch int64) error {
	if err := checkRootfs(req); err != nil {
		return err
	}
	ctr := req.GetSpec().GetContainers()[0].GetContainerSpec()
	if len(ctr.GetArgs()) == 0 {
		return apierror.InvalidArgument("spec: the seed has no process to run")
	}

	m.mu.Lock()
	if m.atespace != "" && m.atespace != req.GetAtespace() {
		held := m.atespace
		m.mu.Unlock()
		return apierror.PermissionDenied("this worker holds seeds of atespace %q, so it takes none of atespace %q", held, req.GetAtespace())
	}
	m.atespace = req.GetAtespace()
	if _, ok := m.seeds[req.GetActorUid()]; ok {
		m.mu.Unlock()
		return nil // a retried Seed that already landed
	}
	client, err := m.ensureForkd(ctx, req.GetRunscPath())
	m.mu.Unlock()
	if err != nil {
		return err
	}

	memory := sizing.FromLimits(req.GetResources().GetLimits()).MemoryBytes
	if memory <= 0 {
		return apierror.InvalidArgument("resources: a seed needs a memory limit, which caps its cgroup")
	}
	seedReq := &forkd.SeedRequest{
		SeedID:           req.GetActorUid(),
		Image:            req.GetRootfsImagePath(),
		ImageSHA256:      req.GetRootfsSha256(),
		Args:             ctr.GetArgs(),
		Env:              ctr.GetEnv(),
		Cwd:              ctr.GetCwd(),
		UID:              ctr.GetUser().GetUid(),
		GID:              ctr.GetUser().GetGid(),
		DropCapabilities: true,
		Tmpfs:            []forkd.Tmpfs{{Path: "/tmp", SizeBytes: uint64(max(memory/2, 0))}},
		// The seed holds the pages that its children share, so seed_resources is
		// the cap of the cgroup the seed runs in. The guest sees it as its memory.
		Cgroup:  &forkd.Cgroup{MemoryMax: memory},
		Workers: 1,
		CPUs:    max(sizing.FromLimits(req.GetResources().GetLimits()).VCPUs(), 1),
	}
	seedCtx, cancel := context.WithTimeout(ctx, forkdSeedTimeout)
	defer cancel()
	info, err := client.Seed(seedCtx, seedReq)
	if err != nil {
		return forkdError("seed", err)
	}
	entry := &forkSeed{
		attribution: seedAttribution(req),
		atespace:    req.GetAtespace(),
		templateUID: req.GetTemplateUid(),
		epoch:       epoch,
		stopWatch:   func() {},
	}
	if exited, stop, err := m.watchPID(info.PID); err != nil {
		// Without a watch a dead seed goes unnoticed until a fork fails.
		slog.WarnContext(ctx, "Cannot watch the seed process", slog.String("actorUID", req.GetActorUid()), slog.Any("err", err))
	} else {
		entry.stopWatch = stop
		go m.watchSeed(entry, exited)
	}
	m.mu.Lock()
	m.seeds[req.GetActorUid()] = entry
	m.mu.Unlock()
	return nil
}

func seedAttribution(req *ateworkerpb.SeedWorkloadRequest) resources.ActorAttribution {
	return resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()},
		UID:              req.GetActorUid(),
		TemplateAtespace: req.GetActorTemplateAtespace(),
		TemplateName:     req.GetActorTemplateName(),
	}
}

// checkRootfs holds the image to the seed's own directory and to the hash atelet
// recorded. forkd hashes the file it boots from as well.
func checkRootfs(req *ateworkerpb.SeedWorkloadRequest) error {
	root := req.GetActorDirs().GetRootDir()
	rel, err := filepath.Rel(root, req.GetRootfsImagePath())
	if err != nil || !filepath.IsLocal(rel) {
		return apierror.InvalidArgument("rootfs_image_path %q is not under the seed's directory %q", req.GetRootfsImagePath(), root)
	}
	f, err := os.Open(req.GetRootfsImagePath())
	if err != nil {
		return apierror.FailedPrecondition("opening the seed rootfs image: %v", err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return apierror.FailedPrecondition("hashing the seed rootfs image: %v", err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != req.GetRootfsSha256() {
		return apierror.DataLoss("the seed rootfs image has hash %s, but atelet recorded %s", got, req.GetRootfsSha256())
	}
	return nil
}

// watchSeed reports a seed whose process died on its own. The worker is not
// asked to drop it again, since the control plane deletes the crashed seed
// Actor, which comes back here as DropSeed.
func (m *forkManager) watchSeed(seed *forkSeed, exited <-chan struct{}) {
	<-exited
	if m.shuttingDown() {
		return
	}
	m.mu.Lock()
	cur := m.seeds[seed.attribution.UID]
	if cur != seed {
		m.mu.Unlock()
		return // dropped, or lost with forkd
	}
	delete(m.seeds, seed.attribution.UID)
	m.mu.Unlock()
	slog.Warn("Seed sandbox died", slog.String("actorUID", seed.attribution.UID))
	m.report(seed.attribution, func(ctx context.Context) error {
		return m.reporter.ReportCrashed(ctx, seed.attribution, "seed sandbox exited")
	})
}

// fork starts a forked actor from the seed. The caller holds the actor's lock.
func (m *forkManager) fork(ctx context.Context, req *ateworkerpb.ForkWorkloadRequest, epoch int64) error {
	m.mu.Lock()
	seed, ok := m.seeds[req.GetSeedActorUid()]
	client := m.client
	m.mu.Unlock()
	switch {
	case !ok || client == nil:
		return apierror.NotFound("this worker holds no seed %s", req.GetSeedActorUid())
	case seed.epoch != epoch || req.GetSeedWorkerEpoch() != epoch:
		return apierror.FailedPrecondition("seed %s belongs to worker epoch %d, this process is epoch %d", req.GetSeedActorUid(), seed.epoch, epoch)
	case seed.atespace != req.GetAtespace() || seed.templateUID != req.GetTemplateUid():
		// The seed of another atespace or template is the one thing a forged
		// request would name.
		return apierror.PermissionDenied("seed %s was not built for atespace %s and template %s", req.GetSeedActorUid(), req.GetAtespace(), req.GetTemplateUid())
	}

	actor := resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()},
		UID:              req.GetActorUid(),
		TemplateAtespace: req.GetActorTemplateAtespace(),
		TemplateName:     req.GetActorTemplateName(),
	}
	limits := sizing.FromLimits(req.GetResources().GetLimits())
	netns, err := m.host.Host(ctx, actor, req.GetActorDirs(), limits.MemoryBytes)
	if err != nil {
		return err
	}
	unwind := func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), forkdDeleteTimeout)
		defer cancel()
		_ = client.Delete(cleanup, forkd.KindChild, actor.UID)
		if err := m.host.Unhost(cleanup, actor); err != nil {
			slog.WarnContext(cleanup, "Failed to clean up a forked actor's network", slog.String("actorUID", actor.UID), slog.Any("err", err))
		}
	}

	forkCtx, cancel := context.WithTimeout(ctx, forkdForkTimeout)
	defer cancel()
	child, err := client.Fork(forkCtx, &forkd.ForkRequest{
		SeedID:  seed.attribution.UID,
		ChildID: actor.UID,
		Env:     req.GetEnv(),
		// memory.max kills at the limit. memory.high would only throttle a child
		// that stays above it. The sentry's own memory is what fork_overhead books.
		Cgroup: &forkd.Cgroup{
			CPUMax:    cpuMax(limits.MilliCPU),
			MemoryMax: limits.MemoryBytes + forkOverhead.GetMemoryBytes(),
			PidsMax:   int64(req.GetPidsLimit()),
		},
		Net:           &forkd.Net{NetNSPath: netns},
		MaxLifetimeMS: int64(req.GetMaxLifetimeSeconds()) * 1000,
		LogBytesLimit: req.GetLogBytesLimit(),
	})
	if err != nil {
		unwind()
		return forkdError("fork", err)
	}
	entry := &forkChild{attribution: actor, seedUID: seed.attribution.UID, stopLogs: m.tailLogs(actor, child.StdoutPath, child.StderrPath), ended: make(chan struct{})}
	m.mu.Lock()
	m.children[actor.UID] = entry
	m.mu.Unlock()
	// The child is registered before the probe, so a child that exits during it
	// is torn down by its own watcher and reported once the RPC returns.
	go m.watchChild(entry)

	// A child that ends while the probe polls fails the start at once, rather
	// than at the probe's deadline.
	probeCtx, stopProbe := context.WithCancelCause(ctx)
	defer stopProbe(nil)
	go func() {
		select {
		case <-entry.ended:
			stopProbe(errChildEnded)
		case <-probeCtx.Done():
		}
	}()
	if err := m.host.Probe(probeCtx, actor.UID, req.GetSpec().GetContainers()); err != nil {
		if errors.Is(context.Cause(probeCtx), errChildEnded) {
			err = apierror.FailedPrecondition("the forked sandbox ended before its wakeup probe passed: %s", entry.exit.Reason)
		} else {
			err = fmt.Errorf("while waiting for the wakeup probe: %w", err)
		}
		m.dropChild(ctx, actor.UID)
		return err
	}
	if err := m.host.Activate(actor); err != nil {
		m.dropChild(ctx, actor.UID)
		return err
	}
	return nil
}

var errChildEnded = errors.New("the forked sandbox ended")

// cpuMax is cpu.max for millicores, over a 100 ms period. Zero is no limit.
func cpuMax(millicores int64) string {
	if millicores <= 0 {
		return "max"
	}
	const period = 100000
	return fmt.Sprintf("%d %d", max(millicores*period/1000, 1000), period)
}

// watchChild waits for the child to end, tears it down and reports it.
func (m *forkManager) watchChild(c *forkChild) {
	uid := c.attribution.UID
	var exit *forkd.ExitInfo
	for {
		m.mu.Lock()
		client := m.client
		m.mu.Unlock()
		if client == nil {
			exit = &forkd.ExitInfo{Reason: forkd.ReasonKilled, Detail: "forkd exited"}
			break
		}
		info, err := client.Wait(context.Background(), uid, -1)
		if err == nil {
			exit = info.Exit
			if exit == nil {
				exit = &forkd.ExitInfo{Reason: forkd.ReasonKilled, Detail: "forkd reported no exit"}
			}
			break
		}
		if forkd.IsCode(err, forkd.CodeNotFound) {
			exit = &forkd.ExitInfo{Reason: forkd.ReasonDeleted}
			break
		}
		// forkd is still there, or its exit is about to be seen.
		time.Sleep(m.waitRetry)
	}
	// Before the lock: a fork that holds it is polling the probe and stops at this.
	c.exit = exit
	close(c.ended)

	unlock, ok := m.lockActor(context.Background(), uid)
	if !ok {
		return
	}
	m.mu.Lock()
	ours := m.children[uid] == c
	delete(m.children, uid)
	client := m.client
	m.mu.Unlock()
	if ours {
		c.stopLogs()
		cleanup, cancel := context.WithTimeout(context.Background(), forkdDeleteTimeout)
		if client != nil {
			_ = client.Delete(cleanup, forkd.KindChild, uid)
		}
		if err := m.host.Unhost(cleanup, c.attribution); err != nil {
			slog.WarnContext(cleanup, "Failed to clean up a forked actor's network", slog.String("actorUID", uid), slog.Any("err", err))
		}
		cancel()
	}
	unlock()
	if !ours || m.shuttingDown() {
		return // ended by a Terminate, which the control plane already knows
	}
	switch exit.Reason {
	case forkd.ReasonExited, forkd.ReasonDeleted:
		m.report(c.attribution, func(ctx context.Context) error {
			return m.reporter.ReportExited(ctx, c.attribution, exitCode(exit))
		})
	default:
		message := "forked sandbox ended: " + exit.Reason
		if exit.Detail != "" {
			message += ": " + exit.Detail
		}
		m.report(c.attribution, func(ctx context.Context) error {
			return m.reporter.ReportCrashed(ctx, c.attribution, message)
		})
	}
}

// exitCode is the status the control plane records: the guest's, or 128 plus
// the signal that ended it.
func exitCode(exit *forkd.ExitInfo) int32 {
	switch {
	case exit.ExitCode != nil:
		return int32(*exit.ExitCode & 0xff)
	case exit.Signal > 0:
		return int32(128 + exit.Signal)
	}
	return 0
}

// dropChild removes a child that did not start, or that the control plane
// terminates.
func (m *forkManager) dropChild(ctx context.Context, uid string) {
	m.mu.Lock()
	c := m.children[uid]
	delete(m.children, uid)
	client := m.client
	m.mu.Unlock()
	if c == nil {
		return
	}
	c.stopLogs()
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), forkdDeleteTimeout)
	defer cancel()
	if client != nil {
		_ = client.Delete(cleanup, forkd.KindChild, uid)
	}
	if err := m.host.Unhost(cleanup, c.attribution); err != nil {
		slog.WarnContext(cleanup, "Failed to clean up a forked actor's network", slog.String("actorUID", uid), slog.Any("err", err))
	}
}

// dropSeed discards a seed and the forked actors that came from it.
func (m *forkManager) dropSeed(ctx context.Context, uid string) error {
	m.mu.Lock()
	seed := m.seeds[uid]
	delete(m.seeds, uid)
	client := m.client
	m.mu.Unlock()
	if seed != nil {
		seed.stopWatch()
	}
	if client == nil {
		return nil
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), forkdDeleteTimeout)
	defer cancel()
	if err := client.Delete(cleanup, forkd.KindSeed, uid); err != nil {
		return fmt.Errorf("while deleting the seed from forkd: %w", err)
	}
	return nil
}

// owns reports whether uid is a seed or forked actor of this process.
func (m *forkManager) owns(uid string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, seed := m.seeds[uid]
	_, child := m.children[uid]
	return seed || child
}

// terminate ends a seed or a forked actor for TerminateWorkload.
func (m *forkManager) terminate(ctx context.Context, uid string) error {
	m.mu.Lock()
	_, seed := m.seeds[uid]
	m.mu.Unlock()
	if seed {
		return m.dropSeed(ctx, uid)
	}
	m.dropChild(ctx, uid)
	return nil
}

// forkdError maps a forkd error to the status the RPC answers with.
func forkdError(op string, err error) error {
	var fe *forkd.Error
	if !errors.As(err, &fe) {
		return apierror.Unavailable("forkd %s: %v", op, err)
	}
	switch fe.Code {
	case forkd.CodeInvalidArgument:
		return apierror.InvalidArgument("forkd %s: %s", op, fe.Message)
	case forkd.CodeNotFound:
		return apierror.NotFound("forkd %s: %s", op, fe.Message)
	case forkd.CodeAlreadyExists:
		return apierror.AlreadyExists("forkd %s: %s", op, fe.Message)
	case forkd.CodePermissionDenied:
		return apierror.PermissionDenied("forkd %s: %s", op, fe.Message)
	case forkd.CodeResourceExhausted:
		return apierror.ResourceExhausted("forkd %s: %s", op, fe.Message)
	case forkd.CodeFailedPrecondition:
		return apierror.FailedPrecondition("forkd %s: %s", op, fe.Message)
	case forkd.CodeUnavailable:
		return apierror.Unavailable("forkd %s: %s", op, fe.Message)
	}
	return apierror.Internal("forkd %s: %s", op, fe.Message)
}

// execForkdLauncher starts forkd as a child process.
type execForkdLauncher struct {
	stateDir     string
	cgroupParent string
	uid          int
}

// args are the flags forkd starts with. forkd takes the cgroup parent as an
// absolute path below /sys/fs/cgroup.
func (l execForkdLauncher) args() []string {
	return []string{
		"--forkd-state-dir=" + l.stateDir,
		"--forkd-cgroup-parent=" + l.cgroupParent,
		"--forkd-env-prefixes=ATE_ACTOR_",
		fmt.Sprintf("--forkd-allow-uids=%d", l.uid),
	}
}

func (l execForkdLauncher) Launch(ctx context.Context, bins forkdBinaries) (*forkd.Client, <-chan struct{}, error) {
	binary := bins.Forkd
	if _, err := os.Stat(binary); err != nil {
		return nil, nil, fmt.Errorf("the SandboxConfig has no forkd: %w", err)
	}
	if err := os.MkdirAll(l.stateDir, 0o700); err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(l.cgroupParent, 0o755); err != nil {
		return nil, nil, fmt.Errorf("creating the forkd cgroup: %w", err)
	}
	for _, c := range []string{"memory", "pids", "cpu"} {
		if err := os.WriteFile(filepath.Join(l.cgroupParent, "cgroup.subtree_control"), []byte("+"+c), 0o644); err != nil {
			slog.WarnContext(ctx, "could not delegate a cgroup controller to forkd", slog.String("controller", c), slog.Any("err", err))
		}
	}
	cmd := exec.Command(binary, append(l.args(), bins.args()...)...)
	cmd.Env = []string{}
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("starting %s: %w", binary, err)
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	client := forkd.NewClient(filepath.Join(l.stateDir, "forkd.sock"))
	deadline := time.Now().Add(forkdStartTimeout)
	for {
		probe, cancel := context.WithTimeout(ctx, time.Second)
		_, err := client.Hello(probe)
		cancel()
		if err == nil {
			return client, exited, nil
		}
		select {
		case <-exited:
			return nil, nil, errors.New("forkd exited at startup")
		case <-ctx.Done():
			_ = cmd.Process.Kill()
			return nil, nil, ctx.Err()
		default:
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			return nil, nil, fmt.Errorf("forkd did not answer within %v: %w", forkdStartTimeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// serviceForkHost is the worker's own network, ingress and probe, for the fork
// manager.
type serviceForkHost struct{ s *AteWorkerService }

func (h serviceForkHost) Host(ctx context.Context, actor resources.ActorAttribution, dirs *ateworkerpb.ActorDirs, memoryBytes int64) (string, error) {
	if err := h.s.tunnel.Deactivate(ctx, actor); err != nil {
		return "", err
	}
	// A forked actor has no egress: its TCP is captured and refused.
	hosted, err := h.s.hostActor(ctx, actor, dirs, memoryBytes)
	if err != nil {
		return "", err
	}
	if err := h.s.tunnel.ActivateEgress(actor, nil); err != nil {
		_ = h.s.unhostActor(ctx, actor.UID)
		return "", err
	}
	// forkd reads the namespace through the worker's own descriptor.
	return fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), int(hosted.network.Network.RuntimeNetNS)), nil
}

func (h serviceForkHost) Probe(ctx context.Context, actorUID string, containers []*ateworkerpb.Container) error {
	return wakeupprobe.WaitAll(ctx, containers, actornet.ActorVethIP, wakeupprobe.DialFunc(h.s.sandboxDialer(actorUID)))
}

func (h serviceForkHost) Activate(actor resources.ActorAttribution) error {
	return h.s.tunnel.ActivateIngress(actor, ingress.DialFunc(h.s.sandboxDialer(actor.UID)))
}

func (h serviceForkHost) Unhost(ctx context.Context, actor resources.ActorAttribution) error {
	err := h.s.tunnel.Deactivate(ctx, actor)
	return errors.Join(err, h.s.unhostActor(ctx, actor.UID))
}

// fileTail copies a file that something else appends to into r, polling for
// more until stopped, and then reading what is left.
type fileTail struct {
	f    *os.File
	stop chan struct{}
}

func (t *fileTail) Read(p []byte) (int, error) {
	for {
		n, err := t.f.Read(p)
		if n > 0 || (err != nil && err != io.EOF) {
			return n, err
		}
		select {
		case <-t.stop:
			// One last read, in case the final bytes arrived with the stop.
			n, err := t.f.Read(p)
			if n > 0 {
				return n, nil
			}
			if err == nil {
				err = io.EOF
			}
			return 0, err
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// tailActorLogs sends a child's stdout and stderr files to the actor log.
func (s *AteWorkerService) tailActorLogs(actor resources.ActorAttribution, stdout, stderr string) func() {
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for name, path := range map[string]string{"stdout": stdout, "stderr": stderr} {
		f, err := os.Open(path)
		if err != nil {
			slog.Warn("Cannot tail a forked actor's output", slog.String("actorUID", actor.UID), slog.String("file", name), slog.Any("err", err))
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer f.Close()
			s.actorLogger.WrapContainerLogs(bufio.NewReader(&fileTail{f: f, stop: stop}), actor, name)
		}()
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			wg.Wait()
		})
	}
}

// SeedWorkload boots the seed sandbox of a template from the root filesystem
// image atelet built.
func (s *AteWorkerService) SeedWorkload(ctx context.Context, req *ateworkerpb.SeedWorkloadRequest) (*ateworkerpb.SeedWorkloadResponse, error) {
	if err := s.requireFork(); err != nil {
		return nil, err
	}
	if err := ateworkervalidation.ValidateSeedWorkloadRequest(ctx, req); err != nil {
		return nil, err
	}
	epoch, err := s.epoch.Require()
	if err != nil {
		return nil, err
	}
	if err := validateRunscPath(req.GetActorUid(), req.GetActorDirs(), req.GetRunscPath()); err != nil {
		return nil, err
	}
	if len(req.GetSpec().GetContainers()) != 1 {
		return nil, apierror.InvalidArgument("spec: a seed has exactly one container, got %d", len(req.GetSpec().GetContainers()))
	}
	if !s.locks.Lock(ctx, req.GetActorUid()) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(req.GetActorUid())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	release, err := s.beginRPC(req.GetActorUid(), "SeedWorkload", cancel)
	if err != nil {
		return nil, err
	}
	defer release()

	if err := s.fork.seed(ctx, req, epoch); err != nil {
		return nil, err
	}
	return &ateworkerpb.SeedWorkloadResponse{WorkerEpoch: epoch}, nil
}

// ForkWorkload starts a forked actor from a seed.
func (s *AteWorkerService) ForkWorkload(ctx context.Context, req *ateworkerpb.ForkWorkloadRequest) (*ateworkerpb.ForkWorkloadResponse, error) {
	if err := s.requireFork(); err != nil {
		return nil, err
	}
	if err := ateworkervalidation.ValidateForkWorkloadRequest(ctx, req); err != nil {
		return nil, err
	}
	epoch, err := s.epoch.Require()
	if err != nil {
		return nil, err
	}
	if len(req.GetSpec().GetContainers()) != 1 {
		return nil, apierror.InvalidArgument("spec: a forked actor has exactly one container, got %d", len(req.GetSpec().GetContainers()))
	}
	if !s.locks.Lock(ctx, req.GetActorUid()) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(req.GetActorUid())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	release, err := s.beginRPC(req.GetActorUid(), "ForkWorkload", cancel)
	if err != nil {
		return nil, err
	}
	defer release()

	if err := s.fork.fork(ctx, req, epoch); err != nil {
		return nil, err
	}
	s.actorLogger.EmitLifecycleLog(ctx, "Actor started", workerstats.ActorAttributionFromRequest(req))
	return &ateworkerpb.ForkWorkloadResponse{WorkerEpoch: epoch}, nil
}

// DropSeed discards a seed. Idempotent.
func (s *AteWorkerService) DropSeed(ctx context.Context, req *ateworkerpb.DropSeedRequest) (*ateworkerpb.DropSeedResponse, error) {
	if err := s.requireFork(); err != nil {
		return nil, err
	}
	if err := ateworkervalidation.ValidateDropSeedRequest(ctx, req); err != nil {
		return nil, err
	}
	if !s.locks.Lock(ctx, req.GetActorUid()) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(req.GetActorUid())
	if err := s.fork.dropSeed(ctx, req.GetActorUid()); err != nil {
		return nil, err
	}
	return &ateworkerpb.DropSeedResponse{}, nil
}

// requireFork refuses seed and fork calls on a worker that was not started to
// serve them, and while the ForkSeed gate is off.
func (s *AteWorkerService) requireFork() error {
	if s.fork == nil {
		return apierror.FailedPrecondition("this worker holds no seeds: it was not started with --fork-platform=kvm")
	}
	if !preview.IsEnabled(preview.GateForkSeed) {
		return apierror.FailedPrecondition("seeds need the ForkSeed preview gate on the worker")
	}
	return nil
}
