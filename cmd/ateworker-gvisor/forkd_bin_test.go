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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"go.ate.dev/substrate/internal/apierror"
	"google.golang.org/grpc/codes"
)

// release is a gVisor release tree as atelet stages it for an actor.
var testRelease = map[string]string{
	"runsc":                              "runsc v1",
	"gvisor-bin/forkd":                   "forkd v1",
	"gvisor-bin/gvisor_sentry":           "sentry v1",
	"gvisor-bin/gvisor-sentry-prewarmer": "prewarmer v1",
	"gvisor-bin/LICENSE":                 "license",
}

func sha(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// writeRelease writes files, with mode 0555 like the cache publishes them, under
// dir/gvisor and returns the path of its runsc. The directories are the owner's
// alone, as atelet makes them.
func writeRelease(t *testing.T, dir string, files map[string]string) string {
	t.Helper()
	root := filepath.Join(dir, "sandbox-assets", "gvisor")
	for rel, content := range files {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, 0o555); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(filepath.Join(dir, "sandbox-assets"), 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, "runsc")
}

// binRoot returns a root below a temporary directory, the temporary directories
// above it made traversable as /var/lib/ate is.
func binRoot(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for d := dir; d != "/tmp" && d != filepath.Dir(d); d = filepath.Dir(d) {
		if err := os.Chmod(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(dir, "forkd-bin")
}

func fileMode(t *testing.T, p string) fs.FileMode {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Mode().Perm()
}

func TestStageForkdBinsLayout(t *testing.T) {
	assets := t.TempDir()
	runsc := writeRelease(t, assets, testRelease)
	root := binRoot(t)
	staged, err := stageForkdBins(root, runsc)
	if err != nil {
		t.Fatal(err)
	}
	b := staged.binaries()
	if filepath.Dir(staged.dir) != root || !strings.HasPrefix(filepath.Base(staged.dir), "release-") {
		t.Errorf("release dir = %s, want a release-<hash> directory in %s", staged.dir, root)
	}
	if want := filepath.Join(staged.dir, "gvisor-bin", "forkd"); b.Forkd != want {
		t.Errorf("forkd = %s, want %s", b.Forkd, want)
	}
	wantArgs := []string{
		"--forkd-runsc=" + filepath.Join(staged.dir, "runsc"),
		"--forkd-sidecar-dir=" + filepath.Join(staged.dir, "gvisor-bin"),
		"--forkd-expect-runsc-sha256=" + sha("runsc v1"),
		"--forkd-expect-sentry-sha256=" + sha("sentry v1"),
	}
	if got := b.args(); !slices.Equal(got, wantArgs) {
		t.Errorf("args = %v, want %v", got, wantArgs)
	}
	if got := (forkdBinaries{Forkd: "/x/forkd"}).args(); len(got) != 0 {
		t.Errorf("a forkdBinaries with only forkd has args %v, want none", got)
	}

	for rel, content := range testRelease {
		p := filepath.Join(staged.dir, rel)
		got, err := os.ReadFile(p)
		if err != nil || string(got) != content {
			t.Errorf("%s = %q, %v; want %q", rel, got, err, content)
		}
		src, _ := os.Stat(filepath.Join(filepath.Dir(runsc), rel))
		dst, _ := os.Stat(p)
		if !os.SameFile(src, dst) {
			t.Errorf("%s is a copy, want a hard link of the cache file", rel)
		}
		// Others must run it and nobody may change it.
		if m := fileMode(t, p); m&0o005 != 0o005 || m&0o022 != 0 {
			t.Errorf("%s has mode %o, want others able to read and execute and no one else to write", rel, m)
		}
	}
	// Everything above and in the release is traversable and not writable by
	// nobody: the root and the release directory, and the helpers directory.
	for _, d := range []string{root, staged.dir, filepath.Join(staged.dir, "gvisor-bin")} {
		if m := fileMode(t, d); m != 0o755 {
			t.Errorf("%s has mode %o, want 0755", d, m)
		}
	}
	// Staging leaves no scratch directory.
	entries, _ := os.ReadDir(root)
	if len(entries) != 1 {
		t.Errorf("%s holds %d entries, want only the release", root, len(entries))
	}

	// The property the worker needs: atelet deletes the actor's assets and
	// forkd keeps its binaries.
	if err := os.RemoveAll(assets); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(b.Forkd); err != nil {
		t.Errorf("forkd is gone with the actor's assets: %v", err)
	}
	if _, err := os.Stat(filepath.Join(b.SidecarDir, "gvisor-sentry-prewarmer")); err != nil {
		t.Errorf("the prewarmer is gone with the actor's assets: %v", err)
	}
}

func TestStageForkdBinsIsIdempotent(t *testing.T) {
	root := binRoot(t)
	first, err := stageForkdBins(root, writeRelease(t, t.TempDir(), testRelease))
	if err != nil {
		t.Fatal(err)
	}
	// Another seed actor, with the same release in its own assets.
	second, err := stageForkdBins(root, writeRelease(t, t.TempDir(), testRelease))
	if err != nil {
		t.Fatal(err)
	}
	if first.dir != second.dir {
		t.Errorf("the same release was staged at %s and %s", first.dir, second.dir)
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 1 {
		t.Errorf("%s holds %d entries, want 1", root, len(entries))
	}
}

func TestStageForkdBinsRefusesAChangedStagedRelease(t *testing.T) {
	root := binRoot(t)
	runsc := writeRelease(t, t.TempDir(), testRelease)
	staged, err := stageForkdBins(root, runsc)
	if err != nil {
		t.Fatal(err)
	}
	// Someone replaced the sentry in the staged directory.
	sentry := filepath.Join(staged.dir, forkdSentryRel)
	if err := os.Remove(sentry); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sentry, []byte("evil"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err = stageForkdBins(root, runsc)
	if apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stage over a changed release = %v, want FailedPrecondition", err)
	}
}

func TestStagedForkdBinsMatches(t *testing.T) {
	root := binRoot(t)
	runsc := writeRelease(t, t.TempDir(), testRelease)
	staged, err := stageForkdBins(root, runsc)
	if err != nil {
		t.Fatal(err)
	}
	if err := staged.matches(runsc); err != nil {
		t.Errorf("the staged release does not match itself: %v", err)
	}
	if err := staged.matches(writeRelease(t, t.TempDir(), testRelease)); err != nil {
		t.Errorf("the same release in another actor's assets does not match: %v", err)
	}
	other := cloneFiles(testRelease)
	other["gvisor-bin/gvisor_sentry"] = "sentry v2"
	if err := staged.matches(writeRelease(t, t.TempDir(), other)); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("another sentry matches: %v, want FailedPrecondition", err)
	}
	fewer := cloneFiles(testRelease)
	delete(fewer, "gvisor-bin/LICENSE")
	if err := staged.matches(writeRelease(t, t.TempDir(), fewer)); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("a release with fewer files matches: %v, want FailedPrecondition", err)
	}
	// A different release stages next to the first, and forkd keeps the first.
	staged2, err := stageForkdBins(root, writeRelease(t, t.TempDir(), other))
	if err != nil {
		t.Fatal(err)
	}
	if staged2.dir == staged.dir {
		t.Error("two releases share a directory")
	}
	if got := staged2.binaries().SentrySHA256; got != sha("sentry v2") {
		t.Errorf("sentry sha = %s, want the new one", got)
	}
}

func cloneFiles(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// A file that others cannot execute, or that the group can write, is copied and
// the cache's file keeps its mode: a hard link shares the mode with the cache.
func TestStageForkdBinsCopiesWhatCannotBeShared(t *testing.T) {
	assets := t.TempDir()
	runsc := writeRelease(t, assets, testRelease)
	srcDir := filepath.Dir(runsc)
	if err := os.Chmod(filepath.Join(srcDir, "runsc"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(srcDir, "gvisor-bin", "forkd"), 0o775); err != nil {
		t.Fatal(err)
	}
	staged, err := stageForkdBins(binRoot(t), runsc)
	if err != nil {
		t.Fatal(err)
	}
	for rel, wantSrcMode := range map[string]fs.FileMode{"runsc": 0o700, "gvisor-bin/forkd": 0o775} {
		dst := filepath.Join(staged.dir, rel)
		if m := fileMode(t, dst); m != 0o755 {
			t.Errorf("%s has mode %o, want the copy at 0755", rel, m)
		}
		srcInfo, _ := os.Stat(filepath.Join(srcDir, rel))
		dstInfo, _ := os.Stat(dst)
		if os.SameFile(srcInfo, dstInfo) {
			t.Errorf("%s was linked", rel)
		}
		if m := fileMode(t, filepath.Join(srcDir, rel)); m != wantSrcMode {
			t.Errorf("the source %s has mode %o, want it unchanged at %o", rel, m, wantSrcMode)
		}
	}
	// The files that could be linked were.
	srcInfo, _ := os.Stat(filepath.Join(srcDir, forkdSentryRel))
	dstInfo, _ := os.Stat(filepath.Join(staged.dir, forkdSentryRel))
	if !os.SameFile(srcInfo, dstInfo) {
		t.Error("the sentry was copied, want a hard link")
	}
	// A copy still matches the release it came from.
	if err := staged.matches(runsc); err != nil {
		t.Errorf("a copy does not match its release: %v", err)
	}
	other := cloneFiles(testRelease)
	other["runsc"] = "runsc v2"
	if err := staged.matches(writeRelease(t, t.TempDir(), other)); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("another runsc matches a copy: %v", err)
	}
}

// A link that fails, as it does across filesystems, falls back to a copy.
func TestLinkOrCopyFallsBackWhenLinkFails(t *testing.T) {
	dir := t.TempDir()
	src, dst := filepath.Join(dir, "src"), filepath.Join(dir, "dst")
	if err := os.WriteFile(src, []byte("bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	old := linkFile
	linkFile = func(string, string) error { return &os.LinkError{Op: "link", Err: syscall.EXDEV} }
	defer func() { linkFile = old }()
	if err := linkOrCopy(src, dst); err != nil {
		t.Fatal(err)
	}
	srcInfo, _ := os.Stat(src)
	dstInfo, _ := os.Stat(dst)
	if os.SameFile(srcInfo, dstInfo) {
		t.Error("dst is a link although linking fails")
	}
	if got, _ := os.ReadFile(dst); string(got) != "bin" || fileMode(t, dst) != 0o755 {
		t.Errorf("dst = %q mode %o, want the content at 0755", got, fileMode(t, dst))
	}
}

func TestStageForkdBinsNeedsTheWholeRelease(t *testing.T) {
	for _, missing := range []string{"runsc", "gvisor-bin/forkd", "gvisor-bin/gvisor_sentry"} {
		t.Run(missing, func(t *testing.T) {
			files := cloneFiles(testRelease)
			delete(files, missing)
			_, err := stageForkdBins(binRoot(t), writeRelease(t, t.TempDir(), files))
			if apierror.Code(err) != codes.FailedPrecondition {
				t.Errorf("stage without %s = %v, want FailedPrecondition", missing, err)
			}
		})
	}
}

// A symlink in gvisor-bin is not part of the release: it could point out of it.
func TestStageForkdBinsSkipsSymlinks(t *testing.T) {
	runsc := writeRelease(t, t.TempDir(), testRelease)
	if err := os.Symlink("/etc/passwd", filepath.Join(filepath.Dir(runsc), "gvisor-bin", "evil")); err != nil {
		t.Fatal(err)
	}
	staged, err := stageForkdBins(binRoot(t), runsc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(staged.dir, "gvisor-bin", "evil")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the symlink was staged: %v", err)
	}
}

// nobody has to be able to reach the release, so a directory above the root
// that others cannot enter fails the staging and names that directory.
func TestStageForkdBinsNeedsATraversablePath(t *testing.T) {
	base := t.TempDir()
	if err := os.Chmod(base, 0o755); err != nil {
		t.Fatal(err)
	}
	closed := filepath.Join(base, "closed")
	if err := os.Mkdir(closed, 0o750); err != nil {
		t.Fatal(err)
	}
	_, err := stageForkdBins(filepath.Join(closed, "forkd-bin"), writeRelease(t, t.TempDir(), testRelease))
	if apierror.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), closed) {
		t.Errorf("stage below a closed directory = %v, want FailedPrecondition naming %s", err, closed)
	}
}

// The manager stages once, starts forkd from the staged binaries with their
// hashes, serves a second seed after the first actor's assets are gone, and
// refuses a seed of another release.
func TestForkManagerRunsForkdFromTheWorkersBinaries(t *testing.T) {
	f := newForkFixture(t)
	f.m.binRoot = binRoot(t)
	assets := t.TempDir()
	runsc := writeRelease(t, assets, testRelease)
	req := f.seedRequest()
	req.RunscPath = runsc
	if err := f.m.seed(t.Context(), req, forkTestEpoch); err != nil {
		t.Fatalf("seed() = %v", err)
	}
	if len(f.launcher.bins) != 1 {
		t.Fatalf("forkd was started %d times, want once", len(f.launcher.bins))
	}
	bins := f.launcher.bins[0]
	if !strings.HasPrefix(bins.Forkd, f.m.binRoot+"/") || bins.RunscSHA256 != sha("runsc v1") || bins.SentrySHA256 != sha("sentry v1") {
		t.Errorf("forkd started from %+v, want the staged binaries below %s with the release hashes", bins, f.m.binRoot)
	}

	// The first actor's assets go; a seed of another actor with the same
	// release is served by the running forkd.
	if err := os.RemoveAll(assets); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(bins.Forkd); err != nil {
		t.Fatalf("forkd's binary went with the actor's assets: %v", err)
	}
	req2 := f.seedRequest()
	req2.ActorUid = "99999999-9999-4999-8999-999999999999"
	req2.RunscPath = writeRelease(t, t.TempDir(), testRelease)
	if err := f.m.seed(t.Context(), req2, forkTestEpoch); err != nil {
		t.Fatalf("second seed() = %v", err)
	}
	if f.launcher.launches != 1 {
		t.Errorf("forkd was started %d times, want once", f.launcher.launches)
	}

	// A third seed names another release: the running forkd cannot serve it.
	other := cloneFiles(testRelease)
	other["gvisor-bin/gvisor_sentry"] = "sentry v2"
	req3 := f.seedRequest()
	req3.ActorUid = "88888888-8888-4888-8888-888888888888"
	req3.RunscPath = writeRelease(t, t.TempDir(), other)
	if err := f.m.seed(t.Context(), req3, forkTestEpoch); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("seed of another release = %v, want FailedPrecondition", err)
	}
	if got := len(f.fake.SeedRequests()); got != 2 {
		t.Errorf("forkd got %d seed requests, want 2", got)
	}
}
