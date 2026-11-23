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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"syscall"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/nodepath"
)

// forkd runs from a directory of the worker's own, not from the assets of the
// seed actor that happens to start it.
//
//   - forkd runs runsc with --directfs=false, so the sandbox process runs as
//     nobody in a new user namespace and execs gvisor-sentry-prewarmer and
//     gvisor_sentry by path. atelet keeps an actor's sandbox-assets
//     directory to itself (0700), so nobody cannot traverse it.
//   - forkd fixes its runsc and its sidecar directory when it starts, and one
//     forkd serves the worker's life. atelet deletes the first seed actor's
//     assets when that actor goes, and every later seed would then find no
//     sentry.
//
// So the worker puts runsc and the gvisor-bin helpers of the release into
// forkdBinRoot, a directory every user may traverse and nobody may write. A
// hard link keeps the inode of the cache file, so nothing is copied and
// deleting the actor's assets does not delete the file.

// forkdBinRoot holds one directory per release, named by the hashes of its
// files, directly below the node's shared folder. It is not under a worker's
// directory, which atelet keeps 0700.
var forkdBinRoot = filepath.Join(nodepath.BasePath, "forkd-bin")

// linkFile is os.Link, which tests replace.
var linkFile = os.Link

const (
	forkdRunscRel  = "runsc"
	forkdSentryRel = "gvisor-bin/gvisor_sentry"
)

// forkdBinaries is what execForkdLauncher starts forkd from.
type forkdBinaries struct {
	// Forkd is the executable.
	Forkd string
	// Runsc, SidecarDir, RunscSHA256 and SentrySHA256 pin forkd's other
	// binaries. Empty leaves forkd to find them next to itself.
	Runsc, SidecarDir         string
	RunscSHA256, SentrySHA256 string
}

// args are the flags that point forkd at its binaries.
func (b forkdBinaries) args() []string {
	var out []string
	for _, f := range []struct{ name, val string }{
		{"forkd-runsc", b.Runsc},
		{"forkd-sidecar-dir", b.SidecarDir},
		{"forkd-expect-runsc-sha256", b.RunscSHA256},
		{"forkd-expect-sentry-sha256", b.SentrySHA256},
	} {
		if f.val != "" {
			out = append(out, "--"+f.name+"="+f.val)
		}
	}
	return out
}

// stagedForkdBins is a release in forkdBinRoot.
type stagedForkdBins struct {
	dir   string
	files map[string]string // path below dir to SHA-256
}

func (s *stagedForkdBins) binaries() forkdBinaries {
	return forkdBinaries{
		Forkd:        filepath.Join(s.dir, forkdBinaryRel),
		Runsc:        filepath.Join(s.dir, forkdRunscRel),
		SidecarDir:   filepath.Join(s.dir, "gvisor-bin"),
		RunscSHA256:  s.files[forkdRunscRel],
		SentrySHA256: s.files[forkdSentryRel],
	}
}

// releaseFiles lists the release in the directory of runscPath: runsc and the
// regular files of its gvisor-bin directory, as paths below that directory.
// A symlink or a directory is not part of the release.
func releaseFiles(runscPath string) ([]string, error) {
	srcDir := filepath.Dir(runscPath)
	files := []string{forkdRunscRel}
	entries, err := os.ReadDir(filepath.Join(srcDir, "gvisor-bin"))
	if err != nil {
		return nil, fmt.Errorf("listing the release helpers: %w", err)
	}
	for _, e := range entries {
		if e.Type().IsRegular() {
			files = append(files, "gvisor-bin/"+e.Name())
		}
	}
	sort.Strings(files)
	for _, want := range []string{forkdRunscRel, forkdBinaryRel, forkdSentryRel} {
		if !slices.Contains(files, want) {
			return nil, fmt.Errorf("the release in %s has no %s", srcDir, want)
		}
	}
	return files, nil
}

// stageForkdBins puts the release of runscPath, which is the path of a seed
// actor's runsc, into root as a directory named by its contents. A release
// already there is reused when its files hash to the same values.
func stageForkdBins(root, runscPath string) (*stagedForkdBins, error) {
	files, err := releaseFiles(runscPath)
	if err != nil {
		return nil, apierror.FailedPrecondition("forkd binaries: %v", err)
	}
	if err := prepareForkdBinRoot(root); err != nil {
		return nil, apierror.FailedPrecondition("forkd binaries: %v", err)
	}
	tmp, err := os.MkdirTemp(root, ".stage-")
	if err != nil {
		return nil, apierror.Internal("forkd binaries: %v", err)
	}
	defer os.RemoveAll(tmp) // nothing is left of it once it is renamed
	if err := os.Chmod(tmp, 0o755); err != nil {
		return nil, apierror.Internal("forkd binaries: %v", err)
	}
	srcDir := filepath.Dir(runscPath)
	if err := os.Mkdir(filepath.Join(tmp, "gvisor-bin"), 0o755); err != nil {
		return nil, apierror.Internal("forkd binaries: %v", err)
	}
	if err := os.Chmod(filepath.Join(tmp, "gvisor-bin"), 0o755); err != nil {
		return nil, apierror.Internal("forkd binaries: %v", err)
	}
	for _, rel := range files {
		if err := linkOrCopy(filepath.Join(srcDir, rel), filepath.Join(tmp, rel)); err != nil {
			return nil, apierror.FailedPrecondition("forkd binaries: %v", err)
		}
	}
	sums, err := hashFiles(tmp, files)
	if err != nil {
		return nil, apierror.Internal("forkd binaries: %v", err)
	}
	final := filepath.Join(root, releaseName(files, sums))
	if err := os.Rename(tmp, final); err != nil {
		// Another worker or an earlier start staged the same release.
		if _, statErr := os.Stat(final); statErr != nil {
			return nil, apierror.Internal("forkd binaries: %v", err)
		}
		have, hashErr := hashFiles(final, files)
		if hashErr != nil || !sameSums(files, have, sums) {
			return nil, apierror.FailedPrecondition("forkd binaries: %s exists and its files do not match the release being staged (%v); remove it", final, hashErr)
		}
	}
	m := make(map[string]string, len(files))
	for i, rel := range files {
		m[rel] = sums[i]
	}
	return &stagedForkdBins{dir: final, files: m}, nil
}

// matches reports whether the release of runscPath is the one staged. forkd
// runs the staged binaries for as long as it lives, so a seed that names
// another release cannot be served by it.
func (s *stagedForkdBins) matches(runscPath string) error {
	files, err := releaseFiles(runscPath)
	if err != nil {
		return apierror.FailedPrecondition("forkd binaries: %v", err)
	}
	srcDir := filepath.Dir(runscPath)
	mismatch := func(why string) error {
		return apierror.FailedPrecondition("forkd binaries: the seed's SandboxConfig names a different release than the one this worker's forkd runs (%s); the worker has to restart to run it", why)
	}
	if len(files) != len(s.files) {
		return mismatch("another set of files")
	}
	for _, rel := range files {
		want, ok := s.files[rel]
		if !ok {
			return mismatch(rel + " is not in it")
		}
		src, dst := filepath.Join(srcDir, rel), filepath.Join(s.dir, rel)
		srcInfo, err1 := os.Stat(src)
		dstInfo, err2 := os.Stat(dst)
		if err1 == nil && err2 == nil && os.SameFile(srcInfo, dstInfo) {
			continue // a hard link: the same file
		}
		got, err := fileSHA256(src)
		if err != nil {
			return apierror.FailedPrecondition("forkd binaries: %v", err)
		}
		if got != want {
			return mismatch(rel + " differs")
		}
	}
	return nil
}

// prepareForkdBinRoot creates root for everyone to traverse and for no one but
// the worker to write, and checks that nothing above it stops a user from
// reaching it.
func prepareForkdBinRoot(root string) error {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	if err := os.Chmod(root, 0o755); err != nil {
		return err
	}
	for dir := root; ; dir = filepath.Dir(dir) {
		fi, err := os.Stat(dir)
		if err != nil {
			return err
		}
		if fi.Mode().Perm()&0o001 == 0 {
			return fmt.Errorf("%s is mode %o: the sandbox process runs as nobody and could not reach %s", dir, fi.Mode().Perm(), root)
		}
		if dir == filepath.Dir(dir) {
			return nil
		}
	}
}

// linkOrCopy puts src at dst. A hard link keeps the file the cache published,
// which is read-only, and it is used only when the file is the worker's own and
// neither the group nor others can write to it, and others can read and execute
// it: nobody can change a file that every actor's release shares. Any other
// file, or a link that fails (another filesystem, say), is copied with mode
// 0755.
func linkOrCopy(src, dst string) error {
	if err := linkFile(src, dst); err == nil {
		fi, statErr := os.Stat(dst)
		if statErr == nil && safeToShare(fi) {
			return nil
		}
		if err := os.Remove(dst); err != nil {
			return err
		}
	}
	return copyFile(src, dst)
}

func safeToShare(fi os.FileInfo) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && int(st.Uid) == os.Getuid() && fi.Mode().Perm()&0o022 == 0 && fi.Mode().Perm()&0o005 == 0o005
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	fi, err := in.Stat()
	if err != nil {
		return err
	}
	mode := os.FileMode(0o644)
	if fi.Mode().Perm()&0o111 != 0 {
		mode = 0o755
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Chmod(dst, mode) // the umask may have cleared bits
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func hashFiles(dir string, files []string) ([]string, error) {
	sums := make([]string, len(files))
	for i, rel := range files {
		sum, err := fileSHA256(filepath.Join(dir, rel))
		if err != nil {
			return nil, err
		}
		sums[i] = sum
	}
	return sums, nil
}

func sameSums(files, a, b []string) bool {
	if len(a) != len(files) || len(b) != len(files) {
		return false
	}
	for i := range files {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// releaseName names a release by the hashes of its files.
func releaseName(files, sums []string) string {
	h := sha256.New()
	for i, rel := range files {
		fmt.Fprintf(h, "%s %s\n", rel, sums[i])
	}
	return "release-" + hex.EncodeToString(h.Sum(nil))[:16]
}
