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
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/imagecache"
)

// A seed sandbox reads its root filesystem from one read-only EROFS image
// that atelet builds from the template's digest-pinned image. The sandbox
// parses the image itself, with no gofer in between, so an image that someone
// else influenced would be input to a sandbox that has no seccomp filter.
// atelet builds the image from layers it unpacked and hashed, keeps it in the
// content-addressed cache, and checks its hash before every seed.

const (
	// erofsRecipe names how the image is built. A change to the build makes a
	// new cache key, since the key must name immutable content.
	erofsRecipe = "erofs-v2"

	rootfsImageName = "rootfs.erofs"
	rootfsHashName  = "rootfs.erofs.sha256"

	// erofsUUID is the filesystem UUID of every image. With a fixed timestamp
	// it makes the build reproducible: the same layers give the same bytes.
	erofsUUID = "00000000-0000-0000-0000-000000000001"
)

// erofsBuilder writes the merged filesystem of img as an EROFS image at out.
type erofsBuilder interface {
	Build(ctx context.Context, img *imagecache.Image, out string) error
}

// mkfsErofs builds the image with mkfs.erofs, which reads the merged
// filesystem as a tar stream. Timestamps and the UUID are fixed so the image
// is reproducible. The sentry cannot map a regular file whose tail is packed
// inline, so inline data is off: without it a sandbox fails to load its
// executables with EFAULT.
type mkfsErofs struct {
	binary string
}

func (m mkfsErofs) Build(ctx context.Context, img *imagecache.Image, out string) error {
	pr, pw := io.Pipe()
	cmd := exec.CommandContext(ctx, m.binary, "--tar=f", "-E", "noinline_data", "-T", "0", "-U", erofsUUID, "--quiet", out)
	cmd.Stdin = pr
	var stderr strings.Builder
	cmd.Stderr = &stderr
	go func() { pw.CloseWithError(img.WriteTar(pw)) }()
	if err := cmd.Run(); err != nil {
		pr.CloseWithError(err)
		return fmt.Errorf("while running %s: %w: %s", m.binary, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// ensureSeedRootfs puts the template image's EROFS image in the seed's actor
// directory, building it into the cache when the cache has none, and returns
// its path and content hash. The cached copy's hash is checked here on every
// call, so a file that changed on disk after it was built does not boot a seed.
func (s *Atelet) ensureSeedRootfs(ctx context.Context, actorUID string, img *imagecache.Image) (path, sha string, err error) {
	if s.erofs == nil {
		return "", "", apierror.FailedPrecondition("atelet has no mkfs.erofs to build a seed image with")
	}
	key := filecache.URIKey("seed-rootfs", erofsRecipe, img.Digest.String())
	dst := ateletpath.SeedRootfsDir(actorUID)
	if err := os.RemoveAll(dst); err != nil {
		return "", "", wrapFileSystemErr("while removing a stale seed rootfs", err)
	}
	if err := s.sandboxAssetCache.GetDirTo(ctx, key, dst, func(ctx context.Context, dir string) error {
		return buildRootfsEntry(ctx, s.erofs, img, dir)
	}); err != nil {
		return "", "", fmt.Errorf("while building the seed rootfs image: %w", err)
	}
	path = filepath.Join(dst, rootfsImageName)
	want, err := os.ReadFile(filepath.Join(dst, rootfsHashName))
	if err != nil {
		return "", "", wrapFileSystemErr("while reading the recorded rootfs hash", err)
	}
	got, err := fileSHA256(path)
	if err != nil {
		return "", "", wrapFileSystemErr("while hashing the seed rootfs image", err)
	}
	if got != strings.TrimSpace(string(want)) {
		return "", "", apierror.DataLoss("the cached seed rootfs image for %s has hash %s, but %s was recorded when it was built", img.Digest, got, strings.TrimSpace(string(want)))
	}
	return path, got, nil
}

// buildRootfsEntry builds one cache entry: the image and its hash.
func buildRootfsEntry(ctx context.Context, builder erofsBuilder, img *imagecache.Image, dir string) error {
	out := filepath.Join(dir, rootfsImageName)
	if err := builder.Build(ctx, img, out); err != nil {
		return err
	}
	sum, err := fileSHA256(out)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, rootfsHashName), []byte(sum+"\n"), 0o644)
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
