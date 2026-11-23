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

package imagecache

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// mergedEntry is one path of the merged filesystem and the layer that supplies it.
type mergedEntry struct {
	layerDir string
	info     fs.FileInfo
}

// WriteTar writes the merged filesystem of the image as a tar stream: every
// layer applied bottom first, with the whiteouts and opaque directories each
// layer recorded when it was unpacked. The stream is deterministic: entries
// are in path order and nothing depends on the host.
//
// A regular file, directory, symlink, device or fifo keeps its mode, numeric
// owner and modification time. Hard links come out as separate files, and
// extended attributes, file capabilities included, are not written.
func (img *Image) WriteTar(w io.Writer) error {
	merged := map[string]mergedEntry{}
	for _, layerDir := range img.LayerDirs {
		if err := applyLayer(merged, layerDir); err != nil {
			return fmt.Errorf("while merging layer %s: %w", layerDir, err)
		}
	}
	paths := make([]string, 0, len(merged))
	for p := range merged {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	tw := tar.NewWriter(w)
	for _, p := range paths {
		if err := writeTarEntry(tw, p, merged[p]); err != nil {
			return fmt.Errorf("while writing %q: %w", p, err)
		}
	}
	return tw.Close()
}

// applyLayer applies one layer to merged. The layer's whiteouts and opaque
// directories hide what lower layers hold, and then its own tree lands on top.
func applyLayer(merged map[string]mergedEntry, layerDir string) error {
	wh, err := readWhiteouts(layerDir)
	if err != nil {
		return err
	}
	for _, p := range wh.Whiteouts {
		name, skip, err := validateTarName(p)
		if err != nil {
			return err
		}
		if !skip {
			removeTree(merged, name, true)
		}
	}
	for _, p := range wh.Opaques {
		name, skip, err := validateTarName(p)
		if err != nil {
			return err
		}
		if !skip {
			removeTree(merged, name, false)
		}
	}
	implicit := map[string]bool{}
	for _, p := range wh.ImplicitDirs {
		if name, skip, err := validateTarName(p); err == nil && !skip {
			implicit[name] = true
		}
	}

	root := filepath.Join(layerDir, layerFSDirName)
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if prev, ok := merged[rel]; ok {
			switch {
			case info.IsDir() && prev.info.IsDir() && implicit[rel]:
				// The layer only made the directory because a child needed
				// it; the attributes below are the real ones.
				return nil
			case !info.IsDir():
				removeTree(merged, rel, false)
			}
		}
		merged[rel] = mergedEntry{layerDir: layerDir, info: info}
		return nil
	})
}

// removeTree drops what lower layers hold at name's descendants, and at name
// itself when self is set.
func removeTree(merged map[string]mergedEntry, name string, self bool) {
	prefix := name + "/"
	for p := range merged {
		if strings.HasPrefix(p, prefix) || (self && p == name) {
			delete(merged, p)
		}
	}
}

func writeTarEntry(tw *tar.Writer, name string, e mergedEntry) error {
	info := e.info
	hdr, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return err
	}
	hdr.Name = name
	hdr.Format = tar.FormatPAX
	// Reading a file moves its access time, and nothing here needs either.
	hdr.AccessTime, hdr.ChangeTime = time.Time{}, time.Time{}
	// No user or group names: the numeric owner is what the image recorded.
	hdr.Uname, hdr.Gname = "", ""
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		hdr.Uid, hdr.Gid = int(st.Uid), int(st.Gid)
		if st.Mode&syscall.S_IFMT == syscall.S_IFCHR || st.Mode&syscall.S_IFMT == syscall.S_IFBLK {
			hdr.Devmajor, hdr.Devminor = int64(unix.Major(uint64(st.Rdev))), int64(unix.Minor(uint64(st.Rdev)))
		}
	}
	path := filepath.Join(e.layerDir, layerFSDirName, name)
	switch {
	case info.IsDir():
		hdr.Name += "/"
	case info.Mode()&fs.ModeSymlink != 0:
		target, err := os.Readlink(path)
		if err != nil {
			return err
		}
		hdr.Linkname = target
	case info.Mode()&fs.ModeSocket != 0:
		return nil // a socket means nothing in an image
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	n, err := io.Copy(tw, f)
	if err != nil {
		return err
	}
	if n != hdr.Size {
		return errors.New("file changed while it was written")
	}
	return nil
}
