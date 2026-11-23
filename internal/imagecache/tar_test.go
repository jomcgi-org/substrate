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
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// writeTestLayer materializes a pool layer. files maps a rel path to its
// content, "dir/" paths are directories, and "name -> target" is a symlink.
func writeTestLayer(t *testing.T, files map[string]string, wh *whiteoutSet) string {
	t.Helper()
	layer := t.TempDir()
	root := filepath.Join(layer, layerFSDirName)
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		switch {
		case name[len(name)-1] == '/':
			if err := os.MkdirAll(filepath.Join(root, name), 0o755); err != nil {
				t.Fatal(err)
			}
		case content != "" && content[0] == '>':
			if err := os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(content[1:], filepath.Join(root, name)); err != nil {
				t.Fatal(err)
			}
		default:
			if err := os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o640); err != nil {
				t.Fatal(err)
			}
		}
	}
	if wh != nil {
		b, err := json.Marshal(wh)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(layer, layerWhiteoutsFileName), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return layer
}

// readTar returns the entries of a tar stream by name, with the order they
// came in.
func readTar(t *testing.T, b []byte) (map[string]string, []string) {
	t.Helper()
	got := map[string]string{}
	var order []string
	tr := tar.NewReader(bytes.NewReader(b))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return got, order
		}
		if err != nil {
			t.Fatal(err)
		}
		order = append(order, hdr.Name)
		switch hdr.Typeflag {
		case tar.TypeReg:
			content, _ := io.ReadAll(tr)
			got[hdr.Name] = string(content)
		case tar.TypeSymlink:
			got[hdr.Name] = ">" + hdr.Linkname
		case tar.TypeDir:
			got[hdr.Name] = "/"
		}
	}
}

func TestImageWriteTar(t *testing.T) {
	base := writeTestLayer(t, map[string]string{
		"bin/sh":          "busybox",
		"bin/ls":          "ls v1",
		"etc/hosts":       "hosts",
		"etc/old.conf":    "old",
		"var/cache/a":     "a",
		"var/cache/b":     "b",
		"usr/lib/libc.so": ">libc.so.6",
		"tmp/":            "",
	}, nil)
	upper := writeTestLayer(t, map[string]string{
		"bin/ls":       "ls v2",
		"etc/new.conf": "new",
		"var/cache/c":  "c",
		"tmp/":         "",
	}, &whiteoutSet{
		Version:      1,
		Whiteouts:    []string{"etc/old.conf"},
		Opaques:      []string{"var/cache"},
		ImplicitDirs: []string{"tmp"},
	})
	img := &Image{LayerDirs: []string{base, upper}}

	var buf bytes.Buffer
	if err := img.WriteTar(&buf); err != nil {
		t.Fatalf("WriteTar() = %v", err)
	}
	got, order := readTar(t, buf.Bytes())
	want := map[string]string{
		"bin/":            "/",
		"bin/sh":          "busybox",
		"bin/ls":          "ls v2",
		"etc/":            "/",
		"etc/hosts":       "hosts",
		"etc/new.conf":    "new",
		"var/":            "/",
		"var/cache/":      "/",
		"var/cache/c":     "c",
		"usr/":            "/",
		"usr/lib/":        "/",
		"usr/lib/libc.so": ">libc.so.6",
		"tmp/":            "/",
	}
	for name, content := range want {
		if got[name] != content {
			t.Errorf("entry %q = %q, want %q", name, got[name], content)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("unexpected entry %q = %q", name, got[name])
		}
	}
	if !slices.IsSorted(order) {
		t.Errorf("entries are not in path order: %v", order)
	}

	// The same image makes the same stream.
	var again bytes.Buffer
	if err := img.WriteTar(&again); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), again.Bytes()) {
		t.Error("WriteTar() output differs between runs")
	}
}

func TestImageWriteTarImplicitDirKeepsLowerAttributes(t *testing.T) {
	base := writeTestLayer(t, map[string]string{"tmp/": ""}, nil)
	if err := os.Chmod(filepath.Join(base, layerFSDirName, "tmp"), 0o777|os.ModeSticky); err != nil {
		t.Fatal(err)
	}
	upper := writeTestLayer(t, map[string]string{"tmp/file": "x"}, &whiteoutSet{Version: 1, ImplicitDirs: []string{"tmp"}})

	var buf bytes.Buffer
	if err := (&Image{LayerDirs: []string{base, upper}}).WriteTar(&buf); err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(&buf)
	for {
		hdr, err := tr.Next()
		if err != nil {
			t.Fatal("no entry for tmp/")
		}
		if hdr.Name == "tmp/" {
			if mode := hdr.FileInfo().Mode(); mode&os.ModeSticky == 0 {
				t.Errorf("tmp/ lost its sticky bit: %v", mode)
			}
			return
		}
	}
}

func TestImageWriteTarFileReplacesDirectory(t *testing.T) {
	base := writeTestLayer(t, map[string]string{"opt/app/bin": "x", "opt/app/lib": "y"}, nil)
	upper := writeTestLayer(t, map[string]string{"opt/app": "now a file"}, nil)
	var buf bytes.Buffer
	if err := (&Image{LayerDirs: []string{base, upper}}).WriteTar(&buf); err != nil {
		t.Fatal(err)
	}
	got, _ := readTar(t, buf.Bytes())
	if got["opt/app"] != "now a file" {
		t.Errorf("opt/app = %q, want the file", got["opt/app"])
	}
	for name := range got {
		if name == "opt/app/bin" || name == "opt/app/lib" {
			t.Errorf("a file of the replaced directory survived: %s", name)
		}
	}
}

func TestImageWriteTarBadWhiteout(t *testing.T) {
	layer := writeTestLayer(t, map[string]string{"a": "a"}, &whiteoutSet{Version: 1, Whiteouts: []string{"../escape"}})
	if err := (&Image{LayerDirs: []string{layer}}).WriteTar(io.Discard); err == nil {
		t.Error("WriteTar() accepted a whiteout outside the layer")
	}
}
