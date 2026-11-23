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

// Package forkdtest is a forkd for tests: it speaks the wire protocol, keeps
// seeds and children in memory, and lets a test decide when a child exits.
package forkdtest

import (
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/forkd"
)

// Fake is a forkd listening on a socket.
type Fake struct {
	t        *testing.T
	listener net.Listener
	dir      string

	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	nextPID  int
	seeds    map[string]*seed
	children map[string]*child

	// Hello is what hello answers. A test may change it before the first call.
	Hello forkd.HelloInfo
	// SeedErr and ForkErr, when set, fail every seed or fork.
	SeedErr, ForkErr *forkd.Error
	seedReqs         []forkd.SeedRequest
	forkReqs         []forkd.ForkRequest
	deleteReqs       []forkd.DeleteRequest
}

// SeedRequests returns the seed requests so far, in order.
func (f *Fake) SeedRequests() []forkd.SeedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]forkd.SeedRequest(nil), f.seedReqs...)
}

// ForkRequests returns the fork requests so far, in order.
func (f *Fake) ForkRequests() []forkd.ForkRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]forkd.ForkRequest(nil), f.forkReqs...)
}

// DeleteRequests returns the delete requests so far, in order.
func (f *Fake) DeleteRequests() []forkd.DeleteRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]forkd.DeleteRequest(nil), f.deleteReqs...)
}

type seed struct {
	req  forkd.SeedRequest
	info forkd.SeedInfo
}

type child struct {
	info forkd.ChildInfo
	done chan struct{}
	once sync.Once
}

// Serve starts a fake forkd on a socket under t's temp directory and returns
// it with the socket path.
func Serve(t *testing.T) (*Fake, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "forkd-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "forkd.sock")
	lis, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	f := &Fake{
		t: t, listener: lis, dir: dir,
		conns: map[net.Conn]struct{}{}, nextPID: 1000,
		seeds: map[string]*seed{}, children: map[string]*child{},
		Hello: forkd.HelloInfo{
			ForkdVersion: "fake", ProtocolVersion: forkd.ProtocolVersion, Platform: "kvm",
			Assertions: []forkd.Assertion{{Name: "production_mode", OK: true}, {Name: "sentry_seccomp", OK: true}},
		},
	}
	go f.accept()
	t.Cleanup(f.Crash)
	return f, socket
}

// Crash closes the socket and every connection, as a dead forkd does.
func (f *Fake) Crash() {
	_ = f.listener.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	for c := range f.conns {
		_ = c.Close()
	}
	for _, ch := range f.children {
		ch.finish(forkd.ExitInfo{Reason: forkd.ReasonKilled})
	}
}

// Exit ends a child the way its guest would.
func (f *Fake) Exit(childID string, exit forkd.ExitInfo) {
	f.mu.Lock()
	ch := f.children[childID]
	f.mu.Unlock()
	if ch == nil {
		f.t.Fatalf("forkdtest: no child %q", childID)
	}
	ch.finish(exit)
}

// ExitCode is a guest that exited with code.
func ExitCode(code int) forkd.ExitInfo {
	return forkd.ExitInfo{Reason: forkd.ReasonExited, ExitCode: &code}
}

// HasChild reports whether forkd still holds the child.
func (f *Fake) HasChild(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.children[id]
	return ok
}

// HasSeed reports whether forkd still holds the seed.
func (f *Fake) HasSeed(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.seeds[id]
	return ok
}

func (c *child) finish(exit forkd.ExitInfo) {
	c.once.Do(func() {
		exit.AtUnix = time.Now().UnixMilli()
		c.info.State = forkd.StateExited
		c.info.Exit = &exit
		close(c.done)
	})
}

func (f *Fake) accept() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.conns[conn] = struct{}{}
		f.mu.Unlock()
		go f.serve(conn)
	}
}

func (f *Fake) serve(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		f.mu.Lock()
		delete(f.conns, conn)
		f.mu.Unlock()
	}()
	for {
		var req forkd.Request
		if err := forkd.ReadMessage(conn, &req); err != nil {
			return
		}
		resp := f.handle(conn, &req)
		resp.Version, resp.ID = forkd.ProtocolVersion, req.ID
		if err := forkd.WriteMessage(conn, resp); err != nil {
			return
		}
	}
}

func fail(code, msg string) *forkd.Response {
	return &forkd.Response{Error: &forkd.Error{Code: code, Message: msg}}
}

func (f *Fake) handle(conn net.Conn, req *forkd.Request) *forkd.Response {
	switch req.Op {
	case forkd.OpHello:
		f.mu.Lock()
		defer f.mu.Unlock()
		hello := f.Hello
		return &forkd.Response{OK: true, Hello: &hello}
	case forkd.OpSeed:
		return f.seed(req.Seed)
	case forkd.OpFork:
		return f.fork(req.Fork)
	case forkd.OpWait:
		return f.wait(conn, req.Wait)
	case forkd.OpDelete:
		return f.delete(req.Delete)
	case forkd.OpList:
		return f.list()
	}
	return fail(forkd.CodeInvalidArgument, "unknown op "+req.Op)
}

func (f *Fake) seed(req *forkd.SeedRequest) *forkd.Response {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req == nil {
		return fail(forkd.CodeInvalidArgument, "no seed")
	}
	f.seedReqs = append(f.seedReqs, *req)
	if f.SeedErr != nil {
		return &forkd.Response{Error: f.SeedErr}
	}
	// forkd refuses a seed whose cgroup has no memory_max.
	if req.Cgroup == nil || req.Cgroup.MemoryMax <= 0 {
		return fail(forkd.CodeInvalidArgument, "cgroup: a cgroup with memory_max is required")
	}
	if _, ok := f.seeds[req.SeedID]; ok {
		return fail(forkd.CodeAlreadyExists, req.SeedID)
	}
	f.nextPID++
	s := &seed{req: *req, info: forkd.SeedInfo{SeedID: req.SeedID, State: forkd.StateReady, PID: f.nextPID, ImageSHA256: req.ImageSHA256, Platform: "kvm"}}
	f.seeds[req.SeedID] = s
	info := s.info
	return &forkd.Response{OK: true, Seed: &info}
}

func (f *Fake) fork(req *forkd.ForkRequest) *forkd.Response {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req == nil {
		return fail(forkd.CodeInvalidArgument, "no fork")
	}
	f.forkReqs = append(f.forkReqs, *req)
	if f.ForkErr != nil {
		return &forkd.Response{Error: f.ForkErr}
	}
	// forkd refuses a child whose cgroup lacks pids_max or memory_max.
	if cg := req.Cgroup; cg == nil || cg.MemoryMax <= 0 || cg.PidsMax <= 0 {
		return fail(forkd.CodeInvalidArgument, "cgroup: a cgroup with pids_max and memory_max is required")
	}
	s, ok := f.seeds[req.SeedID]
	if !ok {
		return fail(forkd.CodeNotFound, "seed "+req.SeedID)
	}
	if _, ok := f.children[req.ChildID]; ok {
		return fail(forkd.CodeAlreadyExists, req.ChildID)
	}
	f.nextPID++
	logs := filepath.Join(f.dir, "children", req.ChildID)
	if err := os.MkdirAll(logs, 0o755); err != nil {
		return fail(forkd.CodeInternal, err.Error())
	}
	ch := &child{done: make(chan struct{}), info: forkd.ChildInfo{
		ChildID: req.ChildID, SeedID: s.info.SeedID, State: forkd.StateRunning, PID: f.nextPID,
		StdoutPath: filepath.Join(logs, "stdout.log"), StderrPath: filepath.Join(logs, "stderr.log"),
		CgroupPath: "/sys/fs/cgroup/forkd/" + req.ChildID,
	}}
	for _, p := range []string{ch.info.StdoutPath, ch.info.StderrPath} {
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			return fail(forkd.CodeInternal, err.Error())
		}
	}
	f.children[req.ChildID] = ch
	info := ch.info
	return &forkd.Response{OK: true, Child: &info}
}

func (f *Fake) wait(conn net.Conn, req *forkd.WaitRequest) *forkd.Response {
	f.mu.Lock()
	ch := f.children[req.ChildID]
	f.mu.Unlock()
	if ch == nil {
		return fail(forkd.CodeNotFound, "child "+req.ChildID)
	}
	var timeout <-chan time.Time
	if req.TimeoutMS >= 0 {
		timeout = time.After(time.Duration(req.TimeoutMS) * time.Millisecond)
	}
	select {
	case <-ch.done:
	case <-timeout:
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	info := ch.info
	return &forkd.Response{OK: true, Child: &info}
}

func (f *Fake) delete(req *forkd.DeleteRequest) *forkd.Response {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req == nil {
		return fail(forkd.CodeInvalidArgument, "no delete")
	}
	f.deleteReqs = append(f.deleteReqs, *req)
	switch req.Kind {
	case forkd.KindChild:
		ch, ok := f.children[req.ID]
		if !ok {
			return fail(forkd.CodeNotFound, req.ID)
		}
		ch.finish(forkd.ExitInfo{Reason: forkd.ReasonDeleted})
		delete(f.children, req.ID)
	case forkd.KindSeed:
		if _, ok := f.seeds[req.ID]; !ok {
			return fail(forkd.CodeNotFound, req.ID)
		}
		for id, ch := range f.children {
			if ch.info.SeedID == req.ID {
				ch.finish(forkd.ExitInfo{Reason: forkd.ReasonSeedGone})
				delete(f.children, id)
			}
		}
		delete(f.seeds, req.ID)
	default:
		return fail(forkd.CodeInvalidArgument, "kind "+req.Kind)
	}
	return &forkd.Response{OK: true}
}

func (f *Fake) list() *forkd.Response {
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &forkd.Response{OK: true}
	for _, s := range f.seeds {
		resp.Seeds = append(resp.Seeds, s.info)
	}
	for _, c := range f.children {
		resp.Children = append(resp.Children, c.info)
	}
	return resp
}
