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

package forkd_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/forkd"
	"go.ate.dev/substrate/internal/forkd/forkdtest"
)

func TestClient(t *testing.T) {
	fake, socket := forkdtest.Serve(t)
	c := forkd.NewClient(socket)
	ctx := t.Context()

	hello, err := c.Hello(ctx)
	if err != nil || hello.Platform != "kvm" || hello.ProtocolVersion != forkd.ProtocolVersion {
		t.Fatalf("Hello() = %+v, %v", hello, err)
	}
	seed, err := c.Seed(ctx, &forkd.SeedRequest{SeedID: "seed-1", Image: "/img", ImageSHA256: "ab", Args: []string{"/bin/app"}})
	if err != nil || seed.State != forkd.StateReady || seed.PID == 0 {
		t.Fatalf("Seed() = %+v, %v", seed, err)
	}
	if _, err := c.Seed(ctx, &forkd.SeedRequest{SeedID: "seed-1"}); !forkd.IsCode(err, forkd.CodeAlreadyExists) {
		t.Errorf("a second seed of one id = %v, want already_exists", err)
	}
	child, err := c.Fork(ctx, &forkd.ForkRequest{SeedID: "seed-1", ChildID: "child-1", Env: []string{"A=b"}})
	if err != nil || child.State != forkd.StateRunning || child.StdoutPath == "" {
		t.Fatalf("Fork() = %+v, %v", child, err)
	}
	if _, err := c.Fork(ctx, &forkd.ForkRequest{SeedID: "nope", ChildID: "child-2"}); !forkd.IsCode(err, forkd.CodeNotFound) {
		t.Errorf("a fork of an unknown seed = %v, want not_found", err)
	}

	// A wait with a timeout answers while the child runs.
	waiting, err := c.Wait(ctx, "child-1", 10*time.Millisecond)
	if err != nil || waiting.State != forkd.StateRunning {
		t.Fatalf("Wait(timeout) = %+v, %v", waiting, err)
	}
	// A wait without one blocks until the guest exits, on a connection of its own.
	done := make(chan *forkd.ChildInfo, 1)
	go func() {
		info, err := c.Wait(ctx, "child-1", -1)
		if err != nil {
			t.Errorf("Wait() = %v", err)
		}
		done <- info
	}()
	if _, err := c.Hello(ctx); err != nil {
		t.Fatalf("a blocked wait held up another call: %v", err)
	}
	fake.Exit("child-1", forkdtest.ExitCode(3))
	info := <-done
	if info == nil || info.State != forkd.StateExited || info.Exit.ExitCode == nil || *info.Exit.ExitCode != 3 {
		t.Fatalf("Wait() after the exit = %+v", info)
	}

	seeds, children, err := c.List(ctx)
	if err != nil || len(seeds) != 1 || len(children) != 1 {
		t.Errorf("List() = %v, %v, %v", seeds, children, err)
	}
	if err := c.Delete(ctx, forkd.KindChild, "child-1"); err != nil {
		t.Errorf("Delete(child) = %v", err)
	}
	if err := c.Delete(ctx, forkd.KindChild, "child-1"); err != nil {
		t.Errorf("a second Delete() = %v, want nil: forkd does not know it", err)
	}
	if err := c.Delete(ctx, forkd.KindSeed, "seed-1"); err != nil {
		t.Errorf("Delete(seed) = %v", err)
	}
	if got := len(fake.DeleteRequests()); got != 3 {
		t.Errorf("forkd saw %d delete requests, want 3", got)
	}
}

func TestClientCallsFailWhenForkdIsGone(t *testing.T) {
	fake, socket := forkdtest.Serve(t)
	c := forkd.NewClient(socket)
	if _, err := c.Seed(t.Context(), &forkd.SeedRequest{SeedID: "s"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fork(t.Context(), &forkd.ForkRequest{SeedID: "s", ChildID: "c"}); err != nil {
		t.Fatal(err)
	}
	// A wait in flight ends with an error when forkd goes.
	result := make(chan error, 1)
	go func() {
		_, err := c.Wait(t.Context(), "c", -1)
		result <- err
	}()
	time.Sleep(20 * time.Millisecond)
	fake.Crash()
	select {
	case <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("a wait on a dead forkd never returned")
	}
	if _, err := c.Hello(t.Context()); err == nil {
		t.Error("Hello() succeeded against a dead forkd")
	}
}

func TestClientContext(t *testing.T) {
	_, socket := forkdtest.Serve(t)
	c := forkd.NewClient(socket)
	if _, err := c.Seed(t.Context(), &forkd.SeedRequest{SeedID: "s"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fork(t.Context(), &forkd.ForkRequest{SeedID: "s", ChildID: "c"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := c.Wait(ctx, "c", -1); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Wait() with an expired context = %v, want the deadline", err)
	}
}

func TestFrames(t *testing.T) {
	var buf bytes.Buffer
	if err := forkd.WriteMessage(&buf, forkd.Request{Version: 1, Op: forkd.OpHello}); err != nil {
		t.Fatal(err)
	}
	var got forkd.Request
	if err := forkd.ReadMessage(&buf, &got); err != nil || got.Op != forkd.OpHello {
		t.Errorf("ReadMessage() = %+v, %v", got, err)
	}
	if err := forkd.ReadMessage(&buf, &got); err == nil {
		t.Error("ReadMessage() of an empty stream succeeded")
	}
	huge := forkd.Request{Op: string(make([]byte, forkd.MaxFrameBytes))}
	if err := forkd.WriteMessage(&buf, huge); err == nil {
		t.Error("WriteMessage() accepted a frame over the limit")
	}
}
