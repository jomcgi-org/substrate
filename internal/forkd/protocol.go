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

// Package forkd is the client of forkd, the daemon in the gVisor fork that
// holds seed sandboxes and forks single-use child sandboxes from them. The
// types mirror the wire schema in runsc/forkd/protocol.go of
// github.com/jomcgi-org/gvisor, version 1. The protocol is JSON over a Unix
// stream socket. Each message is a 4-byte big-endian length and one JSON
// object. A connection serves one request at a time.
package forkd

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// ProtocolVersion is the version of the wire protocol this package speaks.
const ProtocolVersion = 1

// MaxFrameBytes bounds one message.
const MaxFrameBytes = 1 << 20

// Operations.
const (
	OpHello  = "hello"
	OpSeed   = "seed"
	OpFork   = "fork"
	OpWait   = "wait"
	OpDelete = "delete"
	OpList   = "list"
)

// Error codes.
const (
	CodeInvalidArgument    = "invalid_argument"
	CodeNotFound           = "not_found"
	CodeAlreadyExists      = "already_exists"
	CodeFailedPrecondition = "failed_precondition"
	CodePermissionDenied   = "permission_denied"
	CodeResourceExhausted  = "resource_exhausted"
	CodeUnavailable        = "unavailable"
	CodeInternal           = "internal"
)

// Child and seed states.
const (
	StateRunning = "running"
	StateExited  = "exited"
	StateReady   = "ready"
)

// Exit reasons.
const (
	ReasonExited      = "exited"
	ReasonKilled      = "killed"
	ReasonOOM         = "oom"
	ReasonMaxLifetime = "max_lifetime"
	ReasonLogLimit    = "log_limit"
	ReasonDeleted     = "deleted"
	ReasonSeedGone    = "seed_gone"
)

// Request is one message to forkd.
type Request struct {
	Version int    `json:"version"`
	ID      string `json:"id,omitempty"`
	Op      string `json:"op"`

	Seed   *SeedRequest   `json:"seed,omitempty"`
	Fork   *ForkRequest   `json:"fork,omitempty"`
	Wait   *WaitRequest   `json:"wait,omitempty"`
	Delete *DeleteRequest `json:"delete,omitempty"`
}

// Response is forkd's answer to one Request.
type Response struct {
	Version int    `json:"version"`
	ID      string `json:"id,omitempty"`
	OK      bool   `json:"ok"`
	Error   *Error `json:"error,omitempty"`

	Hello    *HelloInfo  `json:"hello,omitempty"`
	Seed     *SeedInfo   `json:"seed,omitempty"`
	Child    *ChildInfo  `json:"child,omitempty"`
	Seeds    []SeedInfo  `json:"seeds,omitempty"`
	Children []ChildInfo `json:"children,omitempty"`
}

// Error is a failed operation.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// SeedRequest boots a template from a digest-pinned EROFS image.
type SeedRequest struct {
	SeedID      string `json:"seed_id"`
	Image       string `json:"image"`
	ImageSHA256 string `json:"image_sha256"`

	Args             []string `json:"args"`
	Env              []string `json:"env,omitempty"`
	Cwd              string   `json:"cwd,omitempty"`
	UID              uint32   `json:"uid,omitempty"`
	GID              uint32   `json:"gid,omitempty"`
	DropCapabilities bool     `json:"drop_capabilities,omitempty"`
	Tmpfs            []Tmpfs  `json:"tmpfs,omitempty"`

	Workers int `json:"workers,omitempty"`
	CPUs    int `json:"cpus,omitempty"`
}

// Tmpfs is an in-memory mount of the guest.
type Tmpfs struct {
	Path      string `json:"path"`
	SizeBytes uint64 `json:"size_bytes,omitempty"`
}

// ForkRequest forks one child.
type ForkRequest struct {
	SeedID  string   `json:"seed_id"`
	ChildID string   `json:"child_id"`
	Env     []string `json:"env,omitempty"`

	Cgroup *Cgroup `json:"cgroup,omitempty"`
	Net    *Net    `json:"net,omitempty"`

	MaxLifetimeMS int64 `json:"max_lifetime_ms,omitempty"`
	LogBytesLimit int64 `json:"log_bytes_limit,omitempty"`
}

// Cgroup are the limits of a child's cgroup.
type Cgroup struct {
	CPUMax     string `json:"cpu_max,omitempty"`
	MemoryHigh int64  `json:"memory_high,omitempty"`
	MemoryMax  int64  `json:"memory_max,omitempty"`
	PidsMax    int64  `json:"pids_max,omitempty"`
}

// Net is the network of a child.
type Net struct {
	NetNSPath   string `json:"netns_path,omitempty"`
	DisableIPv6 bool   `json:"disable_ipv6,omitempty"`
}

// WaitRequest waits for a child to exit.
type WaitRequest struct {
	ChildID   string `json:"child_id"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
}

// DeleteRequest deletes a child or a seed.
type DeleteRequest struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// Delete kinds.
const (
	KindChild = "child"
	KindSeed  = "seed"
)

// HelloInfo describes forkd.
type HelloInfo struct {
	ForkdVersion    string      `json:"forkd_version"`
	ProtocolVersion int         `json:"protocol_version"`
	Platform        string      `json:"platform"`
	RunscSHA256     string      `json:"runsc_sha256,omitempty"`
	SentrySHA256    string      `json:"sentry_sha256,omitempty"`
	Assertions      []Assertion `json:"assertions"`
	MaxChildren     int         `json:"max_children"`
	Seeds           int         `json:"seeds"`
	Children        int         `json:"children"`
}

// Assertion is the result of one startup check.
type Assertion struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// SeedInfo describes a seed.
type SeedInfo struct {
	SeedID      string `json:"seed_id"`
	State       string `json:"state"`
	PID         int    `json:"pid"`
	ImageSHA256 string `json:"image_sha256"`
	Workers     int    `json:"workers"`
	CPUs        int    `json:"cpus"`
	Platform    string `json:"platform"`
	ReadyMS     int64  `json:"ready_ms"`
	CreatedUnix int64  `json:"created_unix_ms"`
	Children    int    `json:"children"`
}

// ChildInfo describes a child.
type ChildInfo struct {
	ChildID     string    `json:"child_id"`
	SeedID      string    `json:"seed_id"`
	State       string    `json:"state"`
	PID         int       `json:"pid"`
	StdoutPath  string    `json:"stdout_path"`
	StderrPath  string    `json:"stderr_path"`
	CgroupPath  string    `json:"cgroup_path,omitempty"`
	NetNSPath   string    `json:"netns_path,omitempty"`
	ForkMS      float64   `json:"fork_ms"`
	StartedUnix int64     `json:"started_unix_ms"`
	Exit        *ExitInfo `json:"exit,omitempty"`
}

// ExitInfo is the end of a child.
type ExitInfo struct {
	Reason   string `json:"reason"`
	ExitCode *int   `json:"exit_code,omitempty"`
	Signal   int    `json:"signal,omitempty"`
	OOMKills int64  `json:"oom_kills,omitempty"`
	AtUnix   int64  `json:"at_unix_ms"`
	Detail   string `json:"detail,omitempty"`
}

// WriteMessage writes v as one frame.
func WriteMessage(w io.Writer, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(data) > MaxFrameBytes {
		return fmt.Errorf("message of %d bytes exceeds the %d byte limit", len(data), MaxFrameBytes)
	}
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf, uint32(len(data)))
	copy(buf[4:], data)
	_, err = w.Write(buf)
	return err
}

// ReadMessage reads one frame into v. It returns io.EOF only when the stream
// ends between frames.
func ReadMessage(r io.Reader, v any) error {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrameBytes {
		return fmt.Errorf("frame of %d bytes exceeds the %d byte limit", n, MaxFrameBytes)
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	return json.Unmarshal(data, v)
}
