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

package forkd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"
)

// Client calls forkd. Each call uses a connection of its own, so a blocking
// call such as Seed or Wait never holds up another. The context bounds the
// call, connection included.
type Client struct {
	socket string
	seq    atomic.Uint64
}

// NewClient returns a client of the forkd that serves socket.
func NewClient(socket string) *Client {
	return &Client{socket: socket}
}

func (c *Client) do(ctx context.Context, req *Request) (*Response, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", c.socket)
	if err != nil {
		return nil, fmt.Errorf("forkd: %w", err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	// A cancelled context closes the connection under the blocked read.
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Unix(1, 0)) })
	defer stop()

	req.Version = ProtocolVersion
	req.ID = fmt.Sprintf("c%d", c.seq.Add(1))
	if err := WriteMessage(conn, req); err != nil {
		return nil, fmt.Errorf("forkd: sending %s: %w", req.Op, ctxErr(ctx, err))
	}
	var resp Response
	if err := ReadMessage(conn, &resp); err != nil {
		return nil, fmt.Errorf("forkd: %s: %w", req.Op, ctxErr(ctx, err))
	}
	if resp.ID != "" && resp.ID != req.ID {
		return nil, fmt.Errorf("forkd: response %q answers another request than %q", resp.ID, req.ID)
	}
	if !resp.OK {
		if resp.Error == nil {
			return nil, fmt.Errorf("forkd: %s failed without an error", req.Op)
		}
		return nil, resp.Error
	}
	return &resp, nil
}

// ctxErr names the context as the cause when it ended the call. The
// connection deadline can fire a moment before the context reports it.
func ctxErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if _, ok := ctx.Deadline(); ok && errors.Is(err, os.ErrDeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return err
}

// Hello describes forkd.
func (c *Client) Hello(ctx context.Context) (*HelloInfo, error) {
	resp, err := c.do(ctx, &Request{Op: OpHello})
	if err != nil {
		return nil, err
	}
	if resp.Hello == nil {
		return nil, errors.New("forkd: hello answered with no description")
	}
	return resp.Hello, nil
}

// Seed boots a template and returns once it can fork.
func (c *Client) Seed(ctx context.Context, req *SeedRequest) (*SeedInfo, error) {
	resp, err := c.do(ctx, &Request{Op: OpSeed, Seed: req})
	if err != nil {
		return nil, err
	}
	if resp.Seed == nil {
		return nil, errors.New("forkd: seed answered with no seed")
	}
	return resp.Seed, nil
}

// Fork forks a child and returns once its guest runs.
func (c *Client) Fork(ctx context.Context, req *ForkRequest) (*ChildInfo, error) {
	resp, err := c.do(ctx, &Request{Op: OpFork, Fork: req})
	if err != nil {
		return nil, err
	}
	if resp.Child == nil {
		return nil, errors.New("forkd: fork answered with no child")
	}
	return resp.Child, nil
}

// Wait blocks until the child exits, or until timeout when it is not negative.
// The returned child is still running if the timeout expired first.
func (c *Client) Wait(ctx context.Context, childID string, timeout time.Duration) (*ChildInfo, error) {
	ms := int64(-1)
	if timeout >= 0 {
		ms = timeout.Milliseconds()
	}
	resp, err := c.do(ctx, &Request{Op: OpWait, Wait: &WaitRequest{ChildID: childID, TimeoutMS: ms}})
	if err != nil {
		return nil, err
	}
	if resp.Child == nil {
		return nil, errors.New("forkd: wait answered with no child")
	}
	return resp.Child, nil
}

// Delete kills and removes a child, or a seed with its children. A seed or
// child forkd does not know is not an error.
func (c *Client) Delete(ctx context.Context, kind, id string) error {
	_, err := c.do(ctx, &Request{Op: OpDelete, Delete: &DeleteRequest{Kind: kind, ID: id}})
	var fe *Error
	if errors.As(err, &fe) && fe.Code == CodeNotFound {
		return nil
	}
	return err
}

// List returns the seeds and children forkd holds.
func (c *Client) List(ctx context.Context) ([]SeedInfo, []ChildInfo, error) {
	resp, err := c.do(ctx, &Request{Op: OpList})
	if err != nil {
		return nil, nil, err
	}
	return resp.Seeds, resp.Children, nil
}

// IsCode reports whether err is a forkd error with code.
func IsCode(err error, code string) bool {
	var fe *Error
	return errors.As(err, &fe) && fe.Code == code
}
