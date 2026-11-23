# Seeds and forked actors

A template with `fork_config` starts its actors by forking a seed sandbox that
each worker holds, instead of booting or restoring them. The feature is behind
the `ForkSeed` preview gate, for gVisor workers on the KVM platform only. With
the gate off nothing changes for any actor.

## How it works

```
ActorTemplate.fork_config            WorkerPool platform: kvm
        |                                   |
 SeedReconciler --creates--> seed Actor (system, pinned to one Worker)
        |                      ResumeActor -> atelet.Seed -> worker.SeedWorkload -> forkd seed
        |                      RUNNING: Worker.status.seeds lists it
        v
 CreateActor (same atespace) -> forked Actor (SUSPENDED)
        |   ResumeActor: scheduler needs a Worker with a ready seed of the template,
        |   books private limits + runtime fork_overhead
        v   atelet.Fork -> worker.ForkWorkload -> forkd fork -> wakeup probe -> ingress
     RUNNING --workload exits--> worker.ReportActorExited -> control plane deletes the Actor
        '--crash, deadline, never started--> reaper deletes it
```

- A seed is an Actor with `status.fork.role = SEED`, created in the template's
  atespace and pinned to one Worker. Only the control plane suspends, updates,
  resumes or deletes one.
- A forked Actor has `role = FORK`. It runs once: suspend, revert and a second
  resume are refused, and there are no snapshots and no volumes.
- The seed reconciler (`controlapi/seed_reconciler.go`) runs when the gate is
  on. It builds one seed per fork template on each eligible Worker, rebuilds a
  crashed seed after a minute, and repairs `Worker.status.seeds`.
- A Worker is eligible when it reports a `fork_overhead` (its pool has
  `platform: kvm`), is ACTIVE, and matches the template's `worker_selector`.
- Turn the gate off only after the fork templates are deleted: deleting a
  template deletes its seeds, and only with the gate on do the reconciler and
  the reaper run.

## Isolation

Every control and flow below keeps the tag it has in the design.

| Tag | What holds it | Where |
|---|---|---|
| I1 | forkd refuses to run unless its startup assertions pass, and ateworker refuses a forkd whose `hello` fails one. The fork environment is `ATE_ACTOR_*` only. No request body travels in any RPC | `cmd/ateworker-gvisor/fork.go`, `controlapi/workflow_fork.go` |
| I2 | A fork template runs only for Actors in its own atespace. The scheduler matches seeds by atespace and template. A Worker holds seeds of one atespace, enforced by the reconciler and again by the worker process | `controlapi/fork.go`, `scheduling`, `seed_reconciler.go`, `fork.go` |
| I3 | Template env names that look like secrets are rejected. A forked actor has no egress, so it needs no credential | `apivalidation/fork.go` |
| I4 | `single_use` must be true, exits delete the Actor, the reaper deletes the rest, and forkd gets the cgroup limits, the lifetime and the log cap | `controlapi/fork_exit.go`, `fork_reaper.go` |
| I5 | Egress is NONE (the gateway option is rejected for now). The worker builds a netns per child | `apivalidation/fork.go`, `fork.go` |
| I7 | Creating a fork template needs `can_create_fork_template` on the installation. atelet builds the EROFS image from the digest-pinned layers and checks its hash before every seed. atelet and the worker check that a seed belongs to the request's atespace and template | `authz`, `cmd/atelet/erofs.go`, `seed.go` |

I6 (random state and identity of children) and I8 (microVM) are not in this
slice.

## Parts

- API: `ActorTemplate.fork_config`, `ActorStatus.fork`, `Worker.status.seeds`,
  `WorkerSandboxRuntime.fork_overhead`, `WorkerService.ReportActorExited`.
- atelet: `Seed`, `Fork`, `DropSeed`. `Seed` needs `mkfs.erofs`. The atelet
  image carries a static one in `/var/run/ko/mkfs.erofs-<arch>`, which
  `hack/build-mkfs-erofs.sh` builds into `cmd/atelet/kodata` before ko runs
  (the release jobs do). `--mkfs-erofs` overrides the path. The images are built
  without inline data, which the sentry cannot map.
- ateworker-gvisor: `SeedWorkload`, `ForkWorkload`, `DropSeed`, with
  `--fork-platform=kvm`. It starts `forkd` from `gvisor-bin/forkd` of the
  SandboxConfig tarball and speaks version 2 of the forkd protocol of the gVisor
  fork (`internal/forkd`). forkd runs the seed in its own cgroup with
  `memory.max` set to `seed_resources`, which the guest sees as its memory.
  A forked actor gets `memory.max` of its limit plus `fork_overhead`. Only
  `memory.max` kills: a cgroup above `memory.high` is throttled, so forkd
  refuses a limit without `memory.max`.
- WorkerPool: `platform: kvm` gives the worker pods `/dev/kvm`, `SYS_CHROOT`
  and `SETPCAP`.

## Test against a real forkd

`cmd/ateworker-gvisor/fork_forkd_linux_test.go` runs the fork manager with the
real forkd, runsc and worker network on a host with KVM. It needs root, cgroup
v2, `mkfs.erofs` and a static `busybox` on `PATH`, and `ATE_TEST_FORKD_DIR`
naming a directory with `runsc` and `gvisor-bin/{forkd,gvisor_sentry}`. The
engine test also needs `ATE_TEST_ENGINE_IMAGE` and `ATE_TEST_ENGINE_BODY`.

## Not done yet

- Resource usage of forked actors is not sampled.
- No pool of pre-forked actors, and no egress.
- The seed reconciler and the reaper list every template and Actor of the fork
  atespaces each pass.
