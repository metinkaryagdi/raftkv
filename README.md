# raftkv — Raft consensus from scratch, with a replicated key-value store

A from-scratch implementation of the **Raft consensus algorithm** in Go, driving a
replicated in-memory key-value store. It implements the core protocol from
[*In Search of an Understandable Consensus Algorithm*](https://raft.github.io/raft.pdf)
(Ongaro & Ousterhout): **leader election** and **log replication**, with the
safety properties that make systems like etcd, Consul and CockroachDB tick.

The emphasis is on **correctness and test quality**, not feature count. Every
safety-critical property is backed by a real, concurrent, `-race`-clean test —
including the hard ones: log reconciliation after a leader change, and split-brain
prevention under a network partition.

- **~82% statement coverage** (hand-written code), **88% on the core `raft` package**
- All tests run under the Go **race detector**
- A real **5-process cluster** over **gRPC**, with an **HTTP** client API
- The same core is driven by an **in-memory transport** that injects crashes,
  partitions and latency for deterministic tests

---

## Table of contents

- [Scope](#scope)
- [Architecture](#architecture)
- [How it works](#how-it-works)
- [Quick start](#quick-start)
- [Failure demo: surviving a leader crash](#failure-demo-surviving-a-leader-crash)
- [Project structure & phase map](#project-structure--phase-map)
- [Testing](#testing)
- [Design decisions](#design-decisions)
- [References](#references)

---

## Scope

Deliberately scoped to the **core** of Raft, done well, rather than the whole
paper done partially.

**Implemented**
- ✅ **Leader election** — Follower/Candidate/Leader state machine, randomized
  election timeouts, terms, the `RequestVote` RPC, the §5.4.1 up-to-date-log
  voting restriction
- ✅ **Log replication** — the `AppendEntries` RPC, the log-matching property,
  fast conflict backtracking (`ConflictIndex`/`ConflictTerm`), commit-index
  advancement by majority under the §5.4.2 current-term rule
- ✅ **Replicated state machine** — an in-memory key-value store (`set`/`delete`/`get`)
- ✅ **Two transports** behind one interface — in-memory (tests, fault injection)
  and gRPC (real multi-process)
- ✅ **A no-op entry on election** (§8) so entries carried over from previous
  terms become applicable
- ✅ **Failure scenarios** — leader crash / continuous availability, network
  partition / split-brain prevention

**Out of scope** (by design, to keep it focused and finished)
- ❌ Log compaction / snapshotting
- ❌ Dynamic cluster membership changes (adding/removing nodes)
- ❌ Deployment across real, separate servers (a single-machine multi-process
  cluster is used; latency and partitions are simulated)

---

## Architecture

The core algorithm knows nothing about the wire. It talks to peers through a
`Transport` interface and receives RPCs through an `RPCHandler` interface. That
one seam is what makes the project both realistic (gRPC) and rigorously testable
(in-memory fault injection).

```
        client (curl / any HTTP)
                │  GET/PUT/DELETE /kv/{key}
                ▼
        ┌───────────────────┐
        │  server.Server    │  waits for commit, leader-only reads,
        │  (HTTP API)       │  redirects writes to the leader
        └─────────┬─────────┘
                  │ Submit(cmd) / ApplyCh
                  ▼
        ┌───────────────────┐        ┌──────────────────────────┐
        │  raft.Node        │───────▶│  Transport (interface)   │
        │  election +       │        ├──────────────────────────┤
        │  log replication  │◀───────│  inmem   │   grpcx (gRPC) │
        └─────────┬─────────┘  RPCs  └──────────────────────────┘
                  │ apply committed entries
                  ▼
        ┌───────────────────┐
        │  kvstore.Store    │  the replicated state machine
        └───────────────────┘
```

### Node state machine

```mermaid
stateDiagram-v2
    [*] --> Follower
    Follower --> Candidate: election timeout (no heartbeat)
    Candidate --> Candidate: split vote, new term, retry
    Candidate --> Leader: wins a majority of votes
    Candidate --> Follower: sees current leader or higher term
    Leader --> Follower: sees higher term
```

### A write, end to end

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant F as Followers (majority)
    C->>L: PUT /kv/city istanbul
    L->>L: append {set city istanbul} to log
    L->>F: AppendEntries(prevLogIndex, entries)
    F->>L: success (entry stored)
    L->>L: majority stored ⇒ advance commitIndex
    L->>L: apply to state machine
    L-->>C: 200 OK
    L->>F: AppendEntries(leaderCommit++)
    F->>F: apply to state machine
```

---

## How it works

**Leader election.** Each node runs a randomized election timer. On expiry it
becomes a candidate, increments its term, votes for itself, and requests votes
from peers. A vote is granted only if the candidate's log is at least as
up-to-date as the voter's (§5.4.1). Randomized timeouts make split votes rare and
self-correcting; a winner sends heartbeats that suppress further elections.

**Log replication.** Clients' commands become log entries on the leader, which
replicates them via `AppendEntries`. Each RPC carries `prevLogIndex`/`prevLogTerm`;
a follower rejects it unless that entry matches, guaranteeing the **log-matching
property**. On rejection the follower returns a conflict hint so the leader backs
up a whole term at once instead of one index at a time. An entry **commits** once
a majority has stored it *and* it belongs to the leader's current term (§5.4.2) —
the rule that prevents a subtle data-loss race across leader changes.

**Safety under failure.** A leader stranded in a minority partition can append
entries but can never commit them, so it cannot lose data or create a second
source of truth. When it rejoins, it discovers the higher term, steps down, and
its uncommitted tail is truncated and replaced by the majority's log.

---

## Quick start

Requires **Go 1.25+** (developed on 1.26). The race detector additionally needs a
C toolchain (any gcc/clang; on Windows, mingw-w64).

```bash
# Run the whole test suite under the race detector
go test ./... -race

# Coverage (hand-written code)
go test ./internal/... -coverprofile=coverage.out -coverpkg=./internal/...
go tool cover -func=coverage.out | tail -1
```

### Run a live 5-node cluster

```powershell
# Windows (PowerShell)
scripts\run-cluster.ps1
```
```bash
# Linux/macOS/Git Bash
scripts/run-cluster.sh
```

Then talk to it over HTTP (any node; writes are redirected to the leader):

```bash
curl http://127.0.0.1:8001/status
curl -X PUT http://127.0.0.1:8001/kv/city -d istanbul   # -> {"status":"ok"} or a leader hint
curl http://127.0.0.1:8001/kv/city                      # -> {"key":"city","value":"istanbul"}
```

| Method | Path          | Meaning                                  |
|--------|---------------|------------------------------------------|
| GET    | `/status`     | role, term, leader, commit index         |
| GET    | `/kv/{key}`   | read a value (leader only)               |
| PUT    | `/kv/{key}`   | set value (request body = raw value)     |
| DELETE | `/kv/{key}`   | delete a key                             |

A non-leader answers writes/reads with `421 Misdirected Request` and a JSON body
naming the current leader.

### Or run it in Docker

Each node runs as its own container on a shared Docker network, finding its peers
by service name over gRPC — closer to a real deployment than five processes on one
host. The HTTP API of each node is published to a distinct host port (8001–8005).

```bash
docker compose up --build          # start the 5-node cluster
curl http://127.0.0.1:8001/status
curl -X PUT http://127.0.0.1:8001/kv/city -d istanbul

docker compose kill n1             # kill the leader's container and watch failover
curl http://127.0.0.1:8002/kv/city # the write survives on the new leader

docker compose down                # tear everything down
```

> **Note:** BuildKit can fail to build if the project lives in a path with
> non-ASCII characters. If `docker compose up --build` errors on a session
> shared-key, build once with the legacy builder and then start without building:
> `DOCKER_BUILDKIT=0 docker build -t raftkv:latest . && docker compose up -d`.

### Or run it on Kubernetes

[`deploy/k8s/raftkv.yaml`](deploy/k8s/raftkv.yaml) runs the cluster as a
**StatefulSet** behind a headless Service — the same pattern etcd itself uses in
production, because Raft peer addressing needs the stable per-pod identity and
DNS a Deployment doesn't give you. Every pod shares one identical spec: it learns
its own id from its pod name (via the downward API) and derives its peers' stable
DNS names (`raftkv-0.raftkv-headless`, `raftkv-1.raftkv-headless`, ...) from a
replica count, with no per-pod configuration
(see `k8sPeersFromEnv` in [`cmd/raftkv/main.go`](cmd/raftkv/main.go)).

```bash
# Local cluster via kind (https://kind.sigs.k8s.io/)
kind create cluster --name raftkv
scripts/k8s-deploy.sh raftkv        # builds the image, loads it into kind, applies the manifests

kubectl get pods -l app=raftkv -w
kubectl port-forward pod/raftkv-0 8001:8001 &
curl http://127.0.0.1:8001/status

# Kill the leader pod and watch the StatefulSet recreate it while the cluster
# elects a new leader and keeps serving:
scripts/demo-failover-k8s.sh
```

Captured run ([`docs/demo-failover-k8s.log`](docs/demo-failover-k8s.log)):

```
=== Waiting for a leader among the 5 pods ===
  leader = raftkv-2

=== Writing city=istanbul via raftkv-2 ===
{"key":"city","value":"istanbul"}

=== DELETING leader pod raftkv-2 (StatefulSet will recreate it) ===

=== Waiting for the cluster to elect a NEW leader ===
  new leader = raftkv-4

=== Pre-crash write survived, and a new write commits ===
{"key":"city","value":"istanbul"}
{"key":"lang","value":"go"}

=== Pods after failover ===
NAME       READY   STATUS    RESTARTS   AGE
raftkv-0   1/1     Running   0          91s
raftkv-1   1/1     Running   0          91s
raftkv-2   1/1     Running   0          45s   <- recreated by the StatefulSet
raftkv-3   1/1     Running   0          91s
raftkv-4   1/1     Running   0          91s
```

---

## Failure demo: surviving a leader crash

`scripts/demo-failover.ps1` starts a real 5-process cluster, writes a value, then
**kills the leader process** and shows the cluster elect a new leader and keep
serving — with the pre-crash write still intact. Captured run
([`docs/demo-failover.log`](docs/demo-failover.log); exact node ids and term
numbers vary per run):

```
=== Waiting for leader election ===
  leader = n5  (term 5)

=== Writing city=istanbul via leader n5 ===
  read back: city = istanbul

=== KILLING leader n5 (pid 42204) ===
  leader process terminated

=== Waiting for the cluster to elect a NEW leader ===
  new leader = n1  (term 6)

=== Cluster still serving: pre-crash write survived, and a new write commits ===
  city (written before crash) = istanbul
  lang (written after crash)  = go

=== Final status of all live nodes ===
  n1: role=Leader    term=6 leader=n1 commit=4
  n2: role=Follower  term=6 leader=n1 commit=3
  n3: role=Follower  term=6 leader=n1 commit=3
  n4: role=Follower  term=6 leader=n1 commit=3
  n5: (down)
```

Split-brain prevention (only the majority commits; the minority's write fails;
after healing, the minority adopts the majority's log) is proved deterministically
by `TestPartitionSplitBrainPrevention` and `TestLogReconciliationAfterLeaderChange`.

---

## Project structure & phase map

The project was built in five reviewable phases (one commit each).

| Phase | Focus | Key files |
|------|-------|-----------|
| **1** | Leader election | [`internal/raft/raft.go`](internal/raft/raft.go), [`election.go`](internal/raft/election.go), [`handlers.go`](internal/raft/handlers.go) |
| **2** | Log replication | [`internal/raft/replication.go`](internal/raft/replication.go), [`apply.go`](internal/raft/apply.go), [`log.go`](internal/raft/log.go) |
| **3** | State machine, client API, gRPC | [`internal/kvstore/`](internal/kvstore/), [`internal/server/`](internal/server/), [`internal/transport/grpcx/`](internal/transport/grpcx/), [`cmd/raftkv/`](cmd/raftkv/) |
| **4** | Failure scenarios & demo | [`internal/server/failure_test.go`](internal/server/failure_test.go), [`scripts/demo-failover.ps1`](scripts/demo-failover.ps1) |
| **5** | Documentation | this README, [`docs/`](docs/) |

```
internal/
  raft/          core algorithm (transport-agnostic)
    raft.go          Node, config, main loop, role transitions
    election.go      RequestVote fan-out, becoming leader
    replication.go   AppendEntries fan-out, conflict backtracking
    handlers.go      inbound RequestVote / AppendEntries
    apply.go         Submit, commit-index advancement, apply loop
    log.go           log helpers, up-to-date check
    status.go        observable state snapshots
  kvstore/       the replicated state machine
  server/        binds Raft to the store; HTTP client API
  transport/
    inmem/           in-process transport w/ crash & partition injection
    grpcx/           gRPC transport (+ generated raftpb)
cmd/raftkv/      one cluster node (gRPC peers + HTTP API)
deploy/k8s/      StatefulSet + headless Service manifest
scripts/         run-cluster.{ps1,sh}, demo-failover.ps1, k8s-deploy.sh, demo-failover-k8s.sh
```

---

## Testing

The interesting failure modes in Raft are timing- and concurrency-dependent, so
tests use **real goroutines and real timers** (no mocked time), and all run under
`go test -race`.

| Test | What it proves |
|------|----------------|
| `TestElectSingleLeader` | exactly one leader is elected from a cold start |
| `TestReElectionAfterLeaderFailure` | a crashed leader is replaced, in a higher term |
| `TestRejoinedLeaderStepsDown` | a partitioned old leader steps down (no split brain) |
| `TestLeaderStabilityNoSpuriousElections` | heartbeats suppress needless elections |
| `TestLogReplicationBasic` | commands replicate and apply in identical order everywhere |
| `TestFollowerCatchUp` | a reconnected follower catches up automatically |
| `TestLogReconciliationAfterLeaderChange` | divergent tails reconcile; orphans are discarded |
| `TestNoCommitWithoutMajority` | a minority leader can never advance its commit index |
| `TestStateMachineConvergence` | all replicas reach identical state |
| `TestWriteSurvivesLeaderChange` | committed writes are durable across failover |
| `TestClusterAvailableAcrossLeaderCrash` | the cluster keeps serving through a crash |
| `TestPartitionSplitBrainPrevention` | partitions cannot cause split-brain |
| `TestGRPCElectionAndReplication` | the real gRPC/TCP transport works end to end |

Coverage (hand-written code, excluding generated protobuf):

| Package | Coverage |
|---------|----------|
| `internal/raft` (core) | **88%** |
| `internal/kvstore` | 100% |
| `internal/transport/inmem` | 85% |
| `internal/transport/grpcx` | 76% |
| `internal/server` | 64% |
| **total** | **~82%** |

---

## Design decisions

- **Transport as an interface.** The core `raft.Node` depends only on `Transport`
  and `RPCHandler`. The in-memory transport makes tests deterministic and lets
  them inject crashes (`Isolate`), partitions (`Partition`) and latency; gRPC runs
  the same code across processes. This is the single most important structural
  choice — it is what makes the split-brain and reconciliation tests possible.
- **A single mutex, and no RPCs under lock.** All mutable node state is guarded by
  one mutex; network fan-out always happens after releasing it. Vote/replication
  replies are re-validated against the current term when they return, so stale
  responses can't corrupt state.
- **No-op on election (§8).** A new leader appends a no-op entry for its term so
  that committing it transitively commits (and makes applicable) entries inherited
  from earlier terms — without it, a just-elected leader can hold committed-but-
  unapplied data it cannot yet serve.
- **Fast conflict backtracking.** Rejected `AppendEntries` carry a conflict term
  and index so the leader rewinds a whole term per round trip instead of one entry.
- **Leader-only reads.** Reads are served only by the leader for read-your-writes
  behavior. Full linearizability would add a ReadIndex heartbeat barrier — noted,
  and out of scope here.

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus
  Algorithm (Extended Version)*, 2014 — <https://raft.github.io/raft.pdf>
- The Raft website and visualization — <https://raft.github.io/>
