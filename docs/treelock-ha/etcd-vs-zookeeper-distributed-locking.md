# etcd vs ZooKeeper for Distributed Locking — Explicit Comparison Summary

Generated: 2026-05-17
Skill: distributed-systems-analyst
Context: Gravitino distributed locking evaluation

---

## Invariants Both Must Uphold

For distributed locking to be correct, any implementation must guarantee:

- **Mutual exclusion**: at most one holder at any moment — not just "probably one"
- **Liveness on failure**: a crashed holder's lock is released in bounded time
- **Fencing**: a stale holder that resumes after lock expiry must be rejected at the storage layer — not just by the lock manager
- **Linearizability**: reads used to make commit decisions must reflect the most recent write, not a stale replica

---

## Mechanism Comparison

### ZooKeeper + Apache Curator

| Property | Detail |
|----------|--------|
| **Lock primitive** | Ephemeral sequential znodes under a lock path (e.g., `/locks/catalog/lock-0000001`) |
| **Holder election** | Node with the lowest sequence number holds the lock; all others watch the predecessor |
| **Crash release** | Session timeout triggers automatic deletion of the ephemeral node |
| **Fencing token** | `zxid` (ZooKeeper Transaction ID) — monotonically increasing per-write identifier |
| **Read consistency** | Linearizable from leader; follower reads can be stale (use `sync()` before critical reads) |
| **Hierarchical locking** | Natural: the namespace is a tree; lock paths mirror the resource tree |
| **Protocol** | ZAB (ZooKeeper Atomic Broadcast) — Paxos-derived |
| **Operational cost** | Requires a separate quorum cluster (≥3 dedicated nodes); JVM-based |

**How it closes the races:**

- **TOCTOU**: Curator's lock recipe holds the znode from check to commit; no gap.
- **Split-brain**: Session timeout + ephemeral znode means two holders cannot both have active znodes simultaneously — one session will time out.
- **Stale reads**: `sync()` before reads forces a round-trip to leader. If you skip `sync()`, you get stale reads on followers — this is ZK's most common misuse.
- **Lost update**: Conditional writes (`setData` with expected version) provide CAS semantics.

**The subtle ZK watch ordering problem**: A watch fires *after* the event has already been processed by ZooKeeper. A competing writer can acquire the lock and take action before your watch callback even fires. This means watch-based invalidation logic must always re-read state after the watch fires, never act on the watch event itself.

---

### etcd v3.5+

| Property | Detail |
|----------|--------|
| **Lock primitive** | Lease-based: client creates a lease with TTL; lock key is created with `LeaseID` attached |
| **Crash release** | Lease expiry deletes all keys attached to it — including the lock key |
| **Fencing token** | `ModRevision` — per-key, monotonically increasing on every write; built into every response |
| **CAS primitive** | `txn` with `ModRevision` guard: write succeeds only if `ModRevision == expected` |
| **Read consistency** | Linearizable from leader (default); stale read is opt-in (`--consistency=s`) |
| **Hierarchical locking** | Not native — must be layered on top with key prefix conventions |
| **Protocol** | Raft |
| **Operational cost** | Single binary, embeds cleanly, gRPC interface; still requires ≥3 nodes for HA |

**How it closes the races:**

- **TOCTOU**: `txn` lets you do a compare + write atomically — "update key X only if `ModRevision` is still N." One network round-trip closes the check-then-act gap.
- **Split-brain**: Leases are tied to the etcd leader via Raft. A network partition that creates two leaders will cause one side's leases to stop being renewed — lock released on the minority side.
- **Stale reads / lost update**: `ModRevision` fencing token is returned on every write. Storage layers that check fencing tokens will reject writes whose revision is lower than the latest observed.
- **GC pause / slow client**: Lease keepalive must arrive within TTL. If a client is paused and keepalive stops, the lease expires and the lock is released — even if the client thinks it still holds it. The fencing token at the storage layer catches the stale write when the client resumes.

---

## Head-to-Head: The Critical Differences

| Dimension | ZooKeeper + Curator | etcd v3.5+ |
|-----------|---------------------|------------|
| **Fencing token** | `zxid` (global, per ZK write) | `ModRevision` (per-key, per-write) — more granular |
| **CAS primitive** | `setData(path, data, expectedVersion)` | `txn` with `ModRevision` guard — composable, multi-key |
| **Lock acquisition latency** | ~2–5 ms per ZK write (quorum) | ~1–3 ms per etcd write (Raft round-trip) |
| **Hierarchical namespace** | Native: tree is the data model | Layered: key prefixes simulate hierarchy |
| **Watch / notification** | Watches are one-shot; watch fires *after* event | Watch streams are continuous; event ordering guaranteed |
| **Session vs lease** | Session: ZK tracks per-client connection state | Lease: explicit TTL, renewed by client keepalive |
| **Embedded in-process** | No — ZK is always an external quorum | etcd can embed (used in k3s), but production uses external |
| **Operational footprint** | JVM, ZK data dir, separate ensemble | Go binary, simpler upgrade path |
| **Ecosystem trajectory (2026)** | Kafka 4.0 removed it; Pulsar migrating away | Kubernetes standard; CNCF graduated; growing adoption |

---

## The Fencing Token Difference (Most Important)

This is where etcd has a **structural advantage**:

**ZooKeeper**: The `zxid` is a global transaction ID across the entire ZK cluster. To use it as a fencing token, the storage layer needs to know which `zxid` the lock was granted at, and reject writes from a `zxid` lower than the current global state. This works, but the `zxid` is coarse — it increments on *any* ZK write by *any* client.

**etcd**: Every key has its own `ModRevision`. When you acquire a lock, you get back the `ModRevision` of the lock key *at the time you acquired it*. Your subsequent writes to the storage layer carry this token. If another client acquires the lock (after your lease expired), the storage layer sees a higher `ModRevision` and rejects your stale write. This is **per-lock fencing**, which is more precise and easier to implement correctly.

```
etcd lock flow with fencing:

1. Client A acquires lock → receives ModRevision = 42
2. Client A sends write to storage with token 42
3. [GC pause — lease expires]
4. Client B acquires same lock → receives ModRevision = 43
5. Client B writes with token 43
6. Client A resumes, sends write with token 42
7. Storage layer: max_seen = 43, rejecting write with token 42 ✅
```

---

## Split-Brain Under Network Partition

**ZooKeeper**: ZAB requires a quorum. Under a partition:
- Minority side: ZK becomes read-only (writes rejected). Clients on the minority side cannot acquire new locks.
- Ephemeral nodes on the minority side eventually expire if sessions time out.
- **Risk**: If session timeout is long, a minority-side client can hold a lock for an extended period while the majority side has already moved on.

**etcd**: Raft requires a quorum for writes. Under a partition:
- Minority side: etcd leader steps down, cannot process writes. New leader elected on majority side.
- Lease keepalives require write quorum — leases on the minority side will not be renewed and will expire.
- **Risk**: Same as ZK — a long lease TTL means a stranded client on the minority side holds the lock until TTL expiry.

**Both have the same fundamental behavior** under partition: the minority side loses the ability to hold locks. The difference is operational: etcd's lease model makes this more explicit than ZK's session model.

---

## Failure Scenario: GC Pause (The Classic Mistake)

Both etcd and ZooKeeper provide **advisory** locks, not **mandatory** enforcement. A critical section that does:

```
1. acquire_lock()
2. [GC pause for 30 seconds — TTL was 20 seconds]
3. write_to_storage()   # Lock expired. Another holder is now active.
```

...will produce split-brain unless the storage layer enforces fencing tokens. **This is not fixed by choosing etcd over ZooKeeper or vice versa.** The fencing token mechanism must be implemented end-to-end: lock manager → token propagation → storage-layer rejection.

This is the most commonly missed requirement in metadata catalog lock designs.

---

## Verdict

| Use Case | Recommendation | Reason |
|----------|---------------|--------|
| New system, no existing ZK dependency | **etcd** | Lower ops burden, `ModRevision` fencing built in, Kubernetes-native, ecosystem trajectory |
| System already running ZK (e.g., Kafka pre-4.0, HBase, older Gravitino) | **ZK + Curator** | Adding etcd alongside ZK doubles your coordination dependencies for no correctness gain |
| Hierarchical tree locking (parent-blocks-child) | **ZK** has a natural advantage | Hierarchical namespace maps directly; etcd requires prefix-key simulation |
| HA leader election only (coarse-grained) | **etcd** lease with epoch number | Simpler than ZK ephemeral nodes for this one use case |
| High-frequency fine-grained locking (per-table) | **Neither** — use storage-layer OCC | Both ZK and etcd serialize writes through quorum; this is a bottleneck at scale |

---

## 2026 Ecosystem Consensus

etcd is the default choice for new distributed systems needing coordination. ZooKeeper remains valid where it is already deployed and where hierarchical namespace is a first-class requirement. The Apache ecosystem (Kafka 4.0, Pulsar) is actively moving away from ZooKeeper as a required operational dependency.

For metadata catalogs specifically (Iceberg, Delta Lake, Gravitino), the modern pattern is to push coordination **into the storage layer** via OCC and version columns — eliminating the external coordinator entirely for fine-grained operations, and reserving etcd/ZK only for coarse-grained leader election.

---

## Leader Election Algorithms

### ZooKeeper: Ephemeral Sequential Znode Election

**Algorithm** (Curator `LeaderLatch` / `LeaderSelector` recipe):

```
Each candidate:
  1. Create ephemeral sequential znode: /election/candidate-NNNNNN
  2. List all children of /election, sorted ascending by sequence number
  3. If I have the lowest sequence number → I am the leader
  4. Otherwise → watch the znode immediately preceding mine (NOT the leader)
  5. When my watched predecessor is deleted → go to step 2 and re-check

On leader crash:
  - Session timeout triggers automatic deletion of the leader's ephemeral node
  - Only the next-in-line candidate's watch fires (no herd effect)
  - New leader elected in O(session_timeout) time
```

**Why watch the predecessor, not the leader?**
If all candidates watch the leader directly, a single leader deletion fires N-1 watches simultaneously — the "herd effect." Each of those N-1 nodes then hammers ZK with reads and a create attempt. Watching only the predecessor bounds the notification fan-out to 1 per election event.

**Failure boundaries**:
- Leader crash: detected within `session_timeout` (default 30s; configurable to ~3s in practice)
- Network partition: minority side cannot renew session → session expires → ephemeral node deleted → new election on majority side
- False positive (GC pause): if the leader is paused but alive, session can expire and a new leader is elected while the old one resumes — requires epoch/fencing tokens at the storage layer to prevent split-brain writes

**ZK leader election properties**:

| Property | Value |
|----------|-------|
| Detection latency | `session_timeout` (3–30s configurable) |
| Herd effect | None — predecessor-watch pattern |
| Fencing token | `zxid` of the winning create — must be propagated manually |
| Epoch advancement | Manual — application must increment and broadcast an epoch |
| Re-election on false positive | Yes — GC pause can trigger re-election of a live leader |

---

### etcd: Lease-Based Election with CAS

**Algorithm** (etcd `concurrency.Election` API):

```
Each candidate:
  1. Create a lease with TTL (e.g., 10s), start keepalive goroutine
  2. Attempt: txn { if /election/leader does not exist → create it with my LeaseID }
  3. If txn succeeds → I am the leader (my create_revision is the key's create_revision)
  4. If txn fails → watch /election/leader for DELETE event
  5. On DELETE → go to step 2

On leader crash / keepalive failure:
  - Lease expires → all keys attached to the lease are automatically deleted
  - /election/leader disappears → all watchers notified
  - First CAS winner becomes new leader (race on creation)
```

**Fencing token — built in:**
The key's `CreateRevision` monotonically increases with every new leader term. Every write to the storage layer carries this revision. The storage layer rejects writes whose `CreateRevision` is lower than the current max — making split-brain writes impossible without any extra application logic.

**Failure boundaries**:
- Leader crash: detected within lease TTL (configurable to ~2s with short TTL + keepalive)
- Network partition: minority-side leader's keepalive cannot reach quorum → lease expires → key deleted → election on majority side
- False positive (GC pause): lease expires if keepalive misses TTL → new election → stale leader's writes rejected by `CreateRevision` fencing

**etcd leader election properties**:

| Property | Value |
|----------|-------|
| Detection latency | Lease TTL (2–15s typical; lower = more keepalive load) |
| Herd effect | Possible — all watchers race on leader key deletion |
| Fencing token | `CreateRevision` — first-class, automatically propagated |
| Epoch advancement | Automatic — `CreateRevision` increments on every new election |
| Re-election on false positive | Yes — but stale leader writes are caught by `CreateRevision` |

---

### Algorithm Comparison: Leader Election

| Dimension | ZooKeeper | etcd |
|-----------|-----------|------|
| **Core primitive** | Ephemeral sequential znode | Lease-gated CAS key |
| **Predecessor watch (no herd)** | Yes — by design | No — all watchers race on key deletion |
| **Fencing token propagation** | Manual (`zxid` must be tracked by app) | Automatic (`CreateRevision` on every response) |
| **Detection speed** | Session timeout (min ~3s) | Lease TTL (min ~2s, with aggressive keepalive) |
| **False-positive safety** | Requires app-level epoch | `CreateRevision` closes it automatically |
| **Operational setup** | Separate ZK ensemble (≥3 JVM nodes) | etcd cluster (≥3 Go nodes) |
| **API complexity** | Curator `LeaderLatch` wraps the complexity | `concurrency.Election` — clean gRPC API |

**Bottom line on election**: etcd's `CreateRevision` makes it structurally safer against the GC-pause split-brain scenario. With ZK, you must manually track and propagate the `zxid` epoch; etcd does this for you. For a new system, etcd is the right choice for leader election.

---

## Gravitino TreeLock: What It Actually Is

Before comparing, it's important to understand what TreeLock is and is not.

**TreeLock is an in-process, in-JVM, hierarchical read-write lock.**

Reading `TreeLockNode.java` and `LockManager.java`:

```java
// TreeLockNode.java:104
private final ReentrantReadWriteLock readWriteLock;

// TreeLockNode.java:46
@VisibleForTesting final Map<String, TreeLockNode> childMap;  // ConcurrentHashMap
```

- The underlying primitive is `java.util.concurrent.locks.ReentrantReadWriteLock`
- The entire lock tree lives in the JVM heap — a `ConcurrentHashMap` of child nodes rooted at `/`
- Lock acquisition = walking the tree root-to-leaf, acquiring READ on all ancestors and READ/WRITE on the leaf
- Reference counting (`AtomicLong referenceCount`) tracks active lock holders for eviction
- A background thread evicts unreferenced nodes when `totalNodeCount > maxTreeNodeInMemory * 0.5`
- A deadlock checker logs threads holding a lock node for >30s

**There is no network, no external coordinator, no TTL, no fencing token.**

**Lock acquisition for `metalake.catalog.db1.table1` with WRITE on leaf:**
```
/ ──────────── READ
/metalake ──── READ
/metalake/catalog ── READ
/metalake/catalog/db1 ── READ
/metalake/catalog/db1/table1 ── WRITE
```

This is exactly the standard hierarchical intent-locking pattern from database theory (IS/IX/S/X). Gravitino implements the S/X subset (READ = S, WRITE = X, all ancestor locks are READ/S intent locks).

---

## TreeLock vs etcd vs ZooKeeper: Explicit Comparison

### Scope

| Dimension | TreeLock | ZooKeeper | etcd |
|-----------|----------|-----------|------|
| **Scope** | Single JVM (single Gravitino node) | Distributed — across all nodes | Distributed — across all nodes |
| **Lock state location** | JVM heap (`ConcurrentHashMap`) | ZK cluster (znodes) | etcd cluster (keys) |
| **Network round-trips per lock** | 0 | N levels × 1 ZK write each | N levels × 1 etcd write each (or 1 with prefix) |
| **Fencing token** | None | `zxid` (manual propagation) | `ModRevision` / `CreateRevision` (automatic) |
| **Crash recovery** | JVM death releases all JVM locks (OS cleans up threads) | Session timeout releases ephemeral znodes | Lease expiry releases all lease-bound keys |
| **Hierarchical locking** | Native (in-memory tree) | Native (znode tree) | Layered (key prefix simulation) |
| **HA support** | ❌ None — single node only | ✅ Yes | ✅ Yes |

### The Critical Gap: TreeLock Has No HA Story

TreeLock works correctly and efficiently for a **single Gravitino node**. Under active-active HA with multiple Gravitino nodes, each node has its own `LockManager` with its own independent in-memory tree. Two nodes can simultaneously acquire a write lock on `metalake/catalog/db1/table1` — one on each node's tree — with no awareness of each other.

```
Node A (LockManager A):             Node B (LockManager B):
  /metalake/catalog/db1/table1        /metalake/catalog/db1/table1
  WRITE lock acquired ✅               WRITE lock acquired ✅  ← split-brain
  writes table metadata                writes different table metadata
  both writes go to the same DB        last-writer-wins, silently
```

This is not a theoretical race — it is the guaranteed outcome of any active-active multi-node deployment without cross-node coordination.

### Which External Coordinator Suffices for Gravitino?

The answer depends on the HA topology Gravitino targets.

#### Option 1: Active-Standby (one leader, N standbys)

**etcd suffices — and is the right choice.**

- etcd leader election (lease-based) ensures only one Gravitino node is active at a time
- The active node runs TreeLock exactly as today — zero change to the fine-grained locking logic
- On failover: lease expires → standby wins election → becomes the new active → its TreeLock takes over
- The storage layer's own transaction isolation (e.g., database ACID) handles any in-flight writes from the old leader

**What etcd does NOT replace**: TreeLock's fine-grained per-path locking. With active-standby, you don't need to replace it — only one node's TreeLock is active at a time, so there is no cross-node race.

**Fencing requirement**: The new leader must increment an epoch (use etcd's `CreateRevision` as the epoch) and any writes in-flight from the old leader must be detectable as stale. This requires Gravitino's metadata store to store and check the current epoch on every write — a storage-layer change, not a lock-layer change.

```
etcd + TreeLock active-standby:

[Node A is leader, epoch = CreateRevision 42]
  → Node A's TreeLock serializes all fine-grained ops locally
  → All metadata writes carry epoch=42

[Node A pauses (GC), lease expires]
  → etcd deletes /election/leader
  → Node B wins election, epoch = CreateRevision 43

[Node A resumes, attempts a write with epoch=42]
  → Metadata store: current epoch=43, rejecting write with epoch=42 ✅
```

#### Option 2: Active-Active (multiple nodes serving writes concurrently)

**Neither etcd nor ZooKeeper suffices as a drop-in replacement for TreeLock.** 

Distributing TreeLock's per-path locks across an etcd or ZK cluster means every metadata operation — including reads — requires one network round-trip per level of the namespace tree. For a 4-level path, that is 4 sequential ZK/etcd writes per request. This is a 10–50× latency increase over the current in-memory approach.

The correct architecture for active-active is **storage-layer OCC** (the Iceberg/Delta Lake pattern): remove TreeLock for concurrency control and instead use version columns + compare-and-swap at the database layer. TreeLock can be retained as a local cache-invalidation gate, not as the authoritative lock.

### Verdict for Gravitino

| HA Model | Coordinator Needed | TreeLock Role | Recommendation |
|----------|-------------------|---------------|----------------|
| Single node | None | Current behavior — correct and optimal | No change needed |
| Active-standby HA | **etcd** (leader election only) | Unchanged — runs on the active node only | Add etcd lease election; propagate `CreateRevision` epoch to storage writes |
| Active-active HA | etcd or ZK for coordination, OR storage OCC | Replace or demote TreeLock | Storage-layer OCC (Iceberg pattern) is the modern approach; distributed TreeLock via ZK/etcd is operationally expensive |

**For Gravitino's current architecture and likely near-term HA requirements, etcd with active-standby leader election is the appropriate choice.** It is the lowest-cost path to HA correctness: etcd handles failover, TreeLock handles fine-grained in-node serialization, and the storage layer's epoch check closes the fencing gap.

ZooKeeper would also be correct, and its hierarchical namespace is a natural fit for Gravitino's path structure — but Kafka 4.0's removal of ZK and Pulsar's migration away from it signal that adding a new ZK dependency in 2026 is swimming against the current. etcd is the better long-term investment.

---

## Redlock vs etcd

### What Redlock Is

Redlock (proposed by Antirez, Redis creator) is a distributed locking algorithm over multiple independent Redis nodes.

**Algorithm:**
```
To acquire a lock:
  1. Record start time T1
  2. For each of N Redis nodes (typically 5), in sequence:
       SET lock_key unique_token NX PX ttl_ms
  3. Count successful acquisitions
  4. Compute elapsed time: drift = now - T1
  5. If acquired >= ceil(N/2)+1 AND drift < ttl_ms → lock is held
  6. Otherwise → release all acquired locks and retry

To release:
  For each node: if lock_key == my_token → DEL lock_key  (via Lua script)
```

The quorum requirement (≥3 of 5) means a single Redis node failure does not prevent lock acquisition or create a false grant.

---

### The Core Difference: What Each System Coordinates Through

| | Redlock | etcd |
|---|---|---|
| **Coordination medium** | N independent Redis nodes, no consensus between them | Single Raft cluster — all nodes agree on a total order |
| **Consistency model** | No consensus — each node decides independently | Linearizable — Raft guarantees a single committed log |
| **Fencing token** | None — `unique_token` is not monotonic | `ModRevision` — monotonically increasing, per-key |
| **Failure model** | Timing-dependent — correctness requires bounded clock drift | Timing-independent — correctness requires only quorum availability |

This is the fundamental divide. Both use TTLs — but timing enters the picture at completely different layers, and that difference determines what breaks when timing assumptions fail.

#### Where Timing Lives in Each System

**Redlock — timing is load-bearing for quorum correctness:**

The TTL is set client-side. Each of the 5 Redis nodes independently decides when to expire the lock based on its own local clock. There is no consensus between them. If node R3's clock is 5 seconds fast:

```
Client A holds lock on all 5 nodes, TTL=10s.
At wall-clock T=5s: R3 expires the lock (its clock reads T=10s).
Client B acquires lock on R1, R2, R3  →  quorum of 3/5. Granted.
Client A still holds R4, R5 — and thinks it has 5/5.
Both A and B satisfy the quorum condition simultaneously.
```

The timing assumption (bounded clock drift) is **what makes the quorum mean anything**. If that assumption breaks, the quorum breaks — two holders exist simultaneously, and no fencing token exists to detect it.

**etcd — timing determines liveness, consensus enforces correctness:**

etcd also uses TTLs for leases. But when a lease TTL fires, that expiry is not decided by a single node's clock — it is **committed through Raft**. The leader proposes the expiry, the quorum agrees, the log entry is committed, and only then is the lease considered expired by all nodes. A minority node with a skewed clock cannot unilaterally expire a lease and grant a new one.

```
etcd internal lease expiry:
  Leader detects keepalive has not arrived within TTL window
  → Proposes "expire lease L" to Raft log
  → Majority of nodes (≥2 of 3) commit the entry
  → Only now is lease L expired across the cluster
  → Lock key is deleted (also a Raft-committed operation)
```

Clock skew between etcd nodes does not cause two lease holders to coexist, because no single node can grant or expire a lease without a committed majority vote.

**The remaining timing assumption in etcd — and how it's handled:**

etcd does have a timing assumption: the lease holder must send keepalives within the TTL or the lease expires, potentially while the holder is still alive (GC pause). This is a **liveness** timing assumption — it affects *when* the lock is released, not *whether two holders can exist simultaneously*.

When the paused holder resumes and attempts a write, the Raft-committed `ModRevision` / `CreateRevision` has already advanced (a new leader was elected). The storage layer rejects the stale write. The fencing token closes the gap that the TTL opened.

```
Redlock timing failure → two concurrent holders, no detection mechanism
etcd TTL failure      → one expired + one new holder, stale write rejected by revision
```

The table entry is corrected below:

---

### Why Redlock Is Unsafe for Metadata Writes

**Scenario 1 — Clock skew:**
```
Redis node R3 has clock skew of +5 seconds.
Client A acquires lock on R1, R2, R3, R4, R5 with TTL=10s.
From R3's perspective, the lock expires in 5s (not 10s).
At T=6s: R3 expires the lock. Client B acquires lock on R1, R2, R3.
Client B now holds lock on R1, R2, R3.
Client A still holds lock on R4, R5 — and thinks it holds a quorum.
Both A and B believe they hold the lock. Split-brain. ✅ for an attacker, ❌ for you.
```

**Scenario 2 — GC pause (Kleppmann's definitive scenario):**
```
1. Client A acquires Redlock successfully. unique_token = "abc123".
2. Client A enters a 40s GC pause. TTL was 30s.
3. Lock expires on all 5 Redis nodes.
4. Client B acquires Redlock. unique_token = "xyz789".
5. Client B writes to storage: version 2 of table metadata.
6. Client A resumes. It still holds "abc123" in memory.
7. Client A writes to storage: version 2 of table metadata (overwrites B's write).
```
There is no fencing token to prevent step 7. The storage layer has no way to know that Client A's lock expired. Client B's write is silently overwritten.

With etcd, step 7 is caught: Client A's write carries `ModRevision=42`, Client B's write has already bumped the revision to `43`, and the storage layer rejects `42 < 43`.

**Scenario 3 — Redis node restart with AOF persistence disabled (or delayed fsync):**
```
Client A acquires lock on R1, R2, R3, R4, R5.
R3 crashes and restarts. Its in-memory lock state is gone.
Client B acquires lock on R1, R2, R3 (R3 now has no record of A's lock).
Both A and B hold quorums.
```
Redlock's spec recommends a "delayed restart" (wait TTL seconds before accepting requests after restart) to mitigate this, but this is an operational requirement that is easy to misconfigure.

---

### The Antirez Rebuttal and Why It Doesn't Hold for Catalogs

Antirez's position: "You're designing for a world with arbitrary delays. In practice, delays are bounded and clocks are close enough."

This is a reasonable engineering argument for **efficiency locks** — rate limiting, deduplication, cache stampede prevention — where an occasional double-execution costs a cache miss, not data corruption.

It is **not** a reasonable argument for metadata catalog writes, where a double-write means:
- A table that appears to be created but has no storage path
- A renamed schema that exists under both old and new names
- A dropped table that is still queryable
- Concurrent schema alterations producing a merged-but-invalid schema

For Gravitino's metadata store, these are correctness failures, not performance degradations.

---

### Raft: The Consensus Layer That Makes etcd Correct

Yes — etcd's correctness guarantee rests entirely on Raft. Without Raft, etcd would be Redlock with a single Redis node. Understanding Raft explains exactly why clock skew between etcd nodes cannot create two concurrent lock holders.

**Raft in one paragraph:**

Raft is a consensus algorithm designed around a single principle: at any point in time, at most one node is the leader, and every committed log entry has been acknowledged by a majority of nodes. A cluster of 2f+1 nodes can tolerate f failures. Raft operates in terms:

```
Term N:  [Node A is leader]
Term N+1: [Node A unreachable → election → Node B wins → Node B is leader]
```

Each term has at most one leader. A node becomes leader by collecting votes from a majority. A node only votes for one candidate per term. Therefore two nodes cannot both be elected leader in the same term — the pigeonhole principle over the majority quorum makes this impossible.

**Raft leader election algorithm (the actual steps):**

```
All nodes start as Followers with a randomized election timeout (150–300ms).

1. A Follower's timeout fires (no heartbeat received from leader)
   → converts to Candidate
   → increments its current term
   → votes for itself
   → sends RequestVote RPC to all other nodes

2. Each node grants a vote IF:
   - It hasn't voted yet in this term
   - Candidate's log is at least as up-to-date as the voter's log

3. Candidate collects a majority of votes → becomes Leader for this term
   → sends heartbeats (AppendEntries with no entries) to suppress new elections

4. If no majority within election timeout → election times out → increment term → retry

Split vote (two candidates simultaneously): randomized timeouts make this unlikely;
if it occurs, both time out, increment term, and retry — guaranteed to resolve.
```

**How this connects to etcd lease expiry:**

When etcd's Raft leader decides a lease has expired (keepalive not received within TTL), it appends "expire lease L" to its Raft log and broadcasts it. This entry is only committed — and only acted upon — when a **majority of etcd nodes** acknowledge it. The deletion of the lock key is also a Raft log entry. No etcd node unilaterally deletes the key or grants a new lock. Every state transition goes through the committed log.

This is why clock skew between etcd nodes is irrelevant to correctness: the clock on a single node cannot trigger a lock grant. Only a majority-committed log entry can.

**The TTL as a liveness mechanism, not a safety mechanism:**

```
Safety (correctness) = enforced by Raft
  → At most one lock holder at a time
  → Cannot be broken by clock skew between etcd nodes

Liveness (progress) = enforced by TTL + keepalive
  → A crashed holder's lock is released in bounded time
  → CAN be affected by the holder's own clock/GC/network — but only breaks liveness,
     never breaks the "at most one" safety property
  → The fencing token (ModRevision) catches the case where liveness fails
    and a stale holder resumes
```

---

### Kafka KRaft: Raft Without an External Coordinator

Kafka 4.0 (March 2025) removed ZooKeeper entirely and replaced it with **KRaft** — Raft embedded directly inside Kafka brokers. This is the most important recent example of Raft applied to production metadata coordination, and it directly informs the question of how Gravitino should approach HA.

**What Kafka used ZooKeeper for:**
- Controller election (which broker is the active controller)
- Topic metadata (partition counts, replication factor, ISR)
- Broker registration (which brokers are alive)
- ACL storage

All of these are distributed coordination problems. ZK solved them by being an external consensus service. KRaft solves them by embedding consensus into the application itself.

**How KRaft works:**

```
One broker is designated the "active controller" — the Raft leader.
Metadata changes (topic creation, partition reassignment, broker join/leave)
are written as records to a special internal Kafka topic: __cluster_metadata.

All brokers replicate this topic. The controller is the Raft leader for this topic.
Metadata reads come from the local replica (eventually consistent for followers)
or from the controller (linearizable).

Leader election:
  - Raft election among the controller quorum nodes (a designated subset of brokers)
  - Same Raft term/vote/heartbeat algorithm as above
  - New controller's first action: write its epoch to __cluster_metadata
  - All subsequent metadata writes carry this epoch
  - Stale broker commands carrying an old epoch are rejected
```

**What Kafka learned and why it matters:**

| ZooKeeper era | KRaft era |
|---|---|
| Controller election: ZK ephemeral node | Controller election: Raft leader election |
| Metadata storage: ZK znodes | Metadata storage: Kafka log (append-only, replicated) |
| Fencing: ZK epoch (`zxid`) | Fencing: Kafka controller epoch (monotonic, in the log) |
| External dependency: ZK ensemble (3–5 JVM nodes) | No external dependency — Raft is inside the brokers |
| Recovery time: limited by ZK session timeout | Recovery time: limited by Raft election timeout (milliseconds) |

**The key insight from Kafka for Gravitino:**

Kafka's lesson is not "use Raft instead of ZooKeeper." It is: **if your metadata store is itself replicated and ordered, you already have consensus — you don't need a separate coordinator at all.**

Kafka's `__cluster_metadata` topic IS the coordination layer. The append-only log with monotonically increasing offsets IS the fencing token. The Raft leader IS the single writer. The external coordinator (ZK) was redundant once the log existed.

For Gravitino, the analogous pattern would be: if Gravitino's metadata store (relational DB or otherwise) provides serializable transactions with version columns, the store already provides the ordering guarantees that etcd or ZK would provide. The lock then becomes a performance optimization (avoid DB round-trips for every read) rather than the correctness authority.

**How `__cluster_metadata` removes the need to track who's leader:**

The insight is that KRaft does not serialize the leader node ID. It serializes the **controller epoch** — a monotonically increasing integer that advances with every Raft leader election. The epoch IS the leader identity for correctness purposes. You never need to ask "who is leader?" — you only need to ask "is this epoch current?"

Every record written to `__cluster_metadata` carries the controller epoch in its Raft log entry header (via the Raft term, which KRaft maps 1:1 to the controller epoch). The record types include:

```
BeginQuorumEpoch  — written on election, contains new epoch number
TopicRecord       — topic creation, carries epoch in the batch header
PartitionRecord   — partition assignment, carries epoch
LeaderChangeRecord — leadership transition marker
```

When a broker receives a metadata update or a `LeaderAndIsrRequest` from the controller:

```
Broker checks: request.controllerEpoch >= broker.seenEpoch?
  Yes → accept, update seenEpoch
  No  → reject with NOT_CONTROLLER error
```

No external "who is leader" lookup. No lease check. The epoch number in the committed log entry is the proof.

**Why node ID is irrelevant for correctness:**

Raft guarantees that only the term-N leader can write entries tagged with term N to the committed log. A node cannot forge a high epoch — it can only write with the epoch it was legitimately elected under, and that election required a quorum vote. So:

```
Old leader (epoch 5) wakes up after GC pause:
  Tries to write PartitionRecord with epoch=5
  Brokers have seen epoch=6 (new leader elected while old leader was paused)
  Brokers reject: 5 < 6 → NOT_CONTROLLER
  Old leader's write is a no-op. No data corruption.
```

The epoch is both the fencing token and the implicit leader identity. Knowing `epoch=6` means "the node elected in Raft term 6 wrote this" — without ever needing to store or check a node ID separately. This is how the replicated log eliminates the need for an external "who is leader" registry: the log itself is the registry, and the epoch in every committed entry is the proof of authority.

**How epoch reads are efficient — log as recovery, memory as serving:**

The epoch is not read from the log on every request. The log is the durable source of truth for recovery; in steady state the epoch is an in-memory integer on every broker.

```
Broker startup:
  Replay __cluster_metadata log (or from latest snapshot)
  → reconstruct in-memory state: topic map, partition assignments, current epoch
  → this.currentControllerEpoch = last BeginQuorumEpoch record seen

Steady state (every LeaderAndIsrRequest):
  if (request.epoch >= this.currentControllerEpoch) → accept   // O(1) int compare
  else → reject NOT_CONTROLLER

New leader elected:
  KRaft commits BeginQuorumEpoch(epoch=7) to __cluster_metadata via Raft
  All brokers receive the replicated record → update in-memory epoch to 7
```

This is the standard pattern for every log-structured system (Kafka, etcd, databases with WAL):

| Layer | Role |
|---|---|
| Append-only log | Durable, replayable source of truth. Written on every state change. |
| In-memory state | Materialized view of the log. Rebuilt on startup by replaying the log. Serves all reads. |
| Snapshot | Periodically taken to bound replay time on restart — avoids replaying from offset 0 every time. |

etcd follows the same pattern: every key's `ModRevision` is a log offset, but etcd serves reads from a B-tree in memory (bbolt), not by scanning the Raft log. The log gets the write to disk; the B-tree gets the read from RAM.

"Reading the epoch from the log" only happens once — on restart, during state reconstruction. After that it lives in memory until the process dies. The epoch check at request time is always an O(1) integer comparison.

---

**KRaft vs etcd as the coordination layer:**

| | etcd | KRaft pattern |
|---|---|---|
| **Deployment model** | External cluster (separate from app) | Embedded in the application nodes |
| **Coordination primitive** | Lease + CAS over gRPC | Replicated log with Raft leader |
| **Fencing token** | `ModRevision` (per key) | Log offset / controller epoch |
| **Operational overhead** | Separate etcd cluster to manage | No external dependency; app nodes do double duty |
| **Applicable to Gravitino** | Yes — active-standby HA | Yes — if Gravitino embeds a replicated metadata log |
| **Complexity to implement** | Low (client library exists) | High (must implement or embed a Raft library) |

**When to choose KRaft pattern over etcd for Gravitino:**

The KRaft pattern (embed Raft, no external coordinator) is the right architecture if:
- Gravitino targets zero external runtime dependencies
- Gravitino's metadata volume is large enough that the overhead of per-operation etcd round-trips becomes a bottleneck
- Gravitino wants sub-100ms failover (Raft election is faster than etcd lease TTL expiry)

The etcd pattern is the right architecture if:
- Gravitino is deployed in Kubernetes (etcd is already there)
- The engineering cost of embedding Raft is not justified (active-standby with one active node handles the load)
- The team wants a proven, off-the-shelf solution with a client library

For Gravitino's current scale and architecture, **etcd active-standby is the pragmatic path**. KRaft-style embedding is the right long-term target if Gravitino ever needs active-active multi-master writes — but that requires replacing TreeLock with a replicated log, which is a larger architectural shift than adding etcd leader election.

---

### When Redlock Is Acceptable

| Use case | Redlock acceptable? | Reason |
|----------|---------------------|--------|
| Rate limiting (N requests per minute) | ✅ Yes | Occasional over-count is fine |
| Cache stampede prevention | ✅ Yes | Duplicate cache population wastes CPU, not data |
| Deduplication of idempotent jobs | ✅ Yes | Double-execution is a no-op |
| Metadata catalog write coordination | ❌ No | Silent data corruption on split-brain |
| Leader election for active-standby failover | ❌ No | False positive = two active nodes writing |
| Any write where last-writer-wins is wrong | ❌ No | No fencing token = no rejection of stale writes |

---

### Redlock vs etcd: Summary

| Property | Redlock | etcd |
|----------|---------|------|
| **Correctness model** | Probabilistic (bounded timing assumed) | Deterministic (Raft consensus) |
| **Fencing token** | ❌ None | ✅ `ModRevision` — monotonic, per-key |
| **GC pause safety** | ❌ Unsafe — stale holder can write after expiry | ✅ Safe — stale write rejected by revision |
| **Clock skew sensitivity** | ❌ Quorum correctness depends on bounded drift across nodes | ✅ Lease expiry is Raft-committed — clock skew between etcd nodes cannot create two concurrent holders; TTL only affects liveness of the holder |
| **Herd effect on release** | ❌ All N nodes notified, all clients retry | ✅ Watch stream — ordered notification |
| **Operational complexity** | 5 independent Redis nodes (no consensus) | 3-node Raft cluster |
| **Appropriate for Gravitino** | ❌ No | ✅ Yes (active-standby HA) |

**The one-sentence verdict**: Redlock trades correctness guarantees for operational simplicity and speed; etcd trades neither — it uses consensus to give you real correctness at the cost of one more HA cluster to operate. For a metadata catalog where split-brain writes cause data corruption, Redlock is the wrong tool regardless of how well-tuned the TTLs are.

---

## Quick Reference: Race Pattern Coverage

| Race Pattern | ZooKeeper closes it? | etcd closes it? | Condition |
|---|---|---|---|
| TOCTOU | ✅ Yes | ✅ Yes | Lock held across full critical section |
| Split-brain (partition) | ✅ Yes | ✅ Yes | Both require quorum for lock grant |
| Split-brain (GC pause / stale holder) | ⚠️ Advisory only | ⚠️ Advisory only | Requires end-to-end fencing token enforcement |
| Stale read | ⚠️ Requires `sync()` | ✅ Linearizable by default | ZK follower reads are stale without explicit sync |
| Lost update | ✅ CAS via version | ✅ CAS via `ModRevision` txn | Both support conditional writes |
| Watch race (notification ordering) | ⚠️ One-shot, fires after event | ✅ Streaming, ordered | ZK watches require re-read after firing |
