# Gravitino HA Correctness: A DB-Native Proposal

**Issue**: #10474  
**Author**: Mona Chitnis  
**Date**: 2026-05-17  
**Branch**: [monachitnis/gravitino — docs/locking-deep-dive-and-ha-design](https://github.com/monachitnis/gravitino/tree/docs/locking-deep-dive-and-ha-design)

---

## 1. Problem Statement

### What TreeLock guarantees today

`TreeLock` is a correct, zero-overhead single-JVM hierarchical read-write lock. It is built on `ReentrantReadWriteLock` (one per `TreeLockNode`) with a reference-counted in-memory tree rooted in `LockManager`. For a 4-level path `metalake/catalog/schema/table`, it acquires READ on the three ancestors and READ/WRITE on the leaf — the standard intent-locking pattern from database theory.

**TreeLock is not the problem.** It is a correct intra-node concurrency primitive.

### What breaks in HA

In a multi-node deployment, each Gravitino server has its own independent `LockManager` with its own in-memory lock tree. Two nodes can simultaneously acquire a WRITE lock on the same path with no awareness of each other:

```
Node A (LockManager A):                 Node B (LockManager B):
  /ml/catalog/schema/table1               /ml/catalog/schema/table1
  WRITE lock acquired ✓                   WRITE lock acquired ✓   ← split-brain
  writes table metadata                   writes conflicting metadata
  both go to the same shared DB           last-writer-wins, silently
```

### Three confirmed correctness gaps

**Gap 1 — Structural op serialization (Race 3: orphaned child)**

`DROP SCHEMA` on Node A and `CREATE TABLE` under that schema on Node B can both succeed, leaving a `table_meta` row with a `schema_id` pointing to a soft-deleted schema. Confirmed: there are no FK constraints between `table_meta` and `schema_meta` in `schema-1.3.0-postgresql.sql`. The INSERT succeeds. OCC cannot detect this — from Node B's perspective, no conflict exists.

**Gap 2 — OCC completeness (ABA vulnerability)**

`POConverters.updateMetalakePOWithVersion` (line 141):
```java
Long nextVersion = lastVersion;  // BUG: copies, does not increment
```
The OCC discriminant is a full old-value equality check. If metadata returns to a previously-seen state (V1 → V2 → V1), a stale writer at V1 passes the check undetected. Every 2026 lakehouse catalog (Iceberg sequence numbers, Delta Lake `txnVersion`) increments monotonically on every write for exactly this reason.

**Gap 3 — Cross-node cache coherence**

`EntityChangeLog` write side is implemented and wired in all meta services (merged PR #10914). The consumer — the poller that reads from the log and calls `GravitinoCache.invalidate()` — is not yet implemented. Cross-node cache invalidation is write-only. Node A's cache is never invalidated by Node B's mutations.

---

## 2. Prior Proposals — What Each Gets Right and Wrong

| Proposal | Verdict | What it closes | What it misses |
|---|---|---|---|
| **Option A** (etcd/ZK per-write) | ⚠️ Wrong granularity | Cross-node mutual exclusion | Conflates topology management with per-write locking. 4–5 RTTs per write (one per hierarchy level) = 8–25ms overhead before any DB work. Serializes parallel AI agents. Fencing token must still be propagated to DB layer — at which point etcd is redundant with `SELECT FOR UPDATE`. |
| **Option B** (delete TreeLock + OCC) | ⚠️ Correct direction, incomplete | Latency: lock-free reads | Orphaned child race (Gap 1) unaddressed. Deleting TreeLock removes correct intra-node serialization with no benefit. |
| **Option C** (partition + watchdog + trigger) | ⚠️ Strongest fencing story, identity gap | DB trigger fencing is the right enforcement point | `tree_version` alone is an insufficient fencing token — any node that reads the current `tree_version` can present it. Needs `(tree_version, owner_node)` compound check. `System.exit()` watchdog has high blast radius: a transient DB hiccup kills a healthy node. Cross-partition ops unspecified. |
| **Counter-proposal** (new lease table) | ⚠️ Right instinct, unnecessary table | Structural op cross-node fence | New `structural_lease` table is unnecessary. `SELECT FOR UPDATE` on the existing parent entity row provides identical serialization, simultaneously verifies parent existence, and requires zero schema changes. |
| **PR #11020** (LockBackend SPI) | ⚠️ Premature abstraction | Plugin extensibility | The correctness gaps are in the DB layer (missing `SELECT FOR UPDATE`, missing version increment), not in the lock layer. A SPI over backends doesn't fix either bug. Phase B (OCC in `RelationalEntityStore`) is deferred — this proposal delivers Phase B directly without an abstraction layer. |

---

## 3. 2026 Ecosystem Lens

### Storage-layer OCC is the settled pattern

The 2026 field consensus for metadata catalog HA:

**Iceberg REST Catalog v1.6+**: Catalog-level CAS using sequence numbers. Every table write increments `sequence-number`. Conflicting commits rejected at the catalog layer. No external coordinator.

**Delta Lake / Unity Catalog (GA Feb 2026)**: Catalog-mediated commits. Every write carries `txnVersion`. Storage layer rejects `txnVersion < current`. Raft-based active-standby for catalog service HA; per-write coordination handled by the catalog DB.

**Kafka KRaft (Kafka 4.0, March 2025)**: Removed ZooKeeper entirely. Lesson: if the metadata store is itself ordered and replicated, you already have consensus — an external coordinator is redundant. The `controller_epoch` in every metadata record is the fencing token; no per-write coordinator call needed.

**Kleppmann's fencing principle (2016, still correct)**: A lock manager disconnected from the storage layer provides only a hint. The fencing token must be checked at the storage layer. `SELECT FOR UPDATE` within a DB transaction IS the fencing — the DB connection holds the lock, not the JVM thread.

### Why the GC-pause argument is mostly historical in 2026

Kleppmann's motivating example was Java 7-era G1GC with multi-second stop-the-world pauses. Java 21 (LTS) with ZGC or Shenandoah gives sub-millisecond pause times regardless of heap size. The scenario requiring lease TTL < GC pause duration is no longer realistic on modern JVMs.

**The 2026 equivalent failure modes are:**

1. **k8s CPU cgroup throttling**: A pod with tight CPU limits exhibits stall-while-thread-is-alive behavior indistinguishable from a GC pause. An etcd keepalive can miss its window while the process is otherwise alive.

2. **GPU→CPU PCIe offload**: In AI inference deployments, GPU memory pressure causes offloading to host memory via PCIe. The CPU thread coordinating metadata reads via Gravitino stalls during the transfer — same "lock held, JVM thread unscheduled" scenario. This is increasingly common as GPU models grow and memory capacity lags.

`SELECT FOR UPDATE` within a DB transaction is immune to both: the DB connection holds the row lock, not the JVM thread. A throttled or stalled JVM does not release the DB lock until the transaction commits or the DB connection times out.

### Agentic federated metadata workloads

Modern Gravitino deployments serve multi-agent AI systems:
- LLM agents issuing concurrent DDL via tool calls (create table, register model version, evolve feature schema)
- Model training pipelines registering model versions concurrently via `ModelMetaService`
- Feature store operations creating/updating schemas at training time

OCC + retry is the natural pattern for agentic systems: agents already implement retry with exponential backoff. Per-write distributed locks (etcd/ZK) would serialize parallel agents at the lock acquisition layer, destroying the concurrency benefit of multi-agent architectures. Lock-free reads are essential for inference-time metadata lookup latency.

---

## 4. Recommended Solution

### Architecture

```
─────────────────────────────────────────────────────────────────
  SINGLE NODE (today — correct and optimal, no change needed)
─────────────────────────────────────────────────────────────────

  REST Request
      │
      ▼
  Dispatcher
      │
      ▼
  TreeLock (in-memory, nanoseconds)
  ┌──────────────────────────────────┐
  │ READ  /ml                        │
  │ READ  /ml/catalog                │
  │ READ  /ml/catalog/schema         │
  │ WRITE /ml/catalog/schema/table1  │  ← serializes concurrent intra-node requests
  └──────────────────────────────────┘
      │
      ▼
  MetaService → DB (ACID transaction)


─────────────────────────────────────────────────────────────────
  HA: AFTER THIS PROPOSAL (DB layer is the correctness authority)
─────────────────────────────────────────────────────────────────

  Node A                              Node B
  REST Request                        REST Request
      │                                   │
      ▼                                   ▼
  TreeLock (intra-node, unchanged)    TreeLock (intra-node, unchanged)
      │                                   │
      ▼                                   ▼
  MetaService                         MetaService
      │                                   │
      └──────────────┬────────────────────┘
                     │
                     ▼
         ┌───────────────────────┐
         │  BEGIN TRANSACTION    │
         │                       │
         │  SELECT parent_row    │
         │    FOR UPDATE    ◄────┼── serializes cross-node structural ops
         │  (verify not deleted) │    DB row lock held until COMMIT
         │                       │    immune to GC pause / CPU throttle /
         │  INSERT / UPDATE child│    GPU offload stall
         │                       │
         │  COMMIT               │
         └───────────────────────┘
                     │
                     ▼
         ┌───────────────────────┐
         │  entity_change_log    │  ← INSERT on every mutation
         └───────────────────────┘
                  │       │
              Node A    Node B
           EntityChangeLog poller (independent, in-memory cursor)
                  │       │
                  ▼       ▼
           GravitinoCache.invalidate()  ← cross-node cache coherence


  etcd (fires on failover ONLY — zero overhead per write):
  ┌─────────────────────────────────────────────────────┐
  │  Lease election → active node holds /gravitino/leader│
  │  CreateRevision = epoch (Raft-committed, monotonic)  │
  │  Epoch stored in DB on activation                    │
  │  Every structural write: WHERE controller_epoch = N  │
  │  Stale node resumes → epoch mismatch → DB rejects    │
  └─────────────────────────────────────────────────────┘
```

### Fix 1: Structural op parent lock

Add `SELECT ... FOR UPDATE` on the parent entity row within the same DB transaction as every structural child operation. Uses the existing `SessionUtils` / MyBatis session infrastructure — no new dependencies, no schema changes.

**Operations covered**: `createTable`, `createSchema`, `createFileset`, `createTopic`, `createView`, `registerModel`, `dropSchema`, `dropCatalog`, `renameTable`, `renameSchema`

**Files**: `SchemaMetaService.java`, `TableMetaService.java`, `ModelMetaService.java`, `FilesetMetaService.java`, `TopicMetaService.java`, `ViewMetaService.java`

Two races closed:
- **Race 1 (TOCTOU rename)**: Check-then-write on new name has no gap because the parent row is locked through COMMIT
- **Race 3 (orphaned child)**: Concurrent `dropSchema` blocks until `createTable` commits, or vice versa. One sees the parent as deleted and throws `NoSuchSchemaException`. No orphaned rows.

### Fix 2: Version increment

**File**: `core/src/main/java/org/apache/gravitino/storage/relational/utils/POConverters.java` line 141

One-line change closes the ABA vulnerability and aligns with the pattern used by every 2026 lakehouse catalog:

```java
// Before (copies, does not increment):
Long nextVersion = lastVersion;

// After (monotonically increasing — closes ABA):
Long nextVersion = lastVersion + 1;
```

The same fix should be audited across all `update*POWithVersion` methods in `POConverters.java` to ensure consistent behavior.

### Fix 3: EntityChangeLog consumer

Wire the already-designed `EntityChangeLogMapper` into a background poller that drives `GravitinoCache` invalidation cross-node.

**New file**: `core/src/main/java/org/apache/gravitino/cache/EntityChangeLogPoller.java`  
**Modified**: `core/src/main/java/org/apache/gravitino/GravitinoEnv.java` (init + close)

Key design properties:
- `lastConsumedId` is in-memory, per-node, never persisted — no cross-node sync needed
- New node calls `selectMaxChangeId()` on init; cache starts empty, no history to replay
- Idempotent: re-consuming a row only calls `invalidate()` again — harmless
- Bounded staleness: cache coherence lag = poll interval (configurable, default suggested 500ms)

### Topology: Active-Standby via etcd

etcd handles **only** the HA topology question — which node is active. It fires on failover (minutes to hours apart), never on individual writes.

The active node's etcd `CreateRevision` (a Raft-committed, globally monotonic integer) is stored as `controller_epoch` in the entity store on activation. Every structural write includes `AND controller_epoch = #{currentEpoch}` in its WHERE clause. A stale node that resumes after a failover has an outdated epoch — its writes are rejected by the DB predicate without any coordinator round-trip.

This gives fencing token guarantee using the existing DB as the enforcement point, with no per-write overhead from etcd.

**Active-active (long-term)**: Fix 1 (`SELECT FOR UPDATE`) already provides the correctness foundation for multi-master. No additional coordination mechanism needed — the DB transaction IS the cross-node serializer.

---

## 5. Migration Phases

**Phase A — DB correctness (this proposal, no new dependencies)**
- Fix 1: `SELECT FOR UPDATE` on parent rows in all structural meta services
- Fix 2: Version increment in `POConverters.java`
- Closes Race 1, Race 3, ABA
- Single-node behavior unchanged; HA correctness established at DB layer

**Phase B — Cross-node cache coherence**
- Fix 3: Wire `EntityChangeLogPoller` in `GravitinoEnv`
- Bounded-staleness cache coherence across HA nodes
- Configurable poll interval; `pruneOldEntityChanges` for log retention

**Phase C — HA topology**
- Add etcd leader election with `CreateRevision` epoch propagated to DB writes
- Active-standby HA with zero per-write coordinator overhead
- Active-active: Phase A's `SELECT FOR UPDATE` is the complete locking story

---

## 6. How This Improves on Each Prior Proposal

**vs Option A (etcd/ZK per-write)**: This proposal uses etcd only for leader election (fires on failover), not for per-write locking. The per-write serialization is provided by `SELECT FOR UPDATE` inside the DB transaction — zero RTT overhead, immune to the same failure modes (CPU throttle, GPU offload stall) that would expire an etcd lease.

**vs Option C (partition + watchdog + trigger)**: This proposal provides the same DB-layer fencing without the operational complexity. The `SELECT FOR UPDATE` + DB transaction IS the trigger — no separate trigger mechanism, no watchdog threads, no partition routing, no `System.exit()` blast radius, no `(tree_version, owner_node)` identity gap.

**vs Counter-proposal (new lease table)**: `SELECT FOR UPDATE` on the existing parent entity row is strictly simpler — no new table, no new schema migration, and simultaneously verifies parent existence (closes Race 3) which the lease table approach does not.

**vs PR #11020 (LockBackend SPI)**: The SPI is a useful extensibility mechanism for future backends. This proposal delivers the correctness fixes that PR #11020's Phase B defers, directly in the DB layer where they belong. The SPI and this proposal are complementary — the SPI can wrap the fixed behavior.

---


## 7. Open Questions for Community

1. **DB isolation level assumption**: `SELECT FOR UPDATE` on a `deleted_at = 0` predicate relies on the DB seeing committed state at lock time. PostgreSQL `READ COMMITTED` (default) provides this. MySQL `REPEATABLE READ` (default) reads a snapshot — a `FOR UPDATE` on MySQL reads the current committed version even in RR mode, so behavior is correct on both. Worth explicitly documenting the supported isolation levels.

2. **GPU offload TTL calibration**: For inference deployments where PCIe offload stalls are measured in seconds, etcd lease TTL should be set ≥ 30s (well above typical offload stall duration). What is the acceptable failover detection latency for Gravitino operators?

3. **EntityChangeLog poll interval vs DDL latency SLA**: A 500ms poll interval means up to 500ms of cache staleness after a cross-node mutation. For inference-time metadata reads (schema discovery, table listing), is 500ms acceptable, or should the interval be shorter (e.g., 100ms) with correspondingly higher DB polling load?

4. **Version increment audit scope**: The ABA fix applies to `updateMetalakePOWithVersion`. All `update*POWithVersion` methods in `POConverters.java` should be audited for the same pattern. Are there any intentional cases where version should not increment?

5. **Soft delete vs hard delete for structural coherence**: Race 3 is a consequence of soft delete + no FK constraints. Long-term, adding FK constraints with `ON DELETE CASCADE` would make hard delete safe and eliminate the need for `SELECT FOR UPDATE` on parent rows for drop operations. Is this in scope?

---

## References

- Full codebase analysis: `docs/treelog-ha/2026-05-17-gravitino-locking-deep-dive.md`
- Initial option analysis: `docs/treelog-ha/2026-05-17-gravitino-ha-locking-design.md`
- Implementation specification: `docs/treelog-ha/2026-05-17-gravitino-ha-implementation-spec.md`
- Test harness: `docs/treelog-ha/2026-05-17-gravitino-ha-test-harness.md`
- DDIA: Chapters 8–9 (Kleppmann, O'Reilly 2017)
