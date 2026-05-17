# Gravitino HA Locking — Proposal Comment for Issue #10474

> Ready to post as a comment on https://github.com/apache/gravitino/issues/10474

---

## A DB-Native Approach: Three Targeted Fixes, Phased approach to external HA cluster

The prior proposals (etcd/ZooKeeper distributed lock, remove TreeLock + OCC, partition ownership + watchdog, DB lease table, LockBackend SPI) each identify a real problem but treat TreeLock replacement as the solution. This proposal takes a different position: **if the DB already provides ACID transactions and OCC, TreeLock is a correct single-server in-memory optimization — the HA correctness gaps are three specific DB-layer omissions**.

```
─────────────────────────────────────────────────────────────────
  TODAY (single-node correct; HA broken)
─────────────────────────────────────────────────────────────────

  Node A                         Node B
  REST → TreeLock → MetaService  REST → TreeLock → MetaService
           (in-memory)                    (in-memory)
                  ↓                              ↓
              Same DB ←──────── no cross-node visibility ────────┘

  Each node's TreeLock tree is independent.
  Concurrent structural ops on different nodes have no shared fence.

─────────────────────────────────────────────────────────────────
  PROPOSED (DB layer is the correctness authority)
─────────────────────────────────────────────────────────────────

  Node A                              Node B
  REST → TreeLock (unchanged)         REST → TreeLock (unchanged)
              ↓                                   ↓
         MetaService                         MetaService
              ↓                                   ↓
         BEGIN TXN ◄──── SELECT parent FOR UPDATE ────► BEGIN TXN
              ↓             (DB row lock, held to COMMIT)      ↓
         INSERT child ──────────────────────────── INSERT child
              ↓                                        ↓
           COMMIT ───────── one wins, one blocks ─── COMMIT
              ↓
      entity_change_log ←── written on every mutation
              ↓                    ↓
          Node A poller        Node B poller   (independent cursors)
              ↓                    ↓
      GravitinoCache           GravitinoCache
       .invalidate()            .invalidate()

  etcd (active-standby only — zero overhead per write):
    lease election → CreateRevision epoch → stored in DB
    stale node resumes → epoch mismatch → DB rejects write
```

---

## Three Targeted Fixes

| Fix | What it closes | File(s) | Scope |
|-----|---------------|---------|-------|
| `SELECT FOR UPDATE` on parent entity row within structural op DB transaction | Race 1 (TOCTOU rename), Race 3 (orphaned child from concurrent drop+create) | `SchemaMetaService`, `TableMetaService`, `ModelMetaService`, `FilesetMetaService`, `TopicMetaService`, `ViewMetaService` | Add one `selectParentForUpdate` mapper call before each child INSERT/structural UPDATE |
| Version increment in `POConverters.updateMetalakePOWithVersion` line 141 | ABA vulnerability in OCC (`nextVersion = lastVersion` copies without incrementing; V1→V2→V1 cycle undetected) | `POConverters.java:141` | `nextVersion = lastVersion` → `nextVersion = lastVersion + 1` |
| Wire `EntityChangeLog` consumer (`EntityChangeLogPoller`) | Cross-node cache staleness (write side exists in PR #10914; consumer not yet implemented; cache invalidation currently write-only) | New `EntityChangeLogPoller.java`, `GravitinoEnv.java` | Background poll thread: `selectEntityChanges(lastConsumedId, N)` → `GravitinoCache.invalidate()` |

**TreeLock is unchanged.** It is correct for intra-node concurrency and zero-overhead. The DB transaction provides cross-node serialization for structural operations; TreeLock provides intra-node serialization. The two layers are complementary, not redundant.

---

## Why `SELECT FOR UPDATE` Is Sufficient (No External Coordinator)

The DB row lock acquired by `SELECT ... FOR UPDATE` is held by the DB connection through `COMMIT`, not by the JVM thread. This matters because the failure modes that motivate external coordinators — the scenarios where a lock-holding process stalls — do not affect a DB connection lock:

- **k8s CPU cgroup throttling**: A throttled JVM thread cannot release a DB connection lock. The DB holds it until the transaction commits or the connection times out — well beyond any DDL operation duration. This is the dominant 2026 equivalent of Kleppmann's GC-pause scenario: containers routinely run with tight CPU limits, and lease keepalives can miss their window while the process is otherwise alive.
- **GPU→CPU PCIe offload stalls**: In AI inference deployments, GPU memory pressure causes host memory offloads over PCIe, stalling CPU threads coordinating metadata reads via Gravitino. DB connection lock is unaffected by the JVM thread's scheduling state.
- **GC pauses**: Java 21 ZGC/Shenandoah gives sub-millisecond pause times — Kleppmann's motivating 30-second G1GC pause is largely historical on modern JVMs. `SELECT FOR UPDATE` is immune regardless.

Contrast with etcd/ZooKeeper per-write locking: a JVM stall can expire the lease. The storage layer has no mechanism to reject the stale write unless a fencing token is propagated to every DB write — at which point the external coordinator is doing no work that `SELECT FOR UPDATE` doesn't already do.

---

## Tradeoffs Considered

This section documents the tradeoffs evaluated and why this proposal lands where it does, so the community can challenge the assumptions explicitly.

### Tradeoff 1: DB lock contention vs. external coordinator overhead

**`SELECT FOR UPDATE` cost**: Structural operations (createTable, dropSchema, renameTable) are DDL — rare relative to reads. The parent row lock is held for the duration of a single DB transaction (typically < 5ms for a metadata write). Under high DDL concurrency, waiting threads queue at the DB row lock, which is handled by the DB's lock manager with known, bounded wait semantics. This is the same mechanism used by every relational system for concurrent writes.

**External coordinator cost**: etcd or ZooKeeper per-write locking requires one network round-trip per hierarchy level before any DB work begins. For a 4-level path: 4 × 2–5ms = 8–20ms overhead per write, plus keepalive background traffic, plus the coordinator cluster's own HA. This overhead is always paid — even on uncontested writes, even for reads that happen to acquire a READ lock.

**Conclusion**: For Gravitino's DDL-sparse, read-heavy, metadata workload, `SELECT FOR UPDATE` contention is negligible and the coordinator overhead is not. The tradeoff favors DB-native locking unless write contention on individual parent rows becomes measurable — which is a data-driven threshold, not a speculative one.

### Tradeoff 2: Eventual consistency for cache vs. strong read consistency

**EntityChangeLog poll model**: Cross-node cache coherence is eventually consistent with bounded staleness equal to the poll interval (suggested default: 500ms). A mutation on Node B is invisible to Node A's cache for up to one poll cycle.

**What this means in practice**: A client hitting Node A immediately after Node B drops a table may receive a cached hit. The next request will miss. This is acceptable for metadata catalog workloads where DDL is rare and clients tolerate brief inconsistency windows. It is not acceptable for use cases requiring read-your-writes consistency across nodes.

**The safety net**: The DB is always authoritative. A stale cache hit that leads to a write attempt will fail the OCC check (`UPDATE WHERE current_version = N` returns 0 rows). The system never silently corrupts — it surfaces conflicts as retryable errors.

**Alternative considered**: Synchronous cache invalidation (write to `entity_change_log` and block until all nodes acknowledge). Rejected: requires a coordination channel between nodes, defeats the purpose of the broadcast log, and adds latency to every write.

### Tradeoff 3: Active-standby simplicity vs. active-active throughput

**Active-standby (this proposal, Phase C)**: One node handles all writes at a time. etcd lease election determines which node is active. Failover takes one lease TTL (suggested 10–15s). Throughput ceiling: single node's write capacity.

**Active-active (future, enabled by Phase A)**: Multiple nodes handle writes concurrently. `SELECT FOR UPDATE` provides cross-node serialization per structural operation. No leader election needed. Throughput scales with node count for non-conflicting operations. Conflicting operations (concurrent writes to the same parent) serialize at the DB row lock — same as any multi-writer RDBMS workload.

**Why active-standby now**: The three correctness fixes (Phase A + B) are independent of topology. Active-standby adds etcd for failover detection without changing the per-write code path. Active-active requires no additional locking work — `SELECT FOR UPDATE` is already the complete story — but requires careful testing of conflict scenarios across all catalog types and client-visible retry semantics. Phasing allows correctness to be established before concurrency is increased.

### Tradeoff 4: Soft delete + no FK vs. hard delete + FK cascade

Race 3 (orphaned child rows) is a consequence of two design choices: soft delete (`deleted_at` timestamp) and no FK constraints between entity tables. `SELECT FOR UPDATE` on the parent row closes the race within this design.

**Long-term alternative**: Add `FOREIGN KEY (schema_id) REFERENCES schema_meta(schema_id) ON DELETE CASCADE` to `table_meta` (and equivalent for all child tables). Hard deletes would then automatically cascade. This eliminates Race 3 at the schema level rather than at the application lock level — and removes the need for `SELECT FOR UPDATE` on drop operations specifically.

**Why not now**: FK constraints require schema migrations on all supported databases (MySQL, PostgreSQL, H2) and change the semantics of soft-delete-based audit trails. This is a larger, separate change. `SELECT FOR UPDATE` closes the race correctly within the existing design; FK constraints are a future hardening option.

---

## Evolution Path: From Today to Full Distributed Locking

The proposal is explicitly designed as a phased path. Each phase delivers independently shippable correctness improvements; no phase requires the next to be safe.

```
Phase A ──────────────────────────────────────────────────────────
  Deliverable: DB-layer correctness, no new runtime dependencies
  ┌─────────────────────────────────────────────────────────────┐
  │  Fix 1: SELECT FOR UPDATE on parent rows (all meta services)│
  │  Fix 2: Version increment in POConverters line 141          │
  └─────────────────────────────────────────────────────────────┘
  Correctness after Phase A:
  ✓ Race 1 (TOCTOU rename) — closed
  ✓ Race 3 (orphaned child) — closed
  ✓ ABA vulnerability — closed
  ✗ Cross-node cache staleness — still eventually consistent (OK for DDL)
  ✗ Active-standby failover — manual restart or load balancer required
  Topology: single-node only (same as today); HA still unsafe until Phase C

Phase B ──────────────────────────────────────────────────────────
  Deliverable: Cross-node cache coherence (bounded staleness)
  ┌─────────────────────────────────────────────────────────────┐
  │  Fix 3: EntityChangeLogPoller wired in GravitinoEnv         │
  │  New config: gravitino.cache.entityChangeLog.enabled        │
  └─────────────────────────────────────────────────────────────┘
  Correctness after Phase B:
  ✓ All Phase A guarantees
  ✓ Cross-node cache invalidation within poll interval
  ✗ Active-standby failover — still manual
  Topology: multi-node safe for reads; writes still need single active node

Phase C ──────────────────────────────────────────────────────────
  Deliverable: Active-standby HA with automatic failover
  ┌─────────────────────────────────────────────────────────────┐
  │  etcd leader election (GravitinoLeaderElection SPI)         │
  │  controller_epoch propagated to all structural writes       │
  │  Standby nodes serve reads; active node takes writes        │
  └─────────────────────────────────────────────────────────────┘
  Correctness after Phase C:
  ✓ All Phase A + B guarantees
  ✓ Automatic failover within etcd lease TTL (10–15s)
  ✓ Stale leader writes rejected by epoch mismatch at DB layer
  Topology: active-standby HA — production-ready
  New dependency: etcd cluster (3 nodes for production quorum)

Phase D (future — active-active, optional) ─────────────────────
  Deliverable: Multi-master write throughput
  ┌─────────────────────────────────────────────────────────────┐
  │  No new locking work required — Phase A's SELECT FOR UPDATE │
  │  is the complete cross-node serialization story             │
  │  Additions needed:                                          │
  │    - Client-visible retry semantics (409 Conflict → retry)  │
  │    - Per-catalog conflict testing across all data planes    │
  │    - Write throughput benchmarks under contention           │
  └─────────────────────────────────────────────────────────────┘
  When to pursue: when single active node's write throughput
  becomes a measured bottleneck, not a speculative one

Phase E (future — embedded coordination, optional) ──────────────
  Deliverable: Zero external runtime dependencies for HA
  ┌─────────────────────────────────────────────────────────────┐
  │  KafkaRaft-style embedded Raft in Gravitino nodes           │
  │  controller_epoch derived from embedded log offset          │
  │  Eliminates etcd operational dependency                     │
  └─────────────────────────────────────────────────────────────┘
  When to pursue: if Gravitino targets zero-dependency HA
  deployments (edge, air-gapped, or k8s-free environments)
  Complexity: significant — requires embedding a Raft library
  (e.g., Apache Ratis, which Ozone/HBase already use in ASF)
```

**The key property of this evolution path**: each phase increases correctness or availability guarantees without invalidating the previous phase's work. An operator can stop at Phase B (multi-node eventual consistency, single active writer) and have a safe, supportable system. Phase C adds operational complexity (etcd cluster) only when automatic failover is required. Phase D and E are optional scale and independence improvements.

---

## Why Not the Other Proposals

**Option A (etcd/ZooKeeper per-write)**: Conflates topology management with per-write locking. etcd and ZooKeeper are the right tools for Phase C (leader election, coarse-grained, per-failover). They are the wrong tools for per-write structural op serialization — that is Phase A's job, solved more cheaply and more safely by the DB's own row locking. Using etcd per-write also locks in external coordinator dependency before it's needed, skipping directly to the most operationally complex topology.

**Option B (delete TreeLock + OCC)**: Correct direction. Race 3 (orphaned child) is unaddressed without `SELECT FOR UPDATE`; this proposal completes Option B's intent. Deleting TreeLock removes correct intra-node serialization with zero benefit — TreeLock's in-memory operation has no cross-node visibility to break.

**Option C (partition + watchdog + trigger)**: The DB trigger fencing insight is correct and this proposal agrees — the storage layer must be the enforcement point. However: (1) `tree_version` alone is an insufficient fencing token; `(tree_version, owner_node)` compound identity is required or any node that reads the current version can forge a passing write; (2) `System.exit()` watchdog has high blast radius — a transient DB connectivity hiccup kills a healthy node; (3) `SELECT FOR UPDATE` provides the same storage-layer fencing within the DB transaction, without a separate trigger, watchdog, partition routing table, or rebalancing mechanism.

**Counter-proposal (new lease table)**: `SELECT FOR UPDATE` on the existing parent entity row is strictly simpler — no schema migration, no new table, no new background thread for lease renewal, and it simultaneously verifies parent existence (closing Race 3), which the lease table does not.

**PR #11020 (LockBackend SPI)**: The SPI abstraction becomes more valuable once Phase C (etcd) and Phase E (embedded Raft) are in scope — operators can select the appropriate backend for their topology.

---

## 2026 Agentic Workload Fit

Gravitino increasingly serves multi-agent AI systems: LLM agents issuing concurrent DDL via tool calls, model training pipelines registering versions concurrently via `ModelMetaService`, inference servers reading schema metadata at serving time. OCC + retry is the natural pattern — agents already implement retry with backoff. Per-write distributed locks (Phase A skipped in favor of etcd per-write) would serialize parallel agents at lock acquisition, destroying multi-agent concurrency. Lock-free reads are essential for inference-time metadata lookup latency.

The 2026 field consensus (Iceberg REST v1.6+, Delta Lake Unity Catalog GA Feb 2026, Kafka KafkaRaft March 2025) converges on the same conclusion: push correctness into the storage layer via OCC and version columns; reserve external coordinators for coarse-grained topology management only. This proposal follows that pattern exactly.

---

## Agentic Contribution: A Reusable Skill for Future Contributors

This proposal was developed using an agentic coding workflow — not just as a document but as a machine-readable knowledge artifact. As part of this contribution, a **`gravitino-locking-reviewer` Claude Code skill** has been added to the fork:

```
.claude/skills/gravitino-locking-reviewer/SKILL.md
```

The skill encodes Gravitino's distributed state management discipline into an **invocable code review checklist** for AI-assisted development. It covers:

- **TreeLock pattern verification** — correct identifier level and lock type for each operation class (create/drop/rename/read/alter), cross-schema rename lock promotion rules
- **DB-layer serialization review** — `SELECT FOR UPDATE` placement within `SessionUtils.doWithCommit`, parent existence re-check after lock, unique constraint presence
- **OCC version correctness** — monotonic increment check, ABA detection, conflict surfacing as `OccConflictException`
- **Cache coherence audit** — `EntityChangeLog` write coverage, `invalidateByPrefix` scope for parent-level deletes, dispatch table for all entity + operation combinations
- **Leader election and epoch fencing** — epoch monotonicity, storage-layer epoch check, split-brain window bounds

**What this enables in practice**: A contributor writing a new `MetaService` structural operation can invoke `/gravitino-locking-reviewer` and receive a structured analysis of whether the code upholds the five invariants before a human reviewer sees it. Reviewers can invoke the same skill to generate a checklist-grounded review, not a pattern-matched one.

This is an attempt to demonstrate what **AI-amplified OSS contribution** looks like in 2026: not just AI-generated text, but AI-invocable knowledge that makes the *next* contribution safer. The skill references the design docs on this fork branch; if the proposal lands and the docs move to the main repo, the skill can be updated in place.

---

## Open Questions for the Community

1. **DB isolation level**: `SELECT FOR UPDATE` with a `deleted_at = 0` predicate on PostgreSQL `READ COMMITTED` (default) is correct — the lock reads the current committed version. MySQL `REPEATABLE READ` (default) also reads current committed rows for `FOR UPDATE` queries (InnoDB exception to snapshot reads). Explicit documentation of supported isolation levels is recommended.

2. **Poll interval SLA**: A 500ms EntityChangeLog poll interval means up to 500ms cross-node cache staleness after a mutation. For inference-time metadata reads, is this acceptable? For training-time DDL it almost certainly is. Operators should be able to tune this.

3. **Version increment audit scope**: The ABA fix applies to `updateMetalakePOWithVersion`. All `update*POWithVersion` methods in `POConverters.java` should be audited. Are there any intentional cases where version should not increment?

4. **Active-active timing**: At what measured write throughput does the active-standby ceiling become a bottleneck? This should be a data point from benchmarking, not an assumption. Phase D should be deferred until that threshold is crossed.

5. **etcd vs embedded Raft (Phase E)**: For deployments that cannot take an etcd dependency (air-gapped, edge), Apache Ratis (already used by Ozone in the ASF ecosystem) is a viable embedded Raft option. Is zero-external-dependency HA a Gravitino roadmap requirement?

---

## Full Spec

Available on fork branch [`monachitnis/gravitino:docs/locking-deep-dive-and-ha-design`](https://github.com/monachitnis/gravitino/tree/docs/locking-deep-dive-and-ha-design):

| Document | Contents |
|----------|----------|
| `docs/treelock-ha/2026-05-17-gravitino-ha-locking-proposal-final.md` | Full design doc: confirmed bugs with line numbers, prior proposal verdict table, 2026 ecosystem lens, architecture diagrams, migration phases |
| `docs/treelock-ha/2026-05-17-gravitino-ha-implementation-spec.md` | Implementation contracts: `SELECT FOR UPDATE` mapper interfaces, version increment fix audit, `EntityChangeLogPoller` SPI, `GravitinoLeaderElection` etcd SPI |
| `docs/treelock-ha/2026-05-17-gravitino-ha-test-harness.md` | Integration test specification: 6 correctness scenarios across Hive/Iceberg/Kafka/JDBC/Paimon data planes, `HaBaseIT` base class contract |
| `docs/treelock-ha/docker/docker-compose-ha-test.yml` | Docker Compose for HA smoke tests: 2 Gravitino nodes, shared PostgreSQL, etcd, all catalog backends |
| `.claude/skills/gravitino-locking-reviewer/SKILL.md` | Claude Code skill: invocable code review checklist for distributed state management, TreeLock patterns, OCC, EntityChangeLog, and leader election |
