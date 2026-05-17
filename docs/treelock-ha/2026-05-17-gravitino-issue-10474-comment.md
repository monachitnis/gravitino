# Gravitino HA Locking — Proposal Comment for Issue #10474

> Ready to post as a comment on https://github.com/apache/gravitino/issues/10474

---

## A DB-Native Approach: Three Targeted Fixes + Phased Topology for HA cluster

The prior proposals treat TreeLock replacement as the solution. Different position: **TreeLock is correct for intra-node concurrency — the HA correctness gaps are three specific DB-layer omissions.**

```
TODAY (single-node correct; HA broken)
  Node A                         Node B
  REST → TreeLock → MetaService  REST → TreeLock → MetaService
           (in-memory)                    (in-memory)
                  ↓                              ↓
              Same DB ←──────── no cross-node visibility ────────┘

PROPOSED (DB layer is the correctness authority)
  Node A                              Node B
  REST → TreeLock (unchanged)         REST → TreeLock (unchanged)
              ↓                                   ↓
         MetaService                         MetaService
              ↓                                   ↓
         BEGIN TXN ◄── SELECT parent FOR UPDATE ──► BEGIN TXN
              ↓         (held by DB conn to COMMIT)      ↓
         INSERT child ──────── one wins, one blocks ── INSERT child
              ↓
      entity_change_log ← written on every mutation
              ↓                    ↓
          Node A poller        Node B poller   (independent cursors)
              ↓                    ↓
      GravitinoCache.invalidate()   GravitinoCache.invalidate()

  etcd: leader election only — zero overhead per write
    lease → CreateRevision epoch → stored in DB → rejects stale-epoch writes
```

---

## Confirmed Race Conditions

| | Race | Scenario | Root cause |
|---|---|---|---|
| Race 1 | TOCTOU rename | Node A and Node B both check a name doesn't exist, both proceed — one rename silently wins or both succeed with duplicate names | No cross-node lock on parent namespace during check-then-write |
| Race 3 | Orphaned child | `DROP SCHEMA` on Node A commits; Node B's `CREATE TABLE` under that schema also commits — `table_meta` row exists with a soft-deleted `schema_id` | No FK between `table_meta` and `schema_meta`; soft-delete + manual ID joins means the INSERT sees no constraint violation |
| ABA | Lost update | Metalake at V1 → updated to V2 → reverted to V1-equivalent content; stale writer at V1 passes the `UPDATE WHERE old_value` OCC guard undetected | `POConverters` copies `lastVersion` to `nextVersion` without incrementing — version never advances, ABA cycle invisible to OCC |

---

## Three Targeted Fixes

| Fix | Race closed | Location |
|-----|-------------|----------|
| `SELECT ... FOR UPDATE` on parent row, within `SessionUtils.doWithCommit` | Race 1 (TOCTOU rename), Race 3 (orphaned child on concurrent drop+create) | All `*MetaService` structural ops |
| `nextVersion = lastVersion + 1` | ABA: `nextVersion = lastVersion` means V1→V2→V1 passes OCC guard undetected | `POConverters.updateMetalakePOWithVersion` |
| Wire `EntityChangeLogPoller` in `GravitinoEnv` | Cross-node cache staleness (write side in PR #10914; consumer not yet wired) | New `EntityChangeLogPoller`, `GravitinoEnv` |

**TreeLock unchanged.** DB row lock handles cross-node serialization; TreeLock handles intra-node. Complementary, not redundant.

---

## Why `SELECT FOR UPDATE`, Not etcd Per-Write

The DB row lock is held by the DB connection through `COMMIT` — immune to the failure modes that motivate external coordinators:

- **k8s CPU throttle**: throttled JVM thread cannot release a DB connection lock; lease keepalives can miss their window while the process is alive
- **GPU→CPU PCIe offload stalls**: AI inference deployments offload host memory over PCIe, stalling CPU threads; DB lock unaffected by JVM scheduling
- **etcd per-write cost**: 4 hierarchy levels × 2–5ms RTT = 8–20ms overhead per write, always paid, even uncontested — before any DB work

etcd per-write locking is only correct if a fencing token propagates to every DB write (`UPDATE WHERE fencing_token = N`). At that point etcd is redundant with `SELECT FOR UPDATE`.

---

## Agentic Workload Fit: OCC Preserves Parallelism; Distributed Locks Destroy It

Gravitino increasingly serves multi-agent AI systems: LLM agents issuing concurrent DDL via tool calls, training pipelines registering model versions in parallel via `ModelMetaService`, inference servers reading schema metadata at serving time.

**OCC is optimistic — all agents run in parallel, conflicts detected only at commit:**
```
Agent 1 ──read──write──commit✓
Agent 2 ──read──write──commit✓
Agent 3 ──read──write──commit✓  ← all in-flight simultaneously
Agent 4 ──read──write──conflict→retry→commit✓
```
Only the conflicting agent serializes, and only for one retry. Uncontested agents are never delayed. LLM agent tool-call loops have retry-with-backoff built in by design — a 409 Conflict is just another retryable response, requiring no special handling.

**Distributed locks are pessimistic — agents queue at lock acquisition before any work begins:**
```
Agent 1 ──acquire lock──write──release  ← others blocked here
Agent 2           ──────────────────────acquire lock──write──release
Agent 3                                            ──────────────────acquire lock──...
```
Even agents operating on completely unrelated entities wait in the same queue. A 10-agent model registration storm: ~50ms parallel under OCC vs. 10 × (lock RTT + write) ≈ 300ms+ serialized under etcd per-write — before contention is even factored in. Lock-free reads are essential for inference-time `loadTable` latency; acquiring an etcd READ lock per metadata lookup is a non-starter.

The 2026 field consensus (Iceberg REST v1.6+, Delta Lake Unity Catalog, Kafka KRaft) converges here: push serialization into the storage layer via OCC; reserve external coordinators for coarse-grained topology only. Agentic workloads make this constraint harder, not softer.

---

## Tradeoffs

| Tradeoff | This proposal's choice | Rejected alternative |
|---|---|---|
| Serialization mechanism | DB row lock (no new dep, ~5ms DDL txn) | etcd/ZK per-write (8–20ms, external cluster) |
| Cache consistency | Eventual (poll interval, ~500ms), DB always authoritative | Sync cross-node ack (adds coordination channel, latency on every write) |
| HA topology | Active-standby first (Phase C) | Active-active now (Phase A's `FOR UPDATE` is already the full story; test gap not justified until write throughput measured) |
| Race 3 closure | `SELECT FOR UPDATE` on parent (no schema change) | FK `ON DELETE CASCADE` (correct long-term; requires migration on MySQL/PG/H2 + soft-delete semantics change) |

---

## Evolution Path

```
Phase A  DB correctness, zero new deps
  ├─ SELECT FOR UPDATE on parent rows (all MetaServices)
  └─ Version increment fix in POConverters
  ✓ Race 1, Race 3, ABA closed  ✗ cache staleness  ✗ auto-failover

Phase B  Cross-node cache coherence
  └─ EntityChangeLogPoller wired, gravitino.cache.entityChangeLog.enabled
  ✓ + bounded-staleness invalidation  ✗ auto-failover

Phase C  Active-standby HA  [first external dep]
  └─ etcd leader election, controller_epoch in DB, standby serves reads
  ✓ + automatic failover within lease TTL  new dep: etcd (3-node)

Phase D  Active-active  [no new locking work — Phase A is complete]
  └─ client retry semantics, per-catalog conflict testing, throughput benchmarks
  when: single-node write ceiling measured, not speculative

Phase E  Zero-dependency HA
  └─ embedded Raft (Apache Ratis, already in ASF/Ozone ecosystem)
  when: air-gapped / edge deployments require it
```

Each phase is independently shippable. No phase invalidates the previous.

---

## Why Not the Other Proposals

| Proposal | Gap |
|---|---|
| Option A (etcd/ZK per-write) | Conflates topology management with per-write locking; 8–20ms overhead per DDL; only correct if fencing token propagated to every DB write — at which point etcd is redundant with `FOR UPDATE` |
| Option B (delete TreeLock + OCC) | Correct direction; Race 3 unaddressed without parent `FOR UPDATE`; removing TreeLock costs correct intra-node serialization for zero gain |
| Option C (partition + watchdog + trigger) | `tree_version` alone is insufficient fencing token — needs `(tree_version, owner_node)` compound; `System.exit()` watchdog has high blast radius on transient DB hiccup; `FOR UPDATE` provides same storage-layer fencing without trigger, watchdog, or rebalancing machinery |
| Counter-proposal (new lease table) | `FOR UPDATE` on the existing parent row is strictly simpler — no schema migration, no lease renewal thread, and simultaneously closes Race 3 which the lease table does not |
| PR #11020 (LockBackend SPI) | Correct abstraction for Phase C/E; deferred Phase B (OCC in RelationalEntityStore) is what this proposal delivers directly |

---

## Agentic Contribution

This proposal was developed with an agentic coding workflow. A **`gravitino-locking-reviewer` Claude Code skill** is included on the fork branch:

```
.claude/skills/gravitino-locking-reviewer/SKILL.md
```

It encodes Gravitino's distributed state discipline as an invocable code review checklist covering TreeLock patterns, `SELECT FOR UPDATE` placement, OCC version correctness, `EntityChangeLog` invalidation scope, and leader election epoch fencing. Contributors working in this area can invoke it to verify correctness before review, not just read the docs passively.

---

## Validation Pipeline: CI → Playground Staging → Managed Rollout

For revenue-sensitive deployments, correctness fixes must ship with staged confidence — not just passing CI.

**Tier 1 — Hermetic CI** (`HaBaseIT` + `MiniGravitino` + TestContainers):
Two in-process Gravitino nodes, shared PostgreSQL, 6 correctness scenarios. Runs on every PR touching `MetaService`, `POConverters`, `EntityChangeLog`, or locking code. Fast, no external deps.

**Tier 2 — Playground staging** (full data plane fidelity):
[gravitino-playground](https://github.com/apache/gravitino-playground) ships Hive, MySQL, PostgreSQL, Spark, Trino, Prometheus, and Grafana as a single `docker compose up`. A thin overlay converts it into a 2-node HA cluster:

```bash
docker compose \
  -f gravitino-playground/docker-compose.yaml \
  -f docs/treelock-ha/docker/docker-compose-ha-overlay.yml up
```

The overlay (`docker-compose-ha-overlay.yml`) adds only `gravitino-node-b` (same image, shared entity store) and a single-node `etcd`. All catalog backends — Hive metastore, Iceberg REST, MySQL, PostgreSQL — are shared between both nodes. This is the exact topology that surfaces Race 1 and Race 3. No new infra to stand up.

**Interactive beta validation via Jupyter**: Playground's existing `gravitino-spark-trino-example.ipynb` is extended with HA race scenario cells — concurrent Python REST calls racing DDL across both nodes. `gravitino_llamaIndex_demo.ipynb` (already in playground) directly exercises the AI agent model-registration storm (Scenario 5). OSS contributors and beta customers run these before a release cut.

**Observability without new infra**: Playground's Prometheus + Grafana are already wired. Three HA-specific metrics to add as a dashboard:

| Metric | Signal |
|---|---|
| OCC retry rate | Spikes after Phase A ships → confirms `FOR UPDATE` is serializing correctly |
| EntityChangeLog poll lag (`maxId - lastConsumedId`) | Confirms Phase B consumer is keeping up; alerts if falling behind |
| Structural op P99 latency | Baseline before / after `FOR UPDATE` — quantifies the ~5ms DDL overhead claimed in the tradeoff table |

**Staged rollout for managed deployments**:

```
Phase A  Drop-in correctness fix — zero topology change, safe for all existing deployments
Phase B  Opt-in: gravitino.cache.entityChangeLog.enabled=false default
         → dark-launch in staging, flip per tenant when validated
Phase C  New dep (etcd): validate in playground overlay before enabling in production cluster
         Rollback path: disable etcd → falls back to Phase B behavior, single active writer
         Feature flag per deployment; no flag removal until Phase C is stable across customer base
```

Phase A ships with no rollout risk. Phases B and C are gated behind feature flags with explicit rollback paths — the pattern appropriate for production systems with SLA obligations.

---

## Open Questions

1. **DB isolation level**: `FOR UPDATE` on `READ COMMITTED` (PG default) and `REPEATABLE READ` (MySQL/InnoDB default) both read current committed rows for locked queries — should be documented explicitly per supported DB.
2. **Poll interval SLA**: 500ms cross-node cache staleness acceptable for DDL workloads; inference-time reads may need tuning — make configurable.
3. **POConverters audit scope**: `updateMetalakePOWithVersion` confirmed. Are there other `update*POWithVersion` methods where version intentionally does not increment?
4. **Active-active threshold**: Phase D should be data-driven. What write throughput triggers the single-node ceiling?
5. **Phase E dependency**: Is zero-external-dependency HA a roadmap requirement? Apache Ratis is available in-ecosystem if yes.

---

## Full Spec on Fork

[`monachitnis/gravitino:docs/locking-deep-dive-and-ha-design`](https://github.com/monachitnis/gravitino/tree/docs/locking-deep-dive-and-ha-design)

| Document | Contents |
|---|---|
| `docs/treelock-ha/2026-05-17-gravitino-ha-locking-proposal-final.md` | Full design: confirmed bugs, prior proposal verdicts, 2026 ecosystem lens, architecture diagrams, migration phases |
| `docs/treelock-ha/2026-05-17-gravitino-ha-implementation-spec.md` | Implementation contracts: mapper interfaces, version increment audit, `EntityChangeLogPoller` SPI, `GravitinoLeaderElection` etcd SPI |
| `docs/treelock-ha/2026-05-17-gravitino-ha-test-harness.md` | Integration test spec: 6 correctness scenarios across Hive/Iceberg/Kafka/JDBC/Paimon, `HaBaseIT` base class |
| `docs/treelock-ha/docker/docker-compose-ha-test.yml` | Standalone HA compose: 2 Gravitino nodes, shared PostgreSQL, etcd, all catalog backends |
| `docs/treelock-ha/docker/docker-compose-ha-overlay.yml` | Playground overlay: adds `gravitino-node-b` + `etcd` on top of existing playground stack |
| `.claude/skills/gravitino-locking-reviewer/SKILL.md` | Claude Code skill: invocable distributed state review checklist |
