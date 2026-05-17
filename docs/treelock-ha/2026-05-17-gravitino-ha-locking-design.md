# Design Doc: Distributed Concurrency for Multi-Server Gravitino (Issue #10474)

**Status**: Draft — for community discussion. Options are presented for review - before final implementation.
**Issue**: https://github.com/apache/gravitino/issues/10474
**Date**: 2026-05-17

---

## 1. Problem Statement

### What TreeLock guarantees today

`TreeLock` (see `core/src/main/java/org/apache/gravitino/lock/`) implements hierarchical read-write locking within a single JVM. When a server handles a request to create a table in `metalake.catalog.sales`, it acquires:

```
/              READ
/metalake      READ
/metalake/catalog  READ
/metalake/catalog/sales  WRITE   ← exclusive on the schema
```

This guarantees that no concurrent thread on the same server can simultaneously drop `sales` or create another table in `sales`. The guarantee is absolute within one process.

### What breaks in HA

In a multi-server deployment, each Gravitino instance has its own `LockManager` with its own `treeLockRootNode` in JVM memory. Lock state is never shared across processes. Two servers can each acquire what they believe is an exclusive WRITE lock on the same resource — because each is only talking to its own lock tree.

**Concrete race: concurrent `createTable`**

```
Time  Node A                            Node B
  1   acquires WRITE on /m/c/sales     acquires WRITE on /m/c/sales
  2   calls catalog plugin → creates   calls catalog plugin → creates
      table1 in Hive Metastore         table1 in Hive Metastore
  3   store.put(tableEntity) → OK      store.put(tableEntity) → ??
```

At step 3, one node may succeed and the other may hit a unique constraint violation from the DB, get an `IOException("Failed to insert")`, and return a 500 to its API caller — even though the table was actually created in the underlying catalog. Gravitino's in-memory catalog cache on that node may then hold a stale view.

**Concrete race: drop-schema vs. create-table**

```
Time  Node A                            Node B
  1   acquires WRITE on /m/c/sales     acquires WRITE on /m/c/sales
      (intends to drop schema)          (intends to create table in schema)
  2   deletes schema from DB           checks schema exists → yes (not yet deleted)
  3   —                                creates table in catalog plugin
  4   —                                store.put(tableEntity) → FK violation (schema gone)
```

Node B's table creation succeeds in the underlying Hive Metastore but fails to persist in Gravitino's metadata store, leaving the two out of sync.

### Why the DB alone does not fully solve it

Gravitino's relational store already uses an optimistic concurrency control (OCC) pattern for **updates**: the UPDATE SQL for every major entity type includes a full old-value WHERE clause (see `CatalogMetaBaseSQLProvider.updateCatalogMeta`, lines 196–219):

```sql
UPDATE catalog_meta
SET    current_version = #{newMeta.currentVersion}, ...
WHERE  catalog_id      = #{oldMeta.catalogId}
AND    current_version = #{oldMeta.currentVersion}
AND    last_version    = #{oldMeta.lastVersion}
AND    ...
```

If two servers race to update the same entity, exactly one UPDATE will match; the other gets `updateResult == 0` → `IOException("Failed to update the entity")`.

This is real protection, but it covers only three of the five conflict types:

| Conflict type | DB behavior | Current handling |
|--------------|-------------|-----------------|
| Concurrent UPDATE on same entity | One wins, one gets `updateResult == 0` | IOException propagates as 500 |
| Concurrent INSERT (create) of same entity | DB unique constraint violation | JDBC exception → `AlreadyExistsException` (handled) |
| INSERT with missing parent (drop-create race) | FK violation or missing-parent check fails | IOException propagates as 500 |
| Cross-node cache stale read | No DB conflict at all | Wrong data returned silently |
| Concurrent catalog-plugin + store divergence | Store fails after catalog succeeds | Metadata inconsistency, no automatic rollback |

The last two are the hardest cases and are not addressed by DB constraints alone.

---

## 2. Existing Infrastructure Relevant to a Fix

Before designing a solution, it is worth surveying what is already in place.

### 2.1 Optimistic old-value UPDATE check

As described above, every `update*Meta` SQL method in the `*BaseSQLProvider` classes includes the full prior state in the WHERE clause. This is already partial OCC — it just lacks retry logic at the call site and does not cover inserts or catalog-plugin divergence.

### 2.2 `EntityChangeLog`

Several meta services (`SchemaMetaService`, `FilesetMetaService`, `ViewMetaService`, `TopicMetaService`, `ModelMetaService`) already write to an `EntityChangeLog` table on every mutation. The `selectEntityChanges` method exists for polling this log. This infrastructure was built to support cross-node cache invalidation — changes written by Node A are visible to Node B via the log.

### 2.3 `GravitinoCache` invalidation path

`GravitinoCache.CaffeineGravitinoCache` is designed to consume from the change log to invalidate stale cache entries on nodes that did not perform the write. The poller is not yet wired up in production, but the architecture is in place.

### What is still missing

- A distributed mutex (or equivalent) that spans multiple server processes.
- Retry logic at the Dispatcher or Manager layer for OCC conflicts.
- A running poller that reads `EntityChangeLog` and invalidates remote caches.
- Catalog-plugin rollback on store failure (or store-first ordering).

---

## 3. Option A: Replace TreeLock with a Distributed Lock

### Concept

Preserve the exact same hierarchical lock semantics — READ on ancestors, WRITE on target — but back each `TreeLockNode` with a distributed mutex held in an external coordinator (etcd, ZooKeeper, or Redis). From the caller's perspective (`doWithTreeLock`), the API is unchanged.

### Coordinator options

**etcd** — TTL-based leases with compare-and-swap (CAS). A lock is represented as a key under a well-known prefix (e.g. `/gravitino/locks/metalake/catalog/schema`). The leaseholder must renew before the TTL expires; if the node crashes, the lease expires and the lock is automatically released. Supports watch-based notification for waiting acquirers.

**ZooKeeper** — Ephemeral sequential znodes under a path encoding the `NameIdentifier` hierarchy. The standard ZK hierarchical lock recipe is well-documented. ZK's session timeout handles crashed-node cleanup. Gravitino may already depend on ZK in some deployments (e.g. alongside Kafka).

**Redis (Redlock)** — The Redlock algorithm acquires a lock across multiple independent Redis nodes using SETNX + TTL. Widely used but has known correctness concerns under clock skew and network partition (see the Antirez vs. Kleppmann debate). Not recommended for a correctness-critical metadata system.

### Code changes required

| Component | Change |
|-----------|--------|
| `TreeLockNode` | Replace `ReentrantReadWriteLock` with a distributed lock handle (etcd lease or ZK ephemeral node); `lock(LockType)` and `unlock(LockType)` become remote calls |
| `LockManager` | Bootstrap coordinator client at startup; translate `NameIdentifier` path to coordinator key prefix |
| `GravitinoEnv` | Configure and inject coordinator connection |
| Background threads | Deadlock checker may be replaced by coordinator TTL expiry; node cleaner logic simplifies (no need for local reference counting for eviction) |
| Tests | All existing lock tests need coordinator infrastructure (real or mocked) |

The `doWithTreeLock` / `doWithRootTreeLock` API in `TreeLockUtils` requires no change — callers are unaffected.

### Trade-offs

| Dimension | Assessment |
|-----------|-----------|
| Semantic correctness | Complete — identical guarantees to today's single-server behavior |
| Call-site changes | None — `TreeLockUtils` API unchanged |
| Write latency | Each lock acquisition requires network round trips to the coordinator. A 5-level lock chain (root → metalake → catalog → schema → table) means 5 sequential RTTs per operation. At 1 ms RTT, a `loadTable` call adds ~5 ms of lock overhead alone. |
| Read latency | Same 5-RTT overhead applies to read locks, which currently have near-zero cost in the JVM-local implementation |
| Partial failure | If the coordinator cluster is unreachable, **all** Gravitino metadata operations fail. The coordinator becomes a new single point of failure with stricter availability requirements than the existing DB. |
| New operational dependency | Requires deploying and operating an etcd or ZooKeeper cluster alongside Gravitino |
| Memory management | Local reference counting and node eviction logic can be simplified or removed |
| Lock promotion (cross-schema rename) | Translates directly — the catalog-level key is still a single lock acquisition |
| Migration | Backward-compatible if the lock interface is abstracted; a `LocalLockManager` and `DistributedLockManager` can coexist |

---

## 4. Option B: Remove TreeLock, Rely on Storage-Layer Consistency

### Concept

Delete the `lock/` package entirely. Replace the current fail-fast-on-conflict behavior with OCC-with-retry: attempt the operation, detect a DB-level conflict, and retry with a fresh read. Accept that concurrent reads may see read-committed data (not snapshot isolation).

### What already provides protection

The existing old-value UPDATE check (Section 2.1) already detects concurrent writes to the same entity. For Option B to be correct, this protection must be:

1. Extended to INSERT conflicts (currently these throw a raw JDBC exception; `ExceptionUtils.checkSQLException` must translate all unique constraint violations to `AlreadyExistsException`).
2. Extended to parent-existence validation within the same transaction as the child INSERT.
3. Wrapped in retry logic at the Dispatcher layer.

### Code changes required

**Deletions (~800 lines)**:
- `lock/LockType.java`
- `lock/TreeLock.java`
- `lock/TreeLockNode.java`
- `lock/TreeLockUtils.java`
- `lock/LockManager.java`
- All `doWithTreeLock(...)` and `doWithRootTreeLock(...)` call sites (approximately 60–80 occurrences across `CatalogManager`, `MetalakeManager`, `SchemaOperationDispatcher`, `TableOperationDispatcher`, `FilesetOperationDispatcher`, `TopicOperationDispatcher`, `ModelOperationDispatcher`, `ViewOperationDispatcher`)
- `LockManager` initialization in `GravitinoEnv`

**Additions**:
- `RetryUtils.withOccRetry(int maxAttempts, Callable)` — catches `OccConflictException` (new), sleeps with jitter, retries up to `maxAttempts`
- `OccConflictException` — wraps `updateResult == 0` and unique constraint violations, signals retryability
- Parent-existence guard in each `create*` operation — select-for-update on the parent row within the same transaction, or rely on DB FK constraints with appropriate error mapping

**Modifications**:
- `ExceptionUtils.checkSQLException` — ensure all relevant JDBC constraint violations map to Gravitino API exceptions
- Each `create*` / `update*` / `delete*` in meta services — throw `OccConflictException` instead of generic `IOException` on conflict

### The parent-child consistency gap (critical)

Today, `TableOperationDispatcher.createTable` (lines 173–185) acquires a WRITE lock on the schema before calling into the catalog plugin and before writing to the store. This prevents a concurrent `dropSchema` from interleaving.

Without the lock, this sequence is possible:

```
Time  Node A (dropSchema)              Node B (createTable in that schema)
  1   soft-deletes schema in DB        reads schema → exists
  2   —                                calls catalog plugin → creates table
  3   —                                store.put(tableEntity) → FK error (schema deleted)
```

The table now exists in the underlying system but not in Gravitino.

Mitigation options:
1. **FK cascade at DB level** — if the schema row is deleted, the table INSERT fails with a foreign key violation, which maps to a retryable `OccConflictException`. Node B retries, re-reads the schema, finds it gone, and returns `NoSuchSchemaException`. This is clean but requires FK constraints to be in place and enforced (verify for all supported DB engines).
2. **SELECT FOR UPDATE on parent** — each create operation opens a transaction, SELECTs the parent row with `FOR UPDATE`, verifies it is not soft-deleted, then INSERTs the child. This is explicit but adds a write lock per create — essentially moving the mutex into the DB transaction manager.

Both are implementable. Option 2 gives stronger guarantees at the cost of DB write lock overhead.

### Trade-offs

| Dimension | Assessment |
|-----------|-----------|
| Semantic correctness | Weaker than Option A for concurrent creates and drop-create races; correctness depends on careful per-operation DB transaction design |
| Call-site changes | Large — 60–80 `doWithTreeLock` call sites must be removed; retry wrappers added to write paths |
| Write latency | Zero lock overhead; latency is purely DB round-trip time |
| Read latency | Zero lock overhead; reads go straight to cache or DB |
| Partial failure | No new failure modes — only the DB, which is already a hard dependency |
| New operational dependency | None |
| Code reduction | ~800 lines of lock code deleted; codebase simplified |
| Alignment with EntityChangeLog | Strong — the change-log-based cache invalidation track works identically with or without TreeLock |
| Retry complexity | Callers must handle `OccConflictException` and retry; this adds complexity at every write call site and changes the observable behavior of the API (idempotency requirements tighten) |
| Catalog-plugin divergence | Must be addressed separately — if the catalog plugin succeeds but the store INSERT fails and retries exhaust, the metadata store and the catalog are out of sync |

---

## 5. Comparison Summary

| Dimension | Option A: Distributed Lock | Option B: Remove TreeLock |
|-----------|---------------------------|--------------------------|
| Correctness | Strong — equivalent to today | Moderate — requires careful DB transaction design per operation |
| Write latency overhead | High — 5+ network RTTs per operation | Zero |
| Read latency overhead | High — 5+ network RTTs per operation | Zero |
| New operational dependency | Yes — etcd or ZooKeeper cluster | No |
| New single point of failure | Yes — coordinator cluster | No |
| Lines of code changed | Moderate (replace internals of lock package) | Large (remove package + all call sites + add retry logic) |
| Alignment with cache-invalidation track | Neutral | Strong — same `EntityChangeLog` infrastructure serves both |
| Community operational burden | High — operators must deploy and monitor coordinator | Low |
| Migration complexity | Low — API unchanged, implementation swappable | High — all write paths change semantics |
| Risk of subtle correctness bugs | Low (coordinator handles the hard parts) | High (per-operation analysis required for each create/drop/rename path) |

---

## 6. Open Questions for Community Discussion

1. **Workload profile**: Is Gravitino's metadata workload predominantly read-heavy (catalog/schema/table discovery) or write-heavy (frequent DDL)? The latency cost of Option A (5 RTTs per read lock) may be acceptable for mostly-write-light workloads, unacceptable for read-heavy ones.

2. **Acceptable DDL latency**: What is the upper bound on acceptable latency for `createTable`, `dropSchema`, etc.? If DDL can absorb 5–10 ms of lock overhead, Option A is viable. If the target is sub-millisecond, it is not.

3. **Coordinator dependency appetite**: Is the community willing to introduce an etcd or ZooKeeper dependency as a required component for HA deployments? Or should HA be achievable with only the existing DB?

4. **DB transaction isolation**: What isolation level is assumed for production deployments? MySQL default (`REPEATABLE READ`) vs. PostgreSQL default (`READ COMMITTED`) affects which phantom-read scenarios exist in Option B. Is there a tested minimum?

5. **Catalog-plugin divergence**: Both options leave the catalog-plugin-vs.-store consistency problem partially unaddressed. Is a two-phase or compensating approach (store-first, then catalog; or catalog with rollback on store failure) in scope for this issue, or is it a separate concern?

6. **Phasing**: Is the `EntityChangeLog` poller (cache invalidation) considered the first HA deliverable, with the locking question as a follow-on? If so, Option B aligns more naturally with that roadmap.

7. **Retry semantics**: If Option B is chosen, should the retry behavior be transparent to API callers, or should the API surface `503 Retry-After` to clients that hit OCC conflicts? The answer affects client SDK design.
