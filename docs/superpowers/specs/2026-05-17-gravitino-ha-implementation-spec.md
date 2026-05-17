# Gravitino HA Correctness — Implementation Specification

**Companion to**: `2026-05-17-gravitino-ha-locking-proposal-final.md`  
**Date**: 2026-05-17

Implementation contracts expressed in Gravitino's existing patterns: `SessionUtils`, MyBatis mappers, `GravitinoEnv` singleton, `TreeLockUtils.doWithTreeLock`. These are authoritative interface specifications for the implementing PRs — method signatures, SQL contracts, and wiring points are normative; import statements and minor syntactic details are illustrative.

---

## Fix 1: `SELECT FOR UPDATE` on Parent Entity Row

### 1a. `createTable` (TableMetaService)

```java
// Current pattern (simplified):
public TableEntity createTable(NameIdentifier ident, TableEntity table) {
    // TreeLock is acquired by the Dispatcher before this call — unchanged
    TablePO tablePO = POConverters.initializeTablePO(table, ...);
    SessionUtils.doWithCommit(
        SqlSessionManager,
        mapper -> mapper.insertTableMeta(tablePO)
    );
}

// Proposed pattern — add parent lock within same transaction:
public TableEntity createTable(NameIdentifier ident, TableEntity table) {
    Long schemaId = resolveSchemaId(ident.namespace());
    TablePO tablePO = POConverters.initializeTablePO(table, ...);

    SessionUtils.doWithCommit(SqlSessionManager, session -> {
        // 1. Lock parent schema row — acquires DB row lock through COMMIT
        //    Verifies existence AND serializes concurrent creates/drops cross-node
        //    Uses new mapper method: SELECT ... FOR UPDATE
        SchemaMetaPO parentSchema = SchemaMetaMapper.selectSchemaMetaForUpdate(schemaId);

        if (parentSchema == null || parentSchema.getDeletedAt() != 0) {
            throw new NoSuchSchemaException(
                "Schema %s does not exist or has been dropped", ident.namespace());
        }

        // 2. Insert child — within same transaction, parent lock still held
        TableMetaMapper.insertTableMeta(tablePO);

        // EntityChangeLog insert (already done by existing code — no change)
        EntityChangeLogMapper.insertEntityChange(
            metalakeName, Entity.EntityType.TABLE, ident.toString(), OperateType.CREATE);
    });
}
```

**New mapper method** (MyBatis XML or annotation):
```sql
-- SchemaMetaMapper.selectSchemaMetaForUpdate
SELECT schema_id, schema_name, deleted_at, ...
FROM schema_meta
WHERE schema_id = #{schemaId}
  AND deleted_at = 0
FOR UPDATE
```

### 1b. `dropSchema` (SchemaMetaService)

```java
// Proposed pattern — lock schema row before soft-deleting children:
public boolean dropSchema(NameIdentifier ident, boolean cascade) {
    Long schemaId = resolveSchemaId(ident);

    SessionUtils.doWithCommit(SqlSessionManager, session -> {
        // Lock the schema row itself — blocks concurrent createTable on this schema
        SchemaMetaPO schema = SchemaMetaMapper.selectSchemaMetaForUpdate(schemaId);

        if (schema == null || schema.getDeletedAt() != 0) {
            return; // already dropped — idempotent
        }

        if (cascade) {
            // Soft-delete all children — they are also within this transaction
            TableMetaMapper.softDeleteTablesBySchemaId(schemaId, currentTime);
            FilesetMetaMapper.softDeleteFilesetsBySchemaId(schemaId, currentTime);
            // ... other child types
        }

        // Soft-delete schema row itself
        SchemaMetaMapper.softDeleteSchemaMetaBySchemaId(schemaId, currentTime);

        EntityChangeLogMapper.insertEntityChange(
            metalakeName, Entity.EntityType.SCHEMA, ident.toString(), OperateType.DELETE);
    });
    // Schema row lock released on COMMIT — any concurrent createTable that was
    // blocked now unblocks, reads deleted_at != 0, throws NoSuchSchemaException
}
```

### 1c. `registerModelVersion` (ModelMetaService — AI workload)

```java
// Same pattern applied to model version registration from training pipelines:
public ModelVersionEntity registerModelVersion(NameIdentifier modelIdent, ...) {
    Long modelId = resolveModelId(modelIdent);
    ModelVersionPO versionPO = POConverters.initializeModelVersionPO(...);

    SessionUtils.doWithCommit(SqlSessionManager, session -> {
        // Lock parent model row — prevents concurrent registerVersion racing dropModel
        ModelMetaPO parentModel = ModelMetaMapper.selectModelMetaForUpdate(modelId);

        if (parentModel == null || parentModel.getDeletedAt() != 0) {
            throw new NoSuchModelException("Model %s does not exist", modelIdent);
        }

        ModelVersionMetaMapper.insertModelVersionMeta(versionPO);
        EntityChangeLogMapper.insertEntityChange(..., OperateType.CREATE);
    });
}
```

---

## Fix 2: Version Increment in POConverters

```java
// File: core/src/main/java/org/apache/gravitino/storage/relational/utils/POConverters.java

// ── BEFORE (line 141) ──────────────────────────────────────────────────────
public static MetalakePO updateMetalakePOWithVersion(
    MetalakePO oldMetalakePO, BaseMetalake newMetalake) {
  Long lastVersion = oldMetalakePO.getLastVersion();
  Long nextVersion = lastVersion;   // BUG: copies, does not increment
  return MetalakePO.builder()
      ...
      .withCurrentVersion(nextVersion)
      .withLastVersion(nextVersion)
      ...
      .build();
}

// ── AFTER ───────────────────────────────────────────────────────────────────
public static MetalakePO updateMetalakePOWithVersion(
    MetalakePO oldMetalakePO, BaseMetalake newMetalake) {
  Long lastVersion = oldMetalakePO.getLastVersion();
  Long nextVersion = lastVersion + 1;   // FIX: monotonically increasing
  return MetalakePO.builder()
      ...
      .withCurrentVersion(nextVersion)
      .withLastVersion(nextVersion)
      ...
      .build();
}
```

**Audit scope** — verify all `update*POWithVersion` methods apply the same pattern:
- `updateMetalakePOWithVersion` — confirmed bug, fix above
- `updateCatalogPOWithVersion` — audit
- `updateSchemaPOWithVersion` — audit
- `updateTablePOWithVersion` — audit
- `updateFilesetPOWithVersion` — audit
- `updateTopicPOWithVersion` — audit
- `updateModelPOWithVersion` — audit

---

## Fix 3: EntityChangeLog Consumer

### 3a. EntityChangeLogPoller class

```java
// New file: core/src/main/java/org/apache/gravitino/cache/EntityChangeLogPoller.java

/**
 * Background poller for cross-node cache invalidation.
 *
 * Reads from entity_change_log (append-only broadcast log) and drives
 * GravitinoCache invalidation on this node. Each HA node runs independently
 * with its own in-memory cursor — no cross-node coordination needed.
 *
 * Design properties:
 * - lastConsumedId is in-memory, per-node; re-initialized from selectMaxChangeId() on start
 * - Idempotent: re-consuming a row only calls invalidate() again — harmless
 * - Bounded staleness: cache coherence lag <= pollIntervalMs
 */
public class EntityChangeLogPoller implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(EntityChangeLogPoller.class);
    private static final int MAX_BATCH = 500;

    private final SqlSessionManager sqlSessionManager;
    private final GravitinoCache<?, ?> cache;
    private final long pollIntervalMs;
    private volatile long lastConsumedId = 0L;
    private ScheduledExecutorService scheduler;

    public EntityChangeLogPoller(
        SqlSessionManager sqlSessionManager,
        GravitinoCache<?, ?> cache,
        long pollIntervalMs) {
      this.sqlSessionManager = sqlSessionManager;
      this.cache = cache;
      this.pollIntervalMs = pollIntervalMs;
    }

    public void start() {
        // New node: cache is empty — no need to replay history
        // Start cursor at current tip of the log
        this.lastConsumedId = fetchMaxChangeId();
        LOG.info("EntityChangeLogPoller starting at cursor id={}", lastConsumedId);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "entity-change-log-poller"));
        scheduler.scheduleAtFixedRate(this::poll, pollIntervalMs, pollIntervalMs, MILLISECONDS);
    }

    private void poll() {
        try {
            List<EntityChangeRecord> changes = fetchEntityChanges(lastConsumedId, MAX_BATCH);
            for (EntityChangeRecord record : changes) {
                dispatchInvalidation(record);
            }
            if (!changes.isEmpty()) {
                lastConsumedId = changes.get(changes.size() - 1).getId();
                LOG.debug("Processed {} change records, cursor advanced to {}",
                    changes.size(), lastConsumedId);
            }
        } catch (Exception e) {
            LOG.warn("EntityChangeLogPoller poll failed (will retry next interval)", e);
            // Do not advance cursor — will re-process on next tick
        }
    }

    private void dispatchInvalidation(EntityChangeRecord record) {
        Entity.EntityType entityType = Entity.EntityType.valueOf(record.getEntityType());
        OperateType opType = record.getOperateType();
        String fullName = record.getFullName(); // e.g., "metalake.catalog.schema.table"

        if (opType == OperateType.DELETE) {
            switch (entityType) {
                case METALAKE:
                    cache.invalidateAll();
                    break;
                case CATALOG:
                    // invalidate catalog + all schemas/tables beneath it
                    cache.invalidateByPrefix(fullName);
                    break;
                case SCHEMA:
                    // invalidate schema + all tables/filesets/topics beneath it
                    cache.invalidateByPrefix(fullName);
                    break;
                case TABLE:
                case FILESET:
                case TOPIC:
                case VIEW:
                case MODEL:
                    cache.invalidate(buildCacheKey(entityType, fullName));
                    break;
                default:
                    cache.invalidate(buildCacheKey(entityType, fullName));
            }
        } else {
            // CREATE or UPDATE — invalidate specific key so next read re-fetches from DB
            cache.invalidate(buildCacheKey(entityType, fullName));
        }
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    // MyBatis session helpers (follow SessionUtils patterns)
    private long fetchMaxChangeId() {
        return SessionUtils.doWithCommitAndFetchResult(
            sqlSessionManager,
            EntityChangeLogMapper.class,
            EntityChangeLogMapper::selectMaxChangeId);
    }

    private List<EntityChangeRecord> fetchEntityChanges(long fromId, int maxRows) {
        return SessionUtils.doWithCommitAndFetchResult(
            sqlSessionManager,
            EntityChangeLogMapper.class,
            mapper -> mapper.selectEntityChanges(fromId, maxRows));
    }
}
```

### 3b. GravitinoEnv wiring

```java
// File: core/src/main/java/org/apache/gravitino/GravitinoEnv.java

// ── ADDITIONS ───────────────────────────────────────────────────────────────

// Field:
private EntityChangeLogPoller entityChangeLogPoller;

// In initialize() — after cache and entity store are initialized:
if (config.get(Configs.ENTITY_CHANGE_LOG_POLL_ENABLED)) {
    long pollIntervalMs = config.get(Configs.ENTITY_CHANGE_LOG_POLL_INTERVAL_MS);
    this.entityChangeLogPoller = new EntityChangeLogPoller(
        sqlSessionManager,
        gravitinoEntityCache,
        pollIntervalMs);
    this.entityChangeLogPoller.start();
    LOG.info("EntityChangeLogPoller started with interval={}ms", pollIntervalMs);
}

// In close():
if (entityChangeLogPoller != null) {
    entityChangeLogPoller.close();
}
```

**New config keys** (follow `Configs.java` pattern):
```java
ConfigEntry<Boolean> ENTITY_CHANGE_LOG_POLL_ENABLED =
    new ConfigBuilder("gravitino.cache.entityChangeLog.enabled")
        .doc("Enable cross-node cache invalidation via entity_change_log polling")
        .version("0.x.0")
        .booleanConf()
        .createWithDefault(false);  // off by default until EntityChangeLog is stable

ConfigEntry<Long> ENTITY_CHANGE_LOG_POLL_INTERVAL_MS =
    new ConfigBuilder("gravitino.cache.entityChangeLog.pollIntervalMs")
        .doc("Interval between entity_change_log polls for cache invalidation (ms)")
        .version("0.x.0")
        .longConf()
        .createWithDefault(500L);
```

---

## Topology: etcd Leader Election Integration

```java
// GravitinoLeaderElection — etcd active-standby leader election interface
// Uses jetcd client library (already a dependency candidate for HA work)

public class GravitinoLeaderElection implements Closeable {

    private static final String LEADER_KEY = "/gravitino/leader";
    private final Client etcdClient;
    private final String nodeId;  // e.g., hostname:port
    private volatile long currentEpoch = -1L;
    private Lease leaseClient;
    private long leaseId;

    public void startElection(long ttlSeconds) throws Exception {
        leaseClient = etcdClient.getLeaseClient();
        leaseId = leaseClient.grant(ttlSeconds).get().getID();

        // Start keepalive — will miss if JVM is CPU-throttled or GPU-offloading
        leaseClient.keepAlive(leaseId, new StreamObserver<LeaseKeepAliveResponse>() {
            public void onNext(LeaseKeepAliveResponse r) { /* lease renewed */ }
            public void onError(Throwable t) { onLeadershipLost(); }
            public void onCompleted() { onLeadershipLost(); }
        });

        // Attempt to become leader via CAS
        ByteSequence key = ByteSequence.from(LEADER_KEY, UTF_8);
        ByteSequence value = ByteSequence.from(nodeId, UTF_8);
        PutOption putOption = PutOption.newBuilder().withLeaseId(leaseId).build();

        Txn txn = etcdClient.getKVClient().txn();
        CompletableFuture<TxnResponse> response = txn
            .If(new Cmp(key, Cmp.Op.VERSION, CmpTarget.version(0)))  // key does not exist
            .Then(Op.put(key, value, putOption))
            .commit();

        TxnResponse result = response.get();
        if (result.isSucceeded()) {
            // I am the leader — get the CreateRevision as the epoch fencing token
            GetResponse getResp = etcdClient.getKVClient().get(key).get();
            this.currentEpoch = getResp.getKvs().get(0).getCreateRevision();
            onLeadershipAcquired(currentEpoch);
        } else {
            // Watch for leader key deletion and retry
            watchAndRetry();
        }
    }

    private void onLeadershipAcquired(long epoch) {
        LOG.info("This node acquired Gravitino leadership with epoch={}", epoch);

        // Store epoch in DB — every subsequent structural write will include
        // AND controller_epoch = #{epoch} in its WHERE clause
        // Stale leader's writes (old epoch) will be rejected by the DB predicate
        SessionUtils.doWithCommit(sqlSessionManager,
            mapper -> mapper.upsertControllerEpoch(nodeId, epoch));

        // Activate TreeLock, MetaServices, etc.
        GravitinoEnv.getInstance().activate();
    }

    private void onLeadershipLost() {
        LOG.warn("Leadership lost (lease expired or keepalive failed). Deactivating.");
        // Drain in-flight requests before deactivating (graceful, unlike System.exit())
        GravitinoEnv.getInstance().deactivate();
    }
}

// Structural write with epoch guard — SQL contract:
//
//   UPDATE schema_meta
//   SET schema_name = #{newName}, last_modified = #{now}
//   WHERE schema_id = #{schemaId}
//     AND deleted_at = 0
//     AND controller_epoch = #{currentEpoch}   ← rejects stale leader writes
//
// If 0 rows updated: either schema not found OR epoch mismatch (stale leader).
// OccConflictException triggers retry or error response.
```

---

## Summary: What Changes, What Stays the Same

```
Component                    Change
─────────────────────────────────────────────────────────────────────
TreeLock / LockManager       UNCHANGED — correct intra-node optimization
TreeLockUtils                UNCHANGED — callers unchanged
Dispatcher layer             UNCHANGED — lock acquisition unchanged
SchemaMetaService            ADD selectSchemaMetaForUpdate before INSERT child
TableMetaService             ADD selectSchemaMetaForUpdate before INSERT table
ModelMetaService             ADD selectModelMetaForUpdate before INSERT version
FilesetMetaService           ADD parent FOR UPDATE
TopicMetaService             ADD parent FOR UPDATE
ViewMetaService              ADD parent FOR UPDATE
POConverters.java:141        CHANGE nextVersion = lastVersion + 1
EntityChangeLogPoller        NEW CLASS — background poll → cache invalidate
GravitinoEnv.java            ADD poller init/close + etcd election (Phase C)
SchemaMetaMapper.xml         ADD selectSchemaMetaForUpdate SQL
TableMetaMapper.xml          (no new SQL needed — uses schema mapper)
ModelMetaMapper.xml          ADD selectModelMetaForUpdate SQL
Configs.java                 ADD two new config keys for poller
```
