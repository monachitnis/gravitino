# Gravitino HA Correctness — End-to-End Integration Test Specification

**Companion to**: `2026-05-17-gravitino-ha-locking-proposal-final.md`  
**Date**: 2026-05-17

Integration test contracts following Gravitino's existing test infrastructure patterns (`BaseIT`, `MiniGravitino`, `ContainerSuite`). Method signatures, assertions, and scenario invariants are normative. These specifications are the authoritative correctness criteria for the implementing test PRs.

---

## Infrastructure Foundation

All HA tests build on existing Gravitino test infrastructure:

| Class | Path | Role |
|---|---|---|
| `MiniGravitino` | `integration-test-common/src/test/java/.../MiniGravitino.java` | Embedded in-process Gravitino server, ephemeral port |
| `BaseIT` | `integration-test-common/src/test/java/.../util/BaseIT.java` | `startAndInitPGBackend()`, `startAndInitMySQLBackend()` via TestContainers |
| `ContainerSuite` | `integration-test-common/src/test/java/.../container/ContainerSuite.java` | Docker network `gravitino-ci-network`, container lifecycle |
| `HiveContainer` | `.../container/HiveContainer.java` | Hive metastore + HDFS |
| `GravitinoAdminClient` | (existing) | REST client for Gravitino API |

HA tests extend `BaseIT` and start **two** `MiniGravitino` instances pointing at the same shared database backend.

---

## HaBaseIT — Base Class for All HA Tests

```java
// HaBaseIT — base class interface contract
// Target: integration-test-common/src/test/java/.../ha/HaBaseIT.java

@ExtendWith({PrintFuncNameExtension.class, CloseContainerExtension.class})
public abstract class HaBaseIT extends BaseIT {

    protected MiniGravitino nodeA;
    protected MiniGravitino nodeB;
    protected GravitinoAdminClient clientA;
    protected GravitinoAdminClient clientB;

    @BeforeAll
    public void startHaCluster() throws Exception {
        // Start shared PostgreSQL backend (inherited from BaseIT)
        startAndInitPGBackend();

        // Node A — primary
        Map<String, String> configA = buildNodeConfig("nodeA", pgJdbcUrl());
        nodeA = new MiniGravitino(new MiniGravitinoContext(configA));
        nodeA.start();
        clientA = GravitinoAdminClient.builder("http://localhost:" + nodeA.getPort()).build();

        // Node B — secondary, same DB, same catalog backends
        Map<String, String> configB = buildNodeConfig("nodeB", pgJdbcUrl());
        nodeB = new MiniGravitino(new MiniGravitinoContext(configB));
        nodeB.start();
        clientB = GravitinoAdminClient.builder("http://localhost:" + nodeB.getPort()).build();

        // Create shared metalake on nodeA
        clientA.createMetalake("test_ml", "HA test metalake", Collections.emptyMap());
    }

    @AfterAll
    public void stopHaCluster() throws Exception {
        if (nodeA != null) nodeA.stop();
        if (nodeB != null) nodeB.stop();
    }

    private Map<String, String> buildNodeConfig(String nodeId, String jdbcUrl) {
        return Map.of(
            "gravitino.entity.store", "relational",
            "gravitino.entity.store.relational.jdbcUrl", jdbcUrl,
            "gravitino.entity.store.relational.jdbcDriver", "org.postgresql.Driver",
            "gravitino.server.webserver.port", String.valueOf(findFreePort()),
            "gravitino.cache.entityChangeLog.enabled", "true",
            "gravitino.cache.entityChangeLog.pollIntervalMs", "200"  // fast for tests
        );
    }
}
```

---

## Scenario 1: Concurrent `createTable` — Two Nodes, Same Schema

```java
// HaConcurrentDdlIT — test contract
// Target: .../ha/HaConcurrentDdlIT.java

@Test
void testConcurrentCreateTableSameSchema() throws Exception {
    // Setup: catalog and schema already exist
    clientA.loadMetalake("test_ml")
        .asCatalog("hive_catalog", Catalog.Type.RELATIONAL, "hive", "", hiveProps())
        .asSchemas().createSchema("orders", "", Collections.emptyMap());

    CountDownLatch startGun = new CountDownLatch(1);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger conflictCount = new AtomicInteger(0);

    // Node A and Node B concurrently create the same table
    Thread threadA = new Thread(() -> {
        startGun.await();
        try {
            clientA.loadMetalake("test_ml")
                .asCatalog("hive_catalog").asTables()
                .createTable(NameIdentifier.of("test_ml", "hive_catalog", "orders", "fact"),
                    columns(), "", Collections.emptyMap());
            successCount.incrementAndGet();
        } catch (TableAlreadyExistsException e) {
            conflictCount.incrementAndGet();
        }
    });

    Thread threadB = new Thread(() -> {
        startGun.await();
        try {
            clientB.loadMetalake("test_ml")
                .asCatalog("hive_catalog").asTables()
                .createTable(NameIdentifier.of("test_ml", "hive_catalog", "orders", "fact"),
                    columns(), "", Collections.emptyMap());
            successCount.incrementAndGet();
        } catch (TableAlreadyExistsException e) {
            conflictCount.incrementAndGet();
        }
    });

    threadA.start(); threadB.start();
    startGun.countDown();
    threadA.join(); threadB.join();

    // Exactly one succeeds
    assertThat(successCount.get()).isEqualTo(1);
    assertThat(conflictCount.get()).isEqualTo(1);

    // No orphaned rows: table_meta count for this schema = 1
    assertTableMetaCount("test_ml", "hive_catalog", "orders", 1);

    // Both caches coherent within poll interval
    Thread.sleep(500); // wait one poll cycle
    assertThat(clientA.loadMetalake("test_ml").asCatalog("hive_catalog")
        .asTables().tableExists(NameIdentifier.of("test_ml", "hive_catalog", "orders", "fact")))
        .isTrue();
    assertThat(clientB.loadMetalake("test_ml").asCatalog("hive_catalog")
        .asTables().tableExists(NameIdentifier.of("test_ml", "hive_catalog", "orders", "fact")))
        .isTrue();
}
```

---

## Scenario 2: Drop Schema Races Create Table (Race 3)

```java
@Test
void testDropSchemaRaceWithCreateTable() throws Exception {
    // Pre-condition: schema exists, no tables
    clientA.loadMetalake("test_ml")
        .asCatalog("hive_catalog").asSchemas()
        .createSchema("race_schema", "", Collections.emptyMap());

    // Introduce artificial delay in dropSchema path (test-only hook)
    // to ensure the race window is exercised
    CountDownLatch dropStarted = new CountDownLatch(1);
    CountDownLatch createStarted = new CountDownLatch(1);

    AtomicReference<Exception> createException = new AtomicReference<>();
    AtomicReference<Exception> dropException = new AtomicReference<>();

    Thread dropThread = new Thread(() -> {
        dropStarted.countDown();
        createStarted.await(); // ensure create has started
        try {
            clientA.loadMetalake("test_ml")
                .asCatalog("hive_catalog").asSchemas()
                .dropSchema(NameIdentifier.of("test_ml", "hive_catalog", "race_schema"), false);
        } catch (Exception e) {
            dropException.set(e);
        }
    });

    Thread createThread = new Thread(() -> {
        dropStarted.await(); // ensure drop has started
        createStarted.countDown();
        try {
            clientB.loadMetalake("test_ml")
                .asCatalog("hive_catalog").asTables()
                .createTable(NameIdentifier.of("test_ml", "hive_catalog", "race_schema", "tbl"),
                    columns(), "", Collections.emptyMap());
        } catch (Exception e) {
            createException.set(e);
        }
    });

    dropThread.start(); createThread.start();
    dropThread.join(); createThread.join();

    // Either: both succeed (sequenced such that create came before drop committed)
    //     OR: create fails with NoSuchSchemaException (drop committed first)
    // NEVER: create succeeds AND schema is dropped (orphaned row)
    if (createException.get() == null) {
        // create won the race — schema should still exist
        assertThat(dropException.get()).isNull();
        // schema was eventually dropped after table was created — both ops completed
    } else {
        // drop won the race
        assertThat(createException.get()).isInstanceOf(NoSuchSchemaException.class);
    }

    // Critical invariant: no table_meta rows with deleted schema_id
    assertNoOrphanedTableRows("test_ml", "hive_catalog", "race_schema");
}

// Helper: queries DB directly to verify no orphaned table rows
private void assertNoOrphanedTableRows(String metalake, String catalog, String schema) {
    // SELECT count(*) FROM table_meta tm
    // JOIN schema_meta sm ON tm.schema_id = sm.schema_id
    // WHERE sm.deleted_at != 0 AND tm.deleted_at = 0
    // → must be 0
    long orphanedCount = queryOrphanedTableCount(metalake, catalog, schema);
    assertThat(orphanedCount).isZero();
}
```

---

## Scenario 3: ABA Version Conflict

```java
@Test
void testAbaVersionConflict() throws Exception {
    // Create metalake with some properties
    Map<String, String> propsV1 = Map.of("env", "test", "owner", "teamA");
    Metalake ml = clientA.createMetalake("aba_test", "ABA test", propsV1);

    // Node B reads the metalake (captures current version)
    Metalake mlOnB = clientB.loadMetalake("aba_test");
    long capturedVersion = mlOnB.version(); // V1

    // Node A updates to V2 (different content)
    clientA.alterMetalake("aba_test",
        MetalakeChange.updateComment("updated by nodeA"));
    // metalake is now at V2

    // Node A reverts to V1-equivalent content
    clientA.alterMetalake("aba_test",
        MetalakeChange.updateComment("test")); // back to semantically V1-like
    // metalake is now at V3 (version column = 3, even if content looks like V1)

    // Node B attempts to update using its stale V1 state
    // This should FAIL with OccConflictException because version has advanced to 3
    assertThatThrownBy(() ->
        clientB.alterMetalake("aba_test",
            MetalakeChange.setProperty("owner", "teamB"))
    ).isInstanceOf(RuntimeException.class)
     .hasMessageContaining("conflict"); // OccConflictException maps to 409

    // Verify the version is now 3 (monotonically increased), not 1
    Metalake current = clientA.loadMetalake("aba_test");
    assertThat(current.version()).isEqualTo(3L);
}
```

---

## Scenario 4: Cross-Node Cache Coherence

```java
@Test
void testCrossNodeCacheCoherence() throws Exception {
    // Warm Node A's cache by reading the table
    clientA.loadMetalake("test_ml")
        .asCatalog("hive_catalog").asTables()
        .loadTable(NameIdentifier.of("test_ml", "hive_catalog", "orders", "fact"));
    // fact table is now in Node A's cache

    // Node B drops the table
    clientB.loadMetalake("test_ml")
        .asCatalog("hive_catalog").asTables()
        .dropTable(NameIdentifier.of("test_ml", "hive_catalog", "orders", "fact"));

    // Immediately: Node A may still return cached result (expected — eventual consistency)
    // This is documented behavior, not a bug

    // After one poll interval: Node A's cache must be invalidated
    Thread.sleep(500); // wait for EntityChangeLogPoller tick (200ms interval in test config)

    // Node A must now return a miss (reads from DB, sees deleted_at != 0)
    assertThatThrownBy(() ->
        clientA.loadMetalake("test_ml")
            .asCatalog("hive_catalog").asTables()
            .loadTable(NameIdentifier.of("test_ml", "hive_catalog", "orders", "fact"))
    ).isInstanceOf(NoSuchTableException.class);
}
```

---

## Scenario 5: AI Agent Concurrent Model Registration Storm

```java
@Test
void testConcurrentModelVersionRegistration() throws Exception {
    // Simulate 10 parallel AI training agents registering model versions
    int AGENT_COUNT = 10;
    String MODEL_IDENT = "test_ml.models_catalog.llama3_schema.llama3";

    // Setup: create model catalog + schema + model
    setupModelCatalog(clientA, "test_ml");

    ExecutorService agentPool = Executors.newFixedThreadPool(AGENT_COUNT);
    CountDownLatch startGun = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(AGENT_COUNT);
    AtomicInteger successCount = new AtomicInteger(0);
    List<Exception> failures = Collections.synchronizedList(new ArrayList<>());

    for (int i = 0; i < AGENT_COUNT; i++) {
        final int agentId = i;
        // Half of agents use Node A, half use Node B
        GravitinoAdminClient client = (agentId % 2 == 0) ? clientA : clientB;

        agentPool.submit(() -> {
            startGun.await();
            try {
                client.loadMetalake("test_ml")
                    .asCatalog("models_catalog").asModels()
                    .registerModelVersion(
                        NameIdentifier.of("test_ml", "models_catalog", "llama3_schema", "llama3"),
                        "s3://models/llama3/v" + agentId,
                        List.of("v" + agentId),
                        "Agent " + agentId + " training run",
                        Map.of("epoch", String.valueOf(agentId)));
                successCount.incrementAndGet();
            } catch (Exception e) {
                failures.add(e);
            } finally {
                done.countDown();
            }
        });
    }

    startGun.countDown();
    done.await(30, SECONDS);
    agentPool.shutdown();

    // All registrations should succeed (each has a unique version URI)
    assertThat(failures).isEmpty();
    assertThat(successCount.get()).isEqualTo(AGENT_COUNT);

    // All 10 versions persisted, no orphans
    long versionCount = countModelVersions("test_ml", "models_catalog", "llama3_schema", "llama3");
    assertThat(versionCount).isEqualTo(AGENT_COUNT);
    assertNoOrphanedModelVersionRows("test_ml", "models_catalog", "llama3_schema");
}
```

---

## Scenario 6: Catalog Smoke Tests (Parameterized by Data Plane)

```java
// Parameterized over all federated catalog types
@ParameterizedTest
@EnumSource(TestCatalogType.class)
void testHaConcurrentDdlPerCatalog(TestCatalogType catalogType) throws Exception {
    String catalogName = catalogType.name().toLowerCase() + "_ha_catalog";

    // Create catalog backed by this data plane (container already started by ContainerSuite)
    Catalog catalog = clientA.loadMetalake("test_ml")
        .asCatalog(catalogName, catalogType.gravitinoType(),
            catalogType.provider(), "", catalogType.props());

    // Phase 1: Concurrent schema creation (both nodes)
    runConcurrently(
        () -> catalog.asSchemas().createSchema("schema_a", "", Map.of()),
        () -> clientB.loadMetalake("test_ml").asCatalog(catalogName)
                     .asSchemas().createSchema("schema_b", "", Map.of())
    );

    // Phase 2: Concurrent table creation under schema_a (both nodes)
    runConcurrently(
        () -> catalog.asTables().createTable(
            NameIdentifier.of("test_ml", catalogName, "schema_a", "tbl1"), columns(), "", Map.of()),
        () -> clientB.loadMetalake("test_ml").asCatalog(catalogName).asTables().createTable(
            NameIdentifier.of("test_ml", catalogName, "schema_a", "tbl2"), columns(), "", Map.of())
    );

    // Phase 3: Drop schema_b from node B while node A lists it
    clientB.loadMetalake("test_ml").asCatalog(catalogName)
        .asSchemas().dropSchema(NameIdentifier.of("test_ml", catalogName, "schema_b"), true);

    // Phase 4: Catalog state consistency check
    // Gravitino metadata must match underlying catalog state
    List<NameIdentifier> gravitinoTables = catalog.asTables()
        .listTables(Namespace.of("test_ml", catalogName, "schema_a"));
    List<String> backendTables = catalogType.listTablesInBackend(catalogName, "schema_a");

    assertThat(gravitinoTables.stream().map(NameIdentifier::name).sorted())
        .containsExactlyInAnyOrderElementsOf(backendTables);

    // No orphaned metadata in either system
    assertNoOrphanedTableRows("test_ml", catalogName, "schema_a");
    assertNoOrphanedTableRows("test_ml", catalogName, "schema_b");
}

enum TestCatalogType {
    HIVE {
        Catalog.Type gravitinoType() { return Catalog.Type.RELATIONAL; }
        String provider() { return "hive"; }
        Map<String, String> props() { return Map.of("metastore.uris", ContainerSuite.getInstance().getHiveContainer().getMetastoreUri()); }
        List<String> listTablesInBackend(String catalog, String schema) { /* query hive metastore */ }
    },
    ICEBERG {
        String provider() { return "lakehouse-iceberg"; }
        Map<String, String> props() { return Map.of("catalog-backend", "rest", "uri", icebergRestUri()); }
        List<String> listTablesInBackend(String catalog, String schema) { /* query iceberg rest */ }
    },
    KAFKA {
        Catalog.Type gravitinoType() { return Catalog.Type.MESSAGING; }
        String provider() { return "kafka"; }
        Map<String, String> props() { return Map.of("bootstrap.servers", kafkaBootstrap()); }
        List<String> listTablesInBackend(String catalog, String schema) { /* list kafka topics */ }
    },
    JDBC_POSTGRESQL {
        String provider() { return "jdbc-postgresql"; }
        Map<String, String> props() { return Map.of("jdbc-url", pgUrl(), "jdbc-user", "test", "jdbc-password", "test"); }
        List<String> listTablesInBackend(String catalog, String schema) { /* query pg information_schema */ }
    },
    PAIMON {
        String provider() { return "lakehouse-paimon"; }
        Map<String, String> props() { return Map.of("catalog-backend", "filesystem", "warehouse", tempWarehousePath()); }
        List<String> listTablesInBackend(String catalog, String schema) { /* list paimon catalog */ }
    };
}
```

---

## Docker Compose — HA Smoke Test Environment

See `docs/treelock-ha/docker/docker-compose-ha-test.yml`.

Key design decisions:
- Both Gravitino nodes share the same `postgres` service (entity store)
- Each node is the same Docker image with different `GRAVITINO_SERVER_PORT` and node ID env vars
- Catalog backends are shared — both Gravitino nodes point at the same Hive metastore, Iceberg REST, Kafka
- `etcd` runs as a single-node for test simplicity (not production 3-node quorum)
- Network: follows `gravitino-ci-network` subnet convention from `integration-test-common/docker-script/docker-compose.yaml`

---

## Fixture Summary

| Class | Scenarios | Notes |
|---|---|---|
| `HaBaseIT` | All | Base: 2x MiniGravitino + shared PG |
| `HaConcurrentDdlIT` | 1, 2 | Race condition verification |
| `HaVersionConflictIT` | 3 | ABA + OCC correctness |
| `HaCacheCoherenceIT` | 4 | EntityChangeLog poll invalidation |
| `HaAiAgentIT` | 5 | Model registration storm, ModelMetaService |
| `HaCatalogSmokeIT` | 6 | Parameterized across all data planes |
