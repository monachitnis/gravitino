# Gravitino TreeLock: A Contributor's Deep-Dive

**Audience**: New Java contributor, solid Java background, first time in this codebase.
**Focus**: The locking and concurrency subsystem — everything you need to read, write, or review lock-related code confidently.

---

## 1. Gravitino in 500 Words

Apache Gravitino is a **federated metadata lake**. Instead of copying metadata into a central store, it manages metadata *directly* in the underlying systems (Hive Metastore, Iceberg REST catalog, Kafka, MySQL, etc.) and exposes a unified REST API on top. A change made through Gravitino reflects immediately in the underlying system, and vice versa.

### The object hierarchy

Every entity in Gravitino lives in a strict four-level tree:

```
Metalake
  └── Catalog          (type: RELATIONAL | FILESET | MESSAGING | MODEL)
        └── Schema
              └── Table / Fileset / Topic / Model
```

Each level is addressed by a `NameIdentifier` — a dot-separated path, e.g. `my_lake.hive_catalog.sales.orders`. Internally, `NameIdentifier` decomposes into a `Namespace` (the ancestor levels) and a `name` (the leaf). This path structure maps directly onto the lock tree, which is why `TreeLock` exists.

### The request lifecycle

```
HTTP request
    │
    ▼
[server/web/rest/*Operations.java]   ← JAX-RS endpoint, unmarshals JSON
    │
    ▼
[*Dispatcher.java]                   ← e.g. TableOperationDispatcher
    │  validates, applies capabilities, acquires TreeLock
    ▼
[*Manager.java]                      ← e.g. CatalogManager, MetalakeManager
    │  business logic, cache lookups
    ▼
[EntityStore / catalog plugin]       ← JDBC store or Hive/Iceberg/Kafka client
```

`LockManager` is a cross-cutting singleton initialized once in `GravitinoEnv` and injected into every Dispatcher via `GravitinoEnv.getInstance().lockManager()`. It is never passed as a constructor argument — callers always reach it through `TreeLockUtils`, which fetches it from `GravitinoEnv` internally.

---

## 2. Why TreeLock Exists

The core invariant Gravitino must uphold: **a read of any entity must see a consistent snapshot of that entity and its children; a destructive write to a parent must be exclusive with respect to its children**.

Without a lock, these races are possible:
- Thread A lists tables in `schema.sales` while Thread B drops `schema.sales` — A may iterate a half-deleted namespace.
- Thread A renames `table1` to `table2` while Thread B also renames `table1` to `table3` — one rename silently wins.

Java's `ReentrantReadWriteLock` solves this within a single JVM. The challenge is *which* resource to lock, and at *which level* of the hierarchy. TreeLock answers both questions with a single rule:

> **Lock every ancestor with READ, lock the target node with the required type (READ or WRITE).**

The three canonical scenarios (from `TreeLock.java` lines 37–64):

| Operation | Lock chain | Why |
|-----------|-----------|-----|
| Load table `m.c.db1.t1` | `/` R → `m` R → `m/c` R → `m/c/db1` R → `m/c/db1/t1` **R** | Shared read; other readers can proceed in parallel |
| Alter table in-place (no rename) | `/` R → `m` R → `m/c` R → `m/c/db1` R → `m/c/db1/t1` **W** | Exclusive on the table itself; schema and above still allow concurrent reads |
| Drop or rename table | `/` R → `m` R → `m/c` R → `m/c/db1` **W** | Lock the *parent schema*, not the table, because the schema's child namespace is mutating; prevents a sibling `createTable` from racing |

The key insight for drop/rename: locking the table node itself is insufficient because another thread could be creating a *different* table in the same schema concurrently. The schema-level write lock serializes all child namespace mutations.

---

## 3. The Five Classes

### 3.1 `LockType`
**File**: `core/src/main/java/org/apache/gravitino/lock/LockType.java`

```java
public enum LockType { READ, WRITE }
```

`READ` maps to `ReentrantReadWriteLock.readLock()` — multiple threads can hold it simultaneously.
`WRITE` maps to `ReentrantReadWriteLock.writeLock()` — exclusive; blocks all readers and other writers.

---

### 3.2 `TreeLockNode` — the atom of the lock tree
**File**: `core/src/main/java/org/apache/gravitino/lock/TreeLockNode.java`

Each node in the shared lock tree is a `TreeLockNode`. Key fields:

```java
private final String name;                                   // e.g. "sales", "orders"
private final ReentrantReadWriteLock readWriteLock;          // the actual JVM lock
final Map<String, TreeLockNode> childMap;                    // ConcurrentHashMap of children
private final AtomicLong referenceCount;                     // how many TreeLock instances hold this node
private final Map<ThreadIdentifier, Long> holdingThreadTimestamp; // for deadlock detection
```

**`ThreadIdentifier` (inner class, lines 62–100)** — keys the timestamp map with `(Thread, NameIdentifier)` rather than just `Thread`, because one thread can simultaneously hold nodes from two different lock chains (e.g. a nested `loadSchema` call inside `createTable`).

**`getOrCreateChild(String name)` (lines 184–199)** — the only place child nodes are born. Uses `ConcurrentHashMap.computeIfAbsent` for the creation, but the *caller* (`LockManager.createTreeLock`) wraps it in `synchronized(parent)` to prevent duplicate creation under contention:

```java
Pair<TreeLockNode, Boolean> getOrCreateChild(String name) {
    boolean[] newCreated = new boolean[]{false};
    TreeLockNode childNode = childMap.computeIfAbsent(name, k -> {
        newCreated[0] = true;
        return new TreeLockNode(name);
    });
    childNode.addReference();           // increment before returning
    return Pair.of(childNode, newCreated[0]);
}
```

**`addReference` / `decReference` (lines 128–138)** — both `synchronized` on the node. The reference count tracks how many `TreeLock` instances are currently holding a pointer to this node. When it reaches zero, the node is eligible for eviction by the cleaner thread.

**`lock(LockType)` / `unlock(LockType)` (lines 150–173)** — thin wrappers around `readWriteLock`. `unlock` also calls `referenceCount.decrementAndGet()`, so releasing a lock simultaneously signals the node may be evictable.

---

### 3.3 `TreeLock` — one lock acquisition lifecycle
**File**: `core/src/main/java/org/apache/gravitino/lock/TreeLock.java`

A `TreeLock` is a short-lived object representing one lock acquisition. It holds:

```java
private final List<TreeLockNode> lockNodes;   // ordered root → leaf, populated by LockManager
private final Deque<Pair<TreeLockNode, LockType>> heldLocks; // what has been locked so far
```

**`lock(LockType lockType)` (lines 97–139)**:

```
for i in 0..lockNodes.size():
    type = (i == last) ? lockType : LockType.READ
    lockNodes[i].lock(type)
    heldLocks.push( (node, type) )       ← deque acts as progress tracker
    if exception:
        unlock()                          ← rollback all previously acquired locks
        rethrow
```

The `heldLocks` deque serves double duty: it records progress during acquisition (so rollback knows how far to go) and it determines unlock order.

**`unlock()` (lines 142–178)**:

```
while heldLocks not empty:
    (node, type) = heldLocks.pop()        ← LIFO: leaf unlocked first
    node.unlock(type)                     ← also decrements referenceCount
```

Leaf-first unlock is important: it releases the most specific (and most contended) lock first, unblocking waiting writers as early as possible.

---

### 3.4 `LockManager` — the singleton root
**File**: `core/src/main/java/org/apache/gravitino/lock/LockManager.java`

`LockManager` owns the root of the shared lock tree and manages its lifecycle. Initialized once in `GravitinoEnv`.

```java
TreeLockNode treeLockRootNode;        // the "/" node shared by all TreeLock instances
AtomicLong totalNodeCount;            // global node count for eviction triggering
```

**Config keys** (from `Configs.java`):

| Key | Default | Purpose |
|-----|---------|---------|
| `gravitino.lock.maxNodes` | 100,000 | If exceeded, `createTreeLock` throws immediately |
| `gravitino.lock.minNodes` | 1,000 | Eviction stops when count drops below this |
| `gravitino.lock.cleanIntervalInSecs` | 60 | How often the cleaner thread runs |

**`createTreeLock(NameIdentifier identifier)` (lines 240–283)** — walks the identifier's namespace levels plus its name, calling `getOrCreateChild` under `synchronized(lockNode)` at each level:

```java
String[] levels = identifier.namespace().levels();
levels = ArrayUtils.add(levels, identifier.name());   // append leaf name

for (String level : levels) {
    synchronized (lockNode) {
        Pair<TreeLockNode, Boolean> pair = lockNode.getOrCreateChild(level);
        if (pair.getValue()) totalNodeCount.incrementAndGet();  // newly created
    }
    treeLockNodes.add(pair.getKey());
    lockNode = pair.getKey();
}
```

Note the special case at line 251: `identifier == ROOT` (reference equality, not value equality) short-circuits to return immediately with just the root node — used by `doWithRootTreeLock`.

**Two background threads**:

- **Dead-lock checker** (every 60 s): traverses the tree depth-first; for any `(ThreadIdentifier, timestamp)` entry where `now - timestamp > 30_000 ms`, logs a `WARN`. This is advisory only — it does not interrupt the thread.
- **Node cleaner** (every `cleanIntervalInSecs`): triggers if `totalNodeCount > maxTreeNodeInMemory * 0.5`, then calls `evictStaleNodes` on each child of root.

**`evictStaleNodes(TreeLockNode treeNode, TreeLockNode parent)` (lines 203–231)** — post-order traversal (children evicted before self), with a double-checked pattern:

```java
// First check — no lock, cheap
if (treeNode.getReference() == 0) {
    synchronized (parent) {
        // Second check — under parent lock, safe to remove
        if (treeNode.getReference() == 0) {
            parent.removeChild(treeNode.getName());
            totalNodeCount.decrementAndGet();
        }
    }
}
```

The outer check avoids acquiring the parent lock for active nodes. The inner check re-confirms under the lock, because `createTreeLock` may have incremented the reference between the two checks — and `createTreeLock` also runs under `synchronized(parent)`, making the two operations mutually exclusive.

---

### 3.5 `TreeLockUtils` — the only API you should call
**File**: `core/src/main/java/org/apache/gravitino/lock/TreeLockUtils.java`

```java
public static <R, E extends Exception> R doWithTreeLock(
        NameIdentifier identifier, LockType lockType, Executable<R, E> executable) throws E {
    TreeLock lock = GravitinoEnv.getInstance().lockManager().createTreeLock(identifier);
    try {
        lock.lock(lockType);
        return executable.execute();
    } finally {
        lock.unlock();    // always releases, even on exception
    }
}

public static <R, E extends Exception> R doWithRootTreeLock(
        LockType lockType, Executable<R, E> executable) throws E {
    return doWithTreeLock(LockManager.ROOT, lockType, executable);
}
```

`doWithRootTreeLock` is used when operating on the entire metalake namespace (e.g. `MetalakeManager.listMetalakes`). It locks only the root node — a single READ or WRITE on `/`.

---

## 4. Lock Patterns Across the Codebase

### The standard pattern

```java
// Read — shared, non-blocking for other readers
return TreeLockUtils.doWithTreeLock(ident, LockType.READ,
    () -> store.get(ident, EntityType.TABLE, TableEntity.class));

// Write — exclusive
return TreeLockUtils.doWithTreeLock(parentIdent, LockType.WRITE,
    () -> store.put(newEntity, true));
```

### Representative call sites

| Class | Method | Locked identifier | Lock type | Reason |
|-------|--------|-------------------|-----------|--------|
| `MetalakeManager` | `listMetalakes` | ROOT (`/`) | READ | Lists all metalakes; no specific ident yet |
| `MetalakeManager` | `createMetalake` | ROOT (`/`) | WRITE | Adds to the root namespace |
| `MetalakeManager` | `loadMetalake` | `metalakeIdent` | READ | Read-only, metalake-scoped |
| `MetalakeManager` | `dropMetalake` | ROOT (`/`) | WRITE | Removes from root namespace |
| `CatalogManager` | `loadCatalog` | `metalakeIdent` | READ | Catalog lives under metalake |
| `CatalogManager` | `dropCatalog` | `metalakeIdent` | WRITE | Mutates metalake's child namespace |
| `TableOperationDispatcher` | `listTables` | schema ident | READ | Schema-scoped read |
| `TableOperationDispatcher` | `createTable` | schema ident | WRITE | Adds to schema's child namespace |
| `TableOperationDispatcher` | `loadTable` | table ident | READ | Leaf-level read |
| `TableOperationDispatcher` | `dropTable` | schema ident | WRITE | Removes from schema's child namespace |

### The `alterTable` lock promotion (lines 200–227)

`alterTable` has non-trivial lock selection logic. The default is READ on the table ident (in-place alter). But if the change list contains a `TableChange.RenameTable`:

```java
NameIdentifier nameIdentifierForLock = ident;   // default: table-level READ
for (TableChange change : changes) {
    if (change instanceof TableChange.RenameTable) {
        TableChange.RenameTable rename = (TableChange.RenameTable) change;
        if (rename.getNewSchemaName().isPresent()
                && !rename.getNewSchemaName().get().equals(schemaName)) {
            // Cross-schema rename → lock the entire catalog
            nameIdentifierForLock = getCatalogIdentifier(ident);
            break;
        }
        // Same-schema rename → lock the schema
        nameIdentifierForLock = getSchemaIdentifier(ident);
    }
}

return TreeLockUtils.doWithTreeLock(
    nameIdentifierForLock,
    nameIdentifierForLock.equals(ident) ? LockType.READ : LockType.WRITE,
    () -> { ... });
```

**Why catalog-level for cross-schema rename?** The lock system cannot simultaneously hold WRITE on two non-ancestor nodes (source schema and target schema). Promoting to the catalog level gives an exclusive lock over both schemas at once.

---

## 5. Memory Management and Reference Counting

Node creation is cheap; but with millions of unique metadata paths, the tree could grow without bound. The reference count is the safety mechanism.

### Reference count lifecycle

```
createTreeLock():
    for each level:
        getOrCreateChild(level)  → addReference()   [+1 per node]

TreeLock.lock() → nodes now in heldLocks

TreeLock.unlock() → for each node:
    TreeLockNode.unlock()        → referenceCount.decrementAndGet()  [-1 per node]
```

After `unlock()`, if no other `TreeLock` is referencing the node, `referenceCount == 0` and the cleaner may remove it.

On error during `createTreeLock`, the catch block calls `node.decReference()` on every node in the partial list — preventing a reference leak.

### Eviction algorithm

The cleaner fires when `totalNodeCount > maxNodes * 0.5` (50% of max, not 100%). This is intentionally conservative — evicting at 50% gives headroom before the hard limit at 100% (`checkTreeNodeIsFull` throws).

Post-order traversal ensures leaf nodes are evicted before their parents. A parent with zero children and zero references can itself be evicted in a subsequent pass.

The double-checked pattern in `evictStaleNodes` is correct because both the cleaner and `createTreeLock` synchronize on the parent node before touching the child map — making them mutually exclusive for any given parent.

---

## 6. Reading the Tests

| Test file | What it covers |
|-----------|---------------|
| `TestLockManager.java` (673 lines) | Concurrency stress tests: 10 threads × 1000 iterations; verifies mutual exclusion of WRITE locks via atomic counter; reference count correctness; eviction under memory pressure |
| `TestTreeLock.java` (126 lines) | Unit tests for `lock`/`unlock` lifecycle, exception during acquisition triggers full rollback |
| `TestTreeLockUtils.java` (52 lines) | Smoke tests for `doWithTreeLock` and `doWithRootTreeLock` wrappers |

All tests are in `core/src/test/java/org/apache/gravitino/lock/`.

Run them with:
```bash
./gradlew :core:test --tests "org.apache.gravitino.lock.*" -PskipITs
```

---

## 7. Common Pitfalls and Code Review Checklist

**Always use `TreeLockUtils`, never call `LockManager` directly.** The `finally` block in `doWithTreeLock` guarantees unlock even if the lambda throws. Manual lock/unlock pairs are error-prone.

**Lock the parent for create / drop / rename; lock the leaf for read and in-place alter.** Creating a table → lock the schema WRITE. Dropping a catalog → lock the metalake WRITE. Loading a table → lock the table READ.

**Never hold a TreeLock across a network call or blocking I/O.** The deadlock checker fires after 30 seconds. A slow Hive Metastore call inside a WRITE lock will block all operations on that subtree. Move the I/O outside the lock, or restructure to minimize the critical section.

**Do not acquire a WRITE lock on ROOT unless you are mutating the top-level metalake namespace.** A ROOT WRITE lock serializes every single Gravitino operation globally — it is essentially a server-wide stop-the-world.

**If you add a new manager or dispatcher, follow the established lock discipline for that operation type.** Check the table in Section 4 for the right identifier level and lock type before writing new `doWithTreeLock` calls.

**Reference leaks are hard to detect.** If you add a new code path inside `createTreeLock` or modify the error handling, ensure every `addReference` is paired with a `decReference` in all exit paths (normal, exception, and partial-failure).
