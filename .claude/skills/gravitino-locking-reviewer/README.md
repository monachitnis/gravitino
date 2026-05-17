# gravitino-locking-reviewer

A Claude Code skill for reviewing distributed state management code in Apache Gravitino.

## What it covers

- **TreeLock usage** — correct identifier level and lock type for each operation class
- **DB-layer serialization** — `SELECT FOR UPDATE` on parent rows for structural ops
- **OCC version correctness** — monotonic version increment, ABA vulnerability detection
- **Cache coherence** — `EntityChangeLog` consumer and `GravitinoCache` invalidation scope
- **Leader election and epoch fencing** — etcd lease semantics, epoch-at-storage-layer pattern

## When to invoke

```
/gravitino-locking-reviewer
```

Useful when:
- Writing a new `MetaService` create/drop/rename method
- Adding a new `Dispatcher` or `Manager` class
- Reviewing a PR that touches `doWithTreeLock`, `POConverters`, `EntityChangeLog`, or `GravitinoCache`
- Analyzing an HA proposal for race conditions
- Designing leader election or failover behavior

## Reference docs

- [`docs/treelock-ha/2026-05-17-gravitino-locking-deep-dive.md`](../../docs/treelock-ha/2026-05-17-gravitino-locking-deep-dive.md) — TreeLock internals for new contributors
- [`docs/treelock-ha/2026-05-17-gravitino-ha-locking-proposal-final.md`](../../docs/treelock-ha/2026-05-17-gravitino-ha-locking-proposal-final.md) — full HA design proposal
- [`docs/treelock-ha/2026-05-17-gravitino-ha-implementation-spec.md`](../../docs/treelock-ha/2026-05-17-gravitino-ha-implementation-spec.md) — implementation contracts

## Background

This skill was developed alongside the HA locking proposal for
[apache/gravitino#10474](https://github.com/apache/gravitino/issues/10474).
It encodes the locking discipline, OCC patterns, and race analysis framework
from that proposal into a reusable, AI-invocable code review checklist.

This is an example of **agentic coding contribution** — domain knowledge about
Gravitino's concurrency invariants captured in a form that future contributors
can invoke programmatically, not just read passively.
