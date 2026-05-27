# Core Payment Ledger

A single-node, double-entry payment ledger in Java. Core architectural ideas —
single-writer determinism, double-entry enforcement at the engine level,
two-phase pending transfers, linked atomic batches, idempotency via transfer
IDs, WAL journal with batched fsync — in a Spring Boot service that's operable
by any team that knows Java.

## Documentation

Detailed docs live in [`docs/`](docs/README.md):

- [Architecture](docs/architecture.md) — components, threading model, request
  lifecycle, durability & recovery, failure handling.
- [Design](docs/design.md) — decisions and trade-offs, the LSM engine, the
  Disruptor event caveats, design influences, and what's deliberately out of scope.
- [Business / domain model](docs/business.md) — accounts, transfers,
  double-entry, flags, two-phase & linked transfers, idempotency, result codes,
  and the HTTP API.

## Scope honesty

**This is a focused single-node ledger, not a distributed database.** It does not
ship consensus replication, an LSM forest, fuzz-tested storage repair, or a
deterministic simulator — that's a multi-year project for a team. What this code
captures is the **architectural value** of a purpose-built ledger at a
single-node level:

| Feature | Status |
|---|---|
| Account/Transfer data model + flags | ✅ Implemented |
| Double-entry enforced at engine level | ✅ |
| Single-writer deterministic state machine | ✅ (LMAX Disruptor) |
| Idempotency via transfer ID | ✅ |
| Two-phase pending transfers + timeout | ✅ |
| Linked atomic batches | ✅ (full rollback of state, journal, indexes) |
| Balance limit flags (`debits_must_not_exceed_credits`, ...) | ✅ |
| Command journaling (WAL) + recovery on startup | ✅ (journals input commands, replayed deterministically) |
| CRC32C per record + torn-write detection | ✅ |
| Periodic snapshot/checkpoint | ✅ LSM-backed snapshot + journal-offset tail replay |
| LSM forest storage | ⚠️ single-tree LSM engine (`lsm/`), wired in as the checkpoint store; not in the hot read path |
| Account history / get_account_transfers query | ✅ secondary index + `GET /v1/accounts/{id}/transfers` |
| u128 IDs | ✅ `UInt128` (UUID/ULID-friendly) |
| u128 amounts | ❌ simplified to u64 long |
| VSR consensus replication | ❌ use Apache Ratis externally |
| State sync between replicas | ❌ |
| Storage corruption repair across peers | ❌ |
| VOPR deterministic fuzz testing | ❌ |
| Multiversion binaries / rolling upgrade | ❌ |

The simplifications are deliberate:

- **u128 IDs, u64 amounts** — IDs are 128-bit (`UInt128`) so callers can use
  externally-generated identifiers (UUID/ULID) directly, with negligible
  collision risk and no central sequence to coordinate. Amounts stay u64: for any
  single currency in minor units, `long` max = 9.2e18 is plenty and it halves the
  memory/CPU per amount field. Revisit u128 *amounts* only if multi-currency
  atomic transfers across very large amounts become a requirement.

- **No native consensus** — VSR done right requires Jepsen-grade testing.
  For HA, wrap the ledger in a primary-backup setup using Apache Ratis or
  similar mature Raft library, OR accept single-node operation with file
  snapshot DR.

- **In-memory state + WAL + snapshot** — the hot ledger path still keeps all
  state in RAM (10M accounts at 96 B/each = ~960 MiB). Phase 2 — an on-disk
  LSM storage engine for datasets that exceed RAM — now exists as a standalone,
  tested primitive in the `lsm/` package (see [LSM storage engine](#lsm-storage-engine)).
  It is intentionally **not** yet swapped under `AccountStore`/`TransferStore`:
  doing so changes the read path from an array index to a (memtable + SSTable)
  lookup and needs its own benchmarking pass first.

## Architecture

```
HTTP requests
    │
    ▼
API threads (Netty event loop, Spring WebFlux)
    │  publish event
    ▼
Disruptor ring buffer (2^20 slots, lock-free)
    │  drain
    ▼
Single writer thread
    │   ├── reserve a timestamp block for the command (ts = base + index)
    │   ├── LedgerStateMachine.apply (pure, deterministic, no I/O):
    │   │     validate · mutate balances · linked-chain rollback on failure
    │   ├── journal the INPUT command (not the mutated state)
    │   └── batch fsync at endOfBatch, then complete caller futures
    ▼
Journal WAL (append-only command log, CRC32C per record)
    │
    └── periodic checkpoint → LSM snapshot store (records journal offset)
```

**Command journaling.** The journal records each input command (a batch of
accounts/transfers) plus the timestamp block reserved for it — not the resulting
balances. Recovery re-applies commands through the *same deterministic* state
machine, so a rolled-back linked chain reproduces identically and can never be
half-persisted. Timestamps are derived from the journaled base, so replay needs
no wall clock.

**Recovery.** On startup the engine loads the latest LSM snapshot, rebuilds the
derived indexes (pending status, account→transfers) from the snapshotted transfer
set, then replays only the journal commands written *after* the snapshot's
offset. With no snapshot this degrades to a full replay from offset 0. A torn
tail (power loss) stops replay at the last valid record — committed commands are
never lost.

## Key files to read

- `engine/LedgerStateMachine.java` — the heart. Pure, deterministic apply
  logic for accounts and transfers, with full linked-chain rollback.
- `engine/LedgerEngine.java` — Disruptor wiring, checkpoint scheduling, and
  snapshot+journal recovery on startup.
- `engine/LedgerEventHandler.java` — the ingestion layer: reserves timestamps,
  applies, journals the command, batches fsync, runs checkpoints.
- `journal/Journal.java` — command-log WAL format and batched fsync.
- `persistence/SerializationProcessor.java` — checkpoint abstraction
  (a pluggable serialization interface); `LsmSerializationProcessor`
  is the LSM-backed implementation.
- `storage/AccountStore.java` — primitive-array-based account storage.
- `lsm/Lsm.java` — the LSM storage engine: memtable + WAL + SSTables +
  size-tiered compaction + crash-atomic manifest.

## LSM storage engine

The `lsm/` package is a self-contained log-structured merge store
(`UInt128 key -> byte[] value`) — the "phase 2" path off the all-in-RAM ceiling.
It mirrors the rest of the engine's single-writer model (one owner thread, no
locks) and reuses the project's little-endian + CRC32C conventions.

```
put/delete
    │  append (fsync) ──────────────▶ lsm.wal   (replayed on recovery)
    ▼
Memtable (red-black tree, sorted)
    │  flush when it exceeds memtableFlushBytes
    ▼
SSTable  sst-<seq>.sst  (immutable, sorted: DATA | INDEX | BLOOM | FOOTER)
    │  too many tables → size-tiered compaction (k-way merge, newest wins,
    │  tombstones dropped)
    ▼
MANIFEST  (atomic rename; the source of truth for which tables are live)
```

- **Reads** check the memtable, then each SSTable newest→oldest, short-circuiting
  on the first hit. A per-table Bloom filter skips tables that can't hold the key,
  and a full in-memory index turns a hit into a single seek.
- **Durability**: every write is WAL-appended before it's acknowledged. Flush
  writes the SSTable, commits the manifest, *then* rotates the WAL — so a crash
  at any point recovers to a consistent state (orphan SSTables not named by the
  manifest are ignored).
- **Deletes** are tombstones; they shadow older tables on read and are physically
  dropped only during a full compaction, when no older table survives to
  resurrect the key.

Covered by `LsmTest` (flush, recovery, tombstone shadowing, compaction, a 5k-op
randomized differential test vs. a reference map, and CRC corruption rejection)
and `BloomFilterTest`. Run with `mvn test -Dtest=LsmTest,BloomFilterTest`.

## Running locally

```bash
mvn spring-boot:run
```

The service starts on port 8081 (Spring WebFlux on Netty) and creates a journal
at `./data/journal.log`.

> **Heap sizing.** The stores are pre-allocated from `ledger.max-accounts` /
> `ledger.max-transfers`. The defaults (10M / 100M) reserve several GB up front
> and will OOM on a default JVM heap. For local dev, shrink them, e.g.
> `mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Dledger.max-accounts=10000 -Dledger.max-transfers=10000"`,
> or run with a large `-Xmx` in production sized to your capacities.

```bash
# Create an account (LIABILITY-like, no balance limit)
curl -X POST http://localhost:8081/v1/accounts \
  -H 'Content-Type: application/json' \
  -d '{"accounts":[{"id":1001,"ledger":704,"code":10,"flags":0,"userData64":0,"userData32":0}]}'

# Create another
curl -X POST http://localhost:8081/v1/accounts \
  -H 'Content-Type: application/json' \
  -d '{"accounts":[{"id":1002,"ledger":704,"code":10,"flags":0,"userData64":0,"userData32":0}]}'

# Transfer 100000 from 1001 to 1002
curl -X POST http://localhost:8081/v1/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transfers":[{
       "id":5001,
       "debitAccountId":1001,
       "creditAccountId":1002,
       "amount":100000,
       "pendingId":0,
       "userData64":0,
       "userData32":0,
       "timeoutSeconds":0,
       "ledger":704,
       "code":720,
       "flags":0
     }]}'

# Look up account
curl http://localhost:8081/v1/accounts/1001

# Account transfer history (newest first)
curl 'http://localhost:8081/v1/accounts/1001/transfers?limit=50'

# Trial balance per ledger (debits should equal credits)
curl http://localhost:8081/v1/trial-balance
```

## Performance characteristics

Single writer thread + batched fsync. Throughput is bounded by:

1. **fsync latency** — local NVMe = ~50-100 µs/fsync. With batch size 100,
   that's 1k batches/sec × 100 = **100k transfers/sec**.
2. **Disruptor throughput** — well over 1M events/sec on commodity hardware.
   Not a bottleneck.
3. **State mutation** — primitive array ops + 1 HashMap lookup. ~50 ns per
   transfer. Not a bottleneck.

Disabling fsync (`fsyncBatch: false` for non-durable testing) pushes
throughput past 500k/sec. Real production with fsync = expect ~100-300k
transfers/sec depending on disk.

## Operational gaps you'll want to fill

1. **HA** — wrap with Apache Ratis or write a primary-backup using the
   journal as the replication stream.
2. **Journal truncation** — the command log grows unbounded; a checkpoint
   makes everything before its offset redundant, so old journal segments can be
   reclaimed after a snapshot. Not yet done.
3. **Off-thread checkpointing** — `runCheckpoint` currently snapshots on the
   writer thread, stalling writes for its duration. Production wants a
   copy-on-write / background snapshot.
4. **CDC** — add an event listener interface on commit, pump events to Kafka.
   Hook is in `LedgerEventHandler.onEvent` at the end-of-batch point.
5. **Metrics** — Micrometer is on the classpath via actuator; instrument
   the state machine with counters and histograms.
6. **Recovery testing** — fault injection in the WAL (random bit flips,
   truncated tails, replayed segments) to verify the CRC check actually
   catches corruption.

## When to use this vs alternatives

- **Use this** if you want ledger-style guarantees in pure Java, your data
  fits in RAM, and you're OK with single-node operation (or wrapping HA
  externally). Command journaling, LSM-backed snapshots, and account history
  ship in the box.
- **Use a mature distributed ledger/database** if you need built-in consensus
  replication, on-disk LSM storage, and simulator-tested correctness without
  building any of it yourself.
- **Use Postgres** if your throughput is under 5k tps and your team would
  rather scale operationally than introduce custom infrastructure.

## License

This is example code. Treat as a starting point, not production-ready.
Real production deployment needs the gap-filling listed above plus your own
testing, observability, and ops runbook.
