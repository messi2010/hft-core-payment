# Design

This document records the *why* behind the ledger's engine — the decisions, their
trade-offs, and what is deliberately left out. For the *what* (components, data
flow) see [architecture.md](architecture.md); for the domain see
[business.md](business.md).

## Design principles

1. **Single-writer determinism.** One thread mutates all state, in a total
   order. No locks, no races, and correctness reduces to "is the sequential
   logic correct?".
2. **Double-entry by construction.** A transfer always has exactly one debit leg
   and one credit leg; the engine cannot represent an unbalanced movement.
3. **Determinism enables cheap durability.** If applying a command is a pure
   function of (command, base timestamp, current state), then the log only needs
   the inputs, and recovery is a replay.
4. **Mechanical sympathy where it pays.** Primitive-array stores, no per-account
   objects, batched fsync. But not dogmatically (see §9 — the ring event itself
   trades some of this for ergonomics).
5. **Scope honesty.** Simplifications (u64 amounts, single node) are explicit; flags and
   result codes that are declared but not yet enforced are called out.

## Key decisions

### 1. Single-writer via LMAX Disruptor

A lock-free ring buffer feeds one writer thread. The Disruptor gives a
multi-producer/single-consumer handoff with a sequence barrier that provides the
happens-before guarantees, and `endOfBatch` lets us amortize fsync across a whole
drained batch. Reads are also serialized onto the writer so they never see a
half-applied batch.

**Trade-off:** throughput is bounded by one core's apply rate and by fsync
latency, not by parallelism. For a ledger this is acceptable — purpose-built
ledgers make the same choice — and it's why we *cannot* simply shard (see §10).

### 2. Command-sourcing, not state-sourcing

The journal records the **input command** (a batch of transfers + the reserved
base timestamp), once, at `endOfBatch` — not the mutated balances. Recovery
re-applies commands through the same deterministic state machine.

**Why this matters (the bug it fixes):** the original design journaled per-record
*state* in the middle of applying a chain. If a linked chain failed and rolled
back in memory, the already-written journal records survived, so a restart
replayed effects that the live system had undone — state and journal diverged.
With command-sourcing, replaying the command re-runs validation *and* rollback
identically, so a rolled-back chain can never be half-persisted. This is the
standard approach for command-sourced engines: journal the operation, then
replay it through the same logic.

**Determinism requirement — timestamps.** The state machine assigns
`ts = baseTimestamp + indexInBatch`. The base is reserved once per command from
`IdGenerator.reserve(n)` (a monotonic clock that hands out contiguous blocks) and
journaled in the record header. On replay the base comes from the journal, so no
wall clock is read and timestamps are reproduced exactly. Expiry checks use the
operation's own timestamp as "now", which is also deterministic. After recovery
`IdGenerator.ensureAfter(maxRecoveredTs)` guarantees new writes get strictly
larger timestamps.

**Trade-off:** the journal stores inputs, so recovery cost is "load snapshot +
re-apply tail commands" rather than "memcpy state". Snapshots keep the tail
short.

### 3. Pure, I/O-free state machine

`LedgerStateMachine` takes `(batch, baseTimestamp)` and mutates the stores; it
never touches the journal, disk, or clock. Journaling and durability live in the
*ingestion* layer (`LedgerEventHandler`) and recovery orchestration
(`LedgerEngine`). This separation is what makes the same code path safe to run on
both the live path (with journaling) and the replay path (without).

### 4. Linked-chain atomicity and rollback

A chain is a maximal run of transfers where each but the last carries the
`LINKED` flag. The chain applies all-or-nothing. Rollback is precise: a
`ChainUndo` captures, per chain,

- a per-slot snapshot of the four balance counters before the first mutation,
- the transfer-store size mark (`truncateTo` physically drops inserted transfers,
  freeing the ids for retry),
- the pending-index changes (added vs status-changed, with prior status/expiry),
- the account-index entries added (removed LIFO).

On failure all four are reversed; on success state is left as-is and the command
is journaled. Because rollback restores the store to its pre-chain shape, the
post-recovery state is identical whether or not a chain failed.

### 5. In-memory primitive stores; u128 ids, u64 amounts

Accounts/transfers are not materialized as per-record objects: their scalar
fields live in parallel primitive arrays indexed by slot, with `fastutil`
`Object2IntOpenHashMap<UInt128>` maps from `id → slot` (the hot balance counters
stay in primitive `long[]`). Ids are 128-bit (`UInt128`, an immutable
two-`long` record); amounts stay `long` (u64).

**Why u128 ids:** callers can use externally-generated identifiers (UUID/ULID)
directly — negligible collision probability and no central sequence to coordinate
across upstream systems.

**Trade-off — amounts stay u64:** they max out at 9.2e18 (ample for any single
currency in minor units) and halve memory/CPU per amount field. u128 *amounts*
are *deliberately deferred*: they touch every amount field and the
journal/snapshot codecs for no benefit in a single-currency, minor-unit ledger.
Revisit only if multi-currency atomic transfers over very large amounts become a
requirement.

### 6. The LSM engine

A from-scratch `UInt128 → byte[]` log-structured merge store: a sorted memtable
(red-black tree) fronted by its own WAL, flushed to immutable SSTables
(`DATA | INDEX | BLOOM | FOOTER`, whole-file CRC), with size-tiered compaction
(k-way merge, newest wins, tombstones dropped only in full compaction) and a
crash-atomic `MANIFEST` (temp-write + atomic rename; orphan tables ignored).
Reads check the memtable then SSTables newest→oldest, short-circuiting on the
first hit, with a per-table bloom filter to skip tables that can't hold the key.

**Where it's used:** as the **checkpoint/snapshot backend**, not the hot path.
Putting the balance-mutation hot path behind a memtable+SSTable lookup would be a
regression for an all-in-RAM workload, and would need its own benchmarking pass.
Full internals and test coverage: [`README.md`](../README.md#lsm-storage-engine).

### 7. Snapshots behind a `SerializationProcessor` interface

A pluggable serialization interface: `storeSnapshot(snapshotId,
journalOffset, source)` and `loadLatestSnapshot(sink) → journalOffset`.
Decoupling "how state is stored" from the engine lets tests use an in-memory/no-op
impl and lets the on-disk format evolve independently. The shipped implementation
(`LsmSerializationProcessor`) stores into the LSM (see §6). A snapshot pairs full
state (balances included) with the journal offset to replay from; pending status
and the account index are *not* stored because they are derivable from the
transfer set and rebuilt on load.

### 8. Account history via a secondary index

`get_account_transfers` would otherwise require a full scan of the transfer
store. `AccountTransferIndex` maintains `accountId → transfer ids` in timestamp
order, updated on every insert (under both the debit and credit account),
rolled back with the chain, and rebuilt from the transfer set on recovery
(sorted by timestamp so history is chronological regardless of load order).

### 9. Disruptor event design and its caveats

`LedgerEvent` is a **reference-carrying** event: it holds pointers to the
caller-allocated `List<Account>`/`List<Transfer>`, result lists, and a
`CompletableFuture`. This is ergonomic but diverges from the Disruptor "flyweight"
ideal (a flyweight event would be all primitives + an enum result code + pooled
result events, chunking large payloads across many fixed-size messages).
Consequences to keep in mind:

- **Backpressure — *resolved*.** The API runs on a Netty event loop (Spring
  WebFlux), where blocking is fatal. `publish()` therefore uses
  `ringBuffer.tryNext()` (non-blocking): a full ring throws
  `CapacityExceededException`, which the controller maps to **HTTP 429** instead
  of parking the event-loop thread. The blocking `ringBuffer.next()` is gone.
- **Unbounded payload per slot.** One `createTransfers` call can carry an
  arbitrarily large list, so a single ring slot can hold an arbitrarily large
  batch. The ring is sized for many small events; a few huge batches make memory
  use unpredictable. **Mitigation: bound the batch size** (e.g. a few thousand
  transfers/batch); split larger requests at the API layer.
- **Slot retention.** A processed slot keeps its references until the producer
  *reclaims* that slot (up to a full ring lap later) and calls `reset()`. With
  large payloads this pins `ringSize × payload` of memory. **Mitigation: clear
  the event's references in the consumer right after completing the future.**
- **Returning the recycled event to callers is fragile.** The handler completes
  the future with the *event object*, and the engine reads `e.transferResults` in
  a `thenApply`. This is safe **today only because** that mapping runs
  synchronously inside `complete()` on the writer thread, extracting the value
  before the slot can be reused. An async continuation, or completing from another
  thread, would race slot reuse and read another request's data. **Mitigation:
  complete the future with the extracted result value, never the event.**

None of these is a *live* bug at present, but they are the sharp edges to fix
before pushing large batches or higher concurrency. They do not affect
correctness of the single-writer apply logic itself.

## Design influences

Proven ideas borrowed from purpose-built ledgers and high-throughput
exchange/matching engines:

| Idea | Adopted |
|---|---|
| Single-writer deterministic state machine | yes |
| Double-entry, two-phase pending, linked chains, flags, result codes | yes |
| Command journaling + replay through the engine | yes |
| Snapshot tagged with a sequence/offset; replay only the tail | yes |
| Pluggable serialization (`SerializationProcessor`) | yes |
| Batched fsync at end-of-batch | yes |
| Non-blocking ingestion + fail-fast backpressure | yes (`tryNext` → 429) |
| Reactive HTTP layer (event loop, few threads) | yes (Spring WebFlux / Netty) |
| Flyweight ring event + chunking large payloads | **not yet** (see §9) |
| Object pooling to remove hot-path allocation | not yet (fsync-bound, low ROI) |
| Order matching / order books / margin | N/A (this is a ledger) |
| Symbol sharding | N/A (see §10) |

## 10. Why not shard?

High-throughput exchange engines shard by symbol because a trade touches exactly
one order book and the two users' accounts (risk sharded by user). A ledger
transfer touches **two arbitrary accounts**, and linked transfers can span
arbitrary accounts, so there is no clean shard key that keeps cross-account
atomicity local. This is the same reason purpose-built ledgers run a single
global writer. Horizontal scale therefore means replication for HA (e.g. wrapping
with Apache Ratis), not partitioning the writer.

## Deliberately out of scope

- **u128 amounts** — deferred (§5). (Ids *are* u128 / `UInt128`; only amounts stay u64.)
- **VSR consensus / replication / state sync / cross-peer repair** — use an
  external Raft (Apache Ratis) for HA, or accept single-node with snapshot DR.
- **VOPR-style deterministic fuzzing** — the determinism is in place to make this
  *possible* later; only unit/integration tests exist today.
- **Multiversion binaries / rolling upgrade.**
- **Declared-but-not-yet-enforced transfer flags** —
  `BALANCING_DEBIT/CREDIT`, `CLOSING_DEBIT/CREDIT`, and `HISTORY` (CDC) are
  defined in `TransferFlags`/`AccountFlags` for protocol compatibility but the
  state machine does not act on them yet. The `PENDING_TRANSFER_HAS_DIFFERENT_*`
  result codes are likewise defined but not yet returned. (`OVERFLOWS_*` *are*
  now enforced.) See [business.md](business.md#flag--result-code-coverage).

## Accounting-standard hardening — done

- **u64 overflow guard** — transfers that would wrap a balance counter are
  refused with `OVERFLOWS_*` (the global debit==credit identity held mod 2^64
  even on wrap, but per-account balances would have corrupted).
- **Per-side balance helpers** — `AccountSnapshot.debitBalance()` /
  `creditBalance()` remove the single-convention ambiguity.
- **Trial balance** — `GET /v1/trial-balance` exposes the conservation invariant
  per ledger for reconciliation.
- **Deterministic pending-expiry sweep** — journaled `EXPIRE` command, replayable.

## Hardening backlog (operational)

1. Halt-on-journal-failure instead of surfacing the error.
2. Journal truncation after a successful checkpoint.
3. Off-thread / copy-on-write checkpointing so snapshots don't stall writes.
4. Bound batch size + clear-on-consume + complete-with-value (§9).
5. CDC stream, metrics, WAL fault-injection recovery tests.
