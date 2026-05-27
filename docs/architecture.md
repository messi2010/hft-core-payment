# Architecture

## 1. Overview

tbjava is a **single-writer, in-memory, journaled** ledger. All state-mutating
work is funneled onto one thread through a lock-free ring buffer, applied to
in-RAM stores by a deterministic state machine, and made durable by journaling
the **input commands** to a write-ahead log. State that exceeds a journal's
worth of replay is checkpointed to disk via an LSM-backed snapshot store.

The design optimizes for the property that matters in a ledger: **a total,
deterministic order over every mutation**, which makes correctness easy to
reason about and recovery a pure replay.

## 2. Component map

```
com.payments.tbjava
├── api/            HTTP layer (Spring MVC): TbController + request DTOs
├── client/         in-process client convenience wrapper
├── config/         EngineConfig (data dir, ring size, capacities, snapshot interval)
├── domain/         Account, Transfer, flags, result enums, AccountSnapshot
├── engine/         the core:
│     ├── LedgerEngine          public entry point, Disruptor wiring, recovery, checkpoint scheduling
│     ├── LedgerEvent           the ring-buffer event (one per slot)
│     ├── LedgerEventHandler    the single consumer: ingestion + journaling + checkpoint
│     ├── LedgerStateMachine    pure, deterministic apply logic
│     └── IdGenerator           monotonic timestamp source (block reservation)
├── storage/        in-memory stores: AccountStore, TransferStore,
│                   PendingTransferIndex, AccountTransferIndex (all single-writer-owned)
├── journal/        command WAL: Journal (writer), JournalReader (replay), RecordType
├── lsm/            standalone log-structured merge engine (memtable/WAL/SSTable/bloom/manifest/compaction)
└── persistence/    checkpoint abstraction: SerializationProcessor (interface),
                    LsmSerializationProcessor (LSM-backed), LedgerCodec
```

## 3. Threading model

- **API threads** (Netty event loop, Spring WebFlux) accept HTTP requests,
  translate DTOs into domain objects, and *publish* an event into the Disruptor
  ring buffer, returning a `Mono` bridged from the engine's `CompletableFuture`.
  Publishing is **non-blocking** (`ringBuffer.tryNext()`): a full ring fails fast
  with `CapacityExceededException` → HTTP 429, so the event loop is never parked.
- **One writer thread** (`tbjava-writer`) drains the ring buffer and is the sole
  mutator of all stores. No locks are taken anywhere in the hot path because
  there is only ever one writer.
- **Reads are serialized onto the writer thread too** (account/transfer lookups,
  history queries run as ring events). This trades a little read latency for the
  guarantee that reads never observe a partially-applied batch.
- **One checkpoint thread** (`tbjava-checkpoint`, daemon) periodically *publishes*
  a checkpoint event; the snapshot itself still runs on the writer thread so it
  sees a consistent state.

```
HTTP requests
    │
    ▼
API threads (Netty / WebFlux) ─tryNext publish─┐  (full ring → 429)
                                    ▼
                Disruptor ring buffer (2^20 slots, lock-free, MULTI producer)
                                    │  drain (single consumer)
                                    ▼
                       Writer thread — LedgerEventHandler
                          ├── reserve timestamp block (ts = base + index)
                          ├── LedgerStateMachine.apply  (pure, deterministic)
                          ├── journal the INPUT command
                          ├── fsync once at endOfBatch
                          └── complete caller futures
```

## 4. Request lifecycle (write path)

1. `TbController` maps the request DTO to `List<Account>` / `List<Transfer>` and
   calls `LedgerEngine.createAccounts/createTransfers`.
2. `LedgerEngine.publish` claims a ring slot, resets the event, fills inputs,
   attaches a fresh `CompletableFuture`, and publishes the sequence.
3. The writer thread's `LedgerEventHandler.onEvent`:
   - reserves a contiguous timestamp block from `IdGenerator` (`base = reserve(n)`);
   - calls `LedgerStateMachine.createTransfers(batch, base)` — pure apply, no I/O;
   - appends the **input command** (batch + base timestamp) to the journal buffer;
   - on `endOfBatch`, calls `journal.flush()` (one fsync for the whole batch);
   - completes the event's future.
4. The API thread's future resolves with the per-record result codes and the
   HTTP response is returned.

The fsync happens **before** the future completes, so a caller is never told
"OK" for a command that is not durable.

## 5. State machine

`LedgerStateMachine` performs **no I/O and reads no wall clock**. Every record's
timestamp is `baseTimestamp + indexInBatch`, where `baseTimestamp` is supplied by
the caller (the handler on the live path; the journal on the replay path). Given
the same command and the same base, it produces byte-identical state. This is the
property that makes recovery a pure replay (see §6) and makes linked-chain
rollback reproducible.

It owns four in-memory structures:

| Store | Role |
|---|---|
| `AccountStore` | accounts as parallel primitive arrays, `id → slot` map; balance counters |
| `TransferStore` | append-only transfers, `id → slot` map; supports `truncateTo` for rollback |
| `PendingTransferIndex` | status (OPEN/POSTED/VOIDED) + expiry of two-phase transfers |
| `AccountTransferIndex` | secondary index `accountId → transfer ids` for history queries |

## 6. Durability & recovery

Two persistence mechanisms combine, exactly as in TigerBeetle: a fast-to-append
**command journal** (the recent tail) and periodic **snapshots** (the bulk of
state).

### Command journal (WAL)

Each record is one whole input command — a batch of accounts or transfers — plus
the timestamp block reserved for it (not the resulting balances). Records carry a
CRC32C; `JournalReader` verifies every record and stops at the first
torn/truncated record, treating it as a power-loss tail. See
[design.md → command sourcing](design.md#2-command-sourcing-not-state-sourcing).

### Checkpoint (snapshot)

On a schedule (and on graceful shutdown), the writer thread takes a checkpoint:

1. `journal.flush()` so the recorded offset reflects exactly the durable commands;
2. capture `offset = journal.position()`;
3. `serde.storeSnapshot(offset, offset, source)` — stream every account
   (`AccountSnapshot`, with balances) and transfer into the LSM snapshot store;
4. append a `CHECKPOINT` marker to the journal.

The snapshot is tagged with the journal offset to replay from.

### Recovery (startup)

```
load latest LSM snapshot  ─────────────▶ stores (accounts w/ balances, transfers)
        │                                       │
        │ returns journalOffset                 ▼
        │                              rebuild derived indexes
        │                              (pending status, account→transfers)
        ▼                                       │
replay journal commands from journalOffset ─────┘   (re-applied through the
                                                     deterministic state machine)
        │
        ▼
advance IdGenerator past the max recovered timestamp
```

With no snapshot present, `journalOffset` is 0 and recovery degrades to a full
replay. Pending status and the account-transfer index are **not** snapshotted —
both are fully derivable from the transfer set and are rebuilt in two passes
after the snapshot loads.

## 7. LSM as the checkpoint store

The `lsm/` package is a self-contained `UInt128 → byte[]` log-structured merge
engine (memtable + its own WAL + immutable SSTables + bloom filters +
size-tiered compaction + a crash-atomic manifest). It is wired into the system
as the **snapshot backend**, not into the hot read path. `LsmSerializationProcessor`
keeps accounts and transfers in two separate LSM trees (ids share the 128-bit `UInt128` space)
plus a tiny CRC'd `meta` file recording the snapshot id and journal offset.
Each checkpoint re-`put`s current state; the LSM's compaction reclaims
superseded versions. Full internals: [`README.md`](../README.md#lsm-storage-engine).

## 8. Failure handling

- **Journal append/flush failure** surfaces on the event's `error`; the caller's
  future fails. In a real deployment this should *halt* the engine — divergence
  between in-memory state and the journal is unsafe. (Currently it logs and
  surfaces the error; halting is a noted hardening item.)
- **Torn journal tail** (power loss mid-write) is detected by CRC and stops
  replay at the last valid record. Committed commands are never lost.
- **Corrupt SSTable** is rejected on open by a whole-file CRC check.
- **Crash during flush/compaction** of the LSM is safe: the manifest is swapped
  by atomic rename, and SSTables not named by the surviving manifest are ignored.

## 9. Known structural caveats

These are documented in depth in [design.md](design.md):

- The ring event (`LedgerEvent`) is **reference-carrying** (holds caller-allocated
  lists), so one oversized batch occupies a single slot and processed slots retain
  their last payload until reused. Batch size should be bounded.
  See [design.md → Disruptor event design](design.md#9-disruptor-event-design-and-its-caveats).
- The journal grows unbounded; a checkpoint makes everything before its offset
  redundant, but old segments are not yet truncated.
- Checkpointing runs on the writer thread and stalls writes for its duration.
