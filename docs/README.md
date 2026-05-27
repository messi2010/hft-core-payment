# Ledger documentation

A single-node, double-entry payment ledger written in Java (Spring Boot + LMAX
Disruptor). Core architectural ideas: single-writer determinism, double-entry
enforced at the engine level, two-phase pending transfers, linked atomic batches,
idempotency by transfer id, a command-sourced write-ahead log, and periodic
snapshots — without consensus/replication machinery (use external HA).

These documents synthesize the system from three angles:

| Doc | Audience | What it covers |
|---|---|---|
| [getting-started.md](getting-started.md) | anyone trying the service | Yêu cầu, chạy service, cấu hình, và ví dụ gọi HTTP API bằng `curl` |
| [architecture.md](architecture.md) | engineers operating/extending the service | Components, threading model, request lifecycle, durability & recovery, failure handling |
| [design.md](design.md) | engineers changing the engine internals | Key design decisions, their rationale and trade-offs, the LSM engine, design influences, what is deliberately out of scope |
| [business.md](business.md) | product / integrators / domain owners | The double-entry domain model, account & transfer semantics, flags, two-phase transfers, idempotency, result codes, and the HTTP API |
| [ledger-core/](ledger-core/README.md) | platform integrators | Mapping mỗi business flow của nền tảng vào primitive của ledger, verify + gap analysis, và sequence diagram (Mermaid) từng flow |

For a quick orientation and runnable examples see the top-level
[`README.md`](../README.md). For the LSM internals see
[`README.md` → "LSM storage engine"](../README.md#lsm-storage-engine).

## Status at a glance

Implemented: account/transfer model + flags, double-entry enforcement,
single-writer deterministic state machine, idempotency, two-phase pending
transfers, linked atomic batches with full rollback, balance-limit flags,
command journaling + recovery, CRC32C + torn-write detection, LSM-backed
snapshot/checkpoint, `get_account_transfers`.

Not implemented (see [design.md](design.md#deliberately-out-of-scope)): u128
amounts (ids *are* u128 / `UInt128`; only amounts remain u64), VSR
consensus/replication, state sync, cross-peer repair, VOPR fuzzing, multiversion
upgrade, and several declared-but-not-yet-enforced transfer flags.
