# High availability — Raft active-passive (đã implement)

Mô tả cơ chế replication **đã code** trong package `com.payments.ledger.raft`
(Apache Ratis 3.1.0, gRPC). Mô hình: **active-passive** — một leader là writer
duy nhất, follower replay cùng log để giữ state giống hệt; leader chết thì bầu
follower lên thay.

> Ánh xạ với mô hình cũ: **RaftLog thay Journal+Disruptor** trên đường ghi;
> `applyTransaction` chính là **điểm mutate duy nhất** (single-writer, nay
> replicated). Tái dùng nguyên `LedgerStateMachine` + stores + enum kết quả.

## 1. Topology

```mermaid
flowchart TB
  C["RaftLedgerClient"]
  subgraph N1["Node 1 — LEADER (single writer)"]
    SM1["LedgerRaftStateMachine"]
    ST1["stores in RAM"]
    LG1["RaftLog on disk"]
    SM1 --> ST1
    SM1 --> LG1
  end
  subgraph N2["Node 2 — FOLLOWER"]
    SM2["StateMachine"]
    ST2["stores in RAM"]
    LG2["RaftLog"]
  end
  subgraph N3["Node 3 — FOLLOWER"]
    SM3["StateMachine"]
    ST3["stores in RAM"]
    LG3["RaftLog"]
  end
  C -->|"write"| N1
  C -->|"read"| N1
  N1 ==>|"replicate entries · gRPC"| N2
  N1 ==>|"replicate entries · gRPC"| N3
```

## 2. Đường GHI (write) — `client.io().send(...)`

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant L as Leader
    participant F1 as Follower 1
    participant F2 as Follower 2
    C->>L: send( encodeTransfers(batch), baseTs=0 )
    Note over L: startTransaction() [chỉ leader]<br/>base = clock.reserve(n)<br/>đóng baseTs vào log entry
    L->>L: append entry vào RaftLog (fsync)
    L->>F1: AppendEntries(entry)
    L->>F2: AppendEntries(entry)
    F1-->>L: ack (đã ghi log)
    F2-->>L: ack
    Note over L: đạt QUORUM → COMMIT
    par apply trên MỌI node, đúng thứ tự commit
        L->>L: applyTransaction → LedgerStateMachine.apply(batch, base)
    and
        F1->>F1: applyTransaction → apply(batch, base)
    and
        F2->>F2: applyTransaction → apply(batch, base)
    end
    L-->>C: reply = mã kết quả từng record
```

Điểm cốt lõi:
- **base timestamp** do leader cấp ở `startTransaction` rồi **nằm trong log** ⇒ mọi node tính `ts = base + index` giống hệt (không đọc wall clock).
- Command chỉ trả về client **sau khi commit** (quorum đã ghi log) — tương tự "fsync trước khi báo OK" của bản single-node, nay là "replicated trước khi báo OK".
- `applyTransaction` còn gọi `clock.ensureAfter(base+n-1)` trên **mọi** node.

## 3. Đường ĐỌC (read) — `client.io().sendReadOnly(...)`

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant L as Leader
    C->>L: sendReadOnly( encodeLookupAccount(id) )
    Note over L: query() — đọc thẳng stores in-RAM,<br/>KHÔNG ghi vào log
    L-->>C: AccountView (số dư)
```

## 4. Recovery khi restart — replay RaftLog

```mermaid
flowchart LR
  A["Start node<br/>(group = null)"] --> B["Ratis recover group + RaftLog từ disk"]
  B --> C["lastApplied = 0<br/>(chưa có snapshot)"]
  C --> D["replay các entry đã commit<br/>→ applyTransaction"]
  D --> E["stores in-RAM dựng lại<br/>state y hệt trước khi tắt"]
```

Hiện chưa tích hợp snapshot LSM ⇒ recovery = **replay toàn bộ log đã commit** (đã test: tắt node → bật lại → số dư khôi phục đúng).

## 5. Failover khi leader chết (đã test 3-node)

```mermaid
sequenceDiagram
    autonumber
    participant F1 as Follower (up-to-date)
    participant F2 as Follower
    Note over F1,F2: Leader mất liên lạc (timeout)
    F1->>F2: RequestVote (term+1)
    F2-->>F1: vote
    Note over F1: đạt quorum → F1 thành LEADER<br/>state đã current (apply cùng entries)<br/>clock.ensureAfter ⇒ ts mới > ts cũ
    F1->>F1: tiếp nhận ghi mới (single writer)
    Note over F1,F2: KHÔNG có 2 leader cùng ghi (quorum chống split-brain)
```

## 6. Ánh xạ code ↔ khái niệm Raft

| Khái niệm Raft | Trong code |
|---|---|
| Replicated log entry | command `[op][baseTs][payload]` ([LedgerCommandCodec](../src/main/java/com/payments/ledger/raft/LedgerCommandCodec.java)) |
| State machine apply | `applyTransaction` → `LedgerStateMachine.apply` ([LedgerRaftStateMachine](../src/main/java/com/payments/ledger/raft/LedgerRaftStateMachine.java)) |
| Gán dữ liệu trước khi replicate (leader) | `startTransaction` (reserve + stamp base ts) |
| Read-only query | `query()` đọc stores in-RAM |
| Server/transport | `RaftServer` gRPC ([RaftLedgerServer](../src/main/java/com/payments/ledger/raft/RaftLedgerServer.java)) |
| Client | `RaftClient` ([RaftLedgerClient](../src/main/java/com/payments/ledger/raft/RaftLedgerClient.java)) |
| Bootstrap vs recover | `setGroup(group)` lần đầu; `group=null` khi restart |

## 7. Trạng thái

- ✅ Đã test 1-node: write replicate→apply, read, idempotency, restart-recovery bằng replay log ([RaftReplicationTest](../src/test/java/com/payments/ledger/raft/RaftReplicationTest.java)).
- ✅ Đã test **failover 3-node**: ghi committed → giết leader → cụm bầu leader mới → state committed sống sót, retry cùng id ra `EXISTS` (không double-apply / không transfer ma), tiếp tục ghi được ([RaftFailoverTest](../src/test/java/com/payments/ledger/raft/RaftFailoverTest.java)).
- ⏳ Chưa tích hợp **snapshot + purge RaftLog** (log grow vô hạn — giống backlog journal-truncation).
- ⏳ Chưa wire vào Spring/HTTP (module opt-in).
