# Core Payment Ledger

Ledger thanh toán **double-entry, single-node** viết bằng Java (Spring Boot +
LMAX Disruptor). Đây là **ledger core** — nguồn sự thật duy nhất về số dư cho một
nền tảng thanh toán: nó ghi lại tiền di chuyển giữa các tài khoản với những bảo
đảm mà một hệ thống kế toán cần, chứ **không** định giá, khớp lệnh hay định tuyến.

Các ý tưởng kiến trúc cốt lõi:

- **Single-writer deterministic** — mọi lệnh ghi đi qua một thread duy nhất theo
  một thứ tự toàn cục, nên tính đúng quy về "logic tuần tự có đúng không?".
- **Double-entry cưỡng chế ở tầng engine** — mỗi transfer luôn có đúng một vế nợ
  và một vế có bằng nhau; engine không thể biểu diễn một chuyển động lệch.
- **Two-phase pending** (giữ tiền rồi chốt/hủy), **linked atomic batch**
  (chuỗi all-or-nothing), **idempotency theo transfer id**.
- **WAL command-sourcing + snapshot** — journal ghi *lệnh đầu vào*, recovery là
  replay thuần qua cùng state machine; snapshot (LSM) giữ phần lớn state để rút
  ngắn đuôi replay.

> Chi tiết nội bộ engine, lý do thiết kế, HA (Raft), và cách map nền tảng thanh
> toán vào ledger: xem [ARCHITECTURE.md](ARCHITECTURE.md).
> Đặc tả HTTP API đầy đủ: [docs/openapi.yaml](docs/openapi.yaml).

## Phạm vi & trạng thái (scope honesty)

**Đây là một ledger single-node có chủ đích, không phải một CSDL phân tán.** Bản
single-node không tự ship consensus replication, LSM forest, sửa lỗi lưu trữ
fuzz-test, hay simulator tất định — đó là dự án nhiều năm của cả một đội. Cái mà
code này nắm bắt là **giá trị kiến trúc** của một ledger chuyên dụng ở mức
single-node, cộng thêm một module HA Raft active-passive đã implement (opt-in).

| Tính năng | Trạng thái |
|---|---|
| Mô hình dữ liệu Account/Transfer + flags | ✅ |
| Double-entry cưỡng chế ở tầng engine | ✅ |
| Single-writer deterministic state machine | ✅ (LMAX Disruptor) |
| Idempotency theo transfer id | ✅ |
| Two-phase pending + timeout (auto-expire) | ✅ |
| Linked atomic batch | ✅ (rollback đầy đủ state, journal, index) |
| Cờ giới hạn số dư (`debits_must_not_exceed_credits`, …) | ✅ |
| Command journaling (WAL) + recovery khi khởi động | ✅ (journal lệnh đầu vào, replay tất định) |
| CRC32C mỗi record + phát hiện torn-write | ✅ |
| Snapshot/checkpoint định kỳ | ✅ (LSM snapshot + replay đuôi journal theo offset) |
| LSM storage engine | ⚠️ LSM 1-tree (`lsm/`), dùng làm checkpoint store; chưa nằm trên hot read path |
| Lịch sử tài khoản (`get_account_transfers`) | ✅ secondary index + `GET /v1/accounts/{id}/transfers` |
| u128 id | ✅ `UInt128` (UUID/ULID-friendly) |
| HA replication (Raft active-passive) | ✅ đã implement & test (`raft/`, Apache Ratis) — opt-in, chưa wire vào HTTP |
| u128 amount | ❌ rút gọn thành u64 `long` |
| VOPR deterministic fuzz testing | ❌ |
| Multiversion binaries / rolling upgrade | ❌ |

Các điểm rút gọn là **có chủ đích**:

- **u128 id, u64 amount** — id 128-bit (`UInt128`) để caller dùng định danh sinh
  bên ngoài (UUID/ULID) trực tiếp, xác suất trùng không đáng kể, không cần khóa
  tuần tự tập trung. Amount giữ u64: với một loại tiền tệ tính theo minor unit,
  `long` max = 9.2e18 là dư, lại giảm một nửa bộ nhớ/CPU mỗi trường amount. Chỉ
  xét lại u128 *amount* nếu phát sinh nhu cầu transfer đa tệ atomic với số rất lớn.
- **HA** — bản single-node để vận hành đơn giản; cần production HA thì bật module
  Raft active-passive (`raft/`) hoặc bọc snapshot DR. Xem
  [ARCHITECTURE.md → High availability](ARCHITECTURE.md#10-high-availability--raft-active-passive-đã-implement).
- **State in-RAM + WAL + snapshot** — hot path giữ toàn bộ state trong RAM (10M
  tài khoản ở 96 B/cái ≈ 960 MiB). LSM (`lsm/`) đã tồn tại như một primitive độc
  lập đã test, đang dùng làm checkpoint store; **chưa** swap dưới
  `AccountStore`/`TransferStore` vì việc đó đổi read path từ array-index sang tra
  cứu memtable+SSTable và cần một vòng benchmark riêng.

## Sơ đồ kiến trúc (rút gọn)

```
HTTP request
    │
    ▼
API threads (Netty event loop, Spring WebFlux)
    │  publish event (tryNext → ring đầy = HTTP 429)
    ▼
Disruptor ring buffer (2^20 slot, lock-free, multi-producer)
    │  drain (single consumer)
    ▼
Writer thread duy nhất
    │   ├── reserve khối timestamp cho lệnh (ts = base + index)
    │   ├── LedgerStateMachine.apply (thuần, tất định, không I/O)
    │   ├── journal LỆNH ĐẦU VÀO (không phải state đã đổi)
    │   └── fsync gộp ở endOfBatch, rồi complete future cho caller
    ▼
Journal WAL (append-only, CRC32C mỗi record)
    │
    └── checkpoint định kỳ → LSM snapshot store (ghi kèm journal offset)
```

Giải thích chi tiết từng tầng, mô hình thread, recovery: xem
[ARCHITECTURE.md](ARCHITECTURE.md).

## Bắt đầu nhanh

### Yêu cầu

- **JDK 21** (project dùng record + pattern hiện đại; JDK cũ hơn sẽ báo "release
  version 21 not supported").
- **Maven 3.9+** (chưa có Maven wrapper trong repo).

```bash
# macOS — trỏ JAVA_HOME sang JDK 21 nếu mặc định đang cũ:
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
java -version   # phải in 21.x
```

### Chạy service

```bash
# cách 1: chạy trực tiếp
mvn spring-boot:run

# cách 2: đóng gói rồi chạy jar
mvn clean package
java -jar target/htt-core-payment-0.1.0-SNAPSHOT.jar
```

Khi log in `LedgerEngine ready: accounts=... ringBufferSize=...` là service đã
sẵn sàng. Service nghe cổng **8081** (Spring WebFlux trên Netty) và tạo journal
tại `./data/journal.log`.

```bash
curl -s localhost:8081/actuator/health      # {"status":"UP"}
```

> **Lưu ý heap.** Các store được cấp phát trước theo `ledger.max-accounts` /
> `ledger.max-transfers`. Mặc định (10M / 100M) chiếm vài GB ngay từ đầu và sẽ
> OOM trên heap JVM mặc định. Dev cục bộ nên thu nhỏ, ví dụ:
> `mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Dledger.max-accounts=10000 -Dledger.max-transfers=10000"`,
> hoặc chạy với `-Xmx` lớn ở production khớp dung lượng thực.

### Cấu hình chính

Mặc định trong [`application.yml`](src/main/resources/application.yml) (override
bằng env/flag Spring Boot):

| Khóa | Mặc định | Ý nghĩa |
|---|---|---|
| `server.port` | `8081` | cổng HTTP |
| `ledger.data-dir` | `./data` | nơi lưu journal (WAL) + snapshot. Xóa thư mục này để reset sạch state |
| `ledger.ring-buffer-size` | `1048576` | số slot Disruptor (đầy → HTTP 429) |
| `ledger.max-accounts` / `max-transfers` | 10M / 100M | dung lượng store cấp phát trước |
| `ledger.snapshot.interval-seconds` | `300` | chu kỳ checkpoint |
| `ledger.expiry-sweep-seconds` | `30` | chu kỳ quét auto-void pending hết hạn |

## Mô hình nghiệp vụ

Ledger trả lời nhanh và đáng tin một câu hỏi: *số dư của mỗi tài khoản là bao
nhiêu, cho một dòng chuyển động giá trị có thứ tự?*

### Khái niệm cốt lõi

| Khái niệm | Ý nghĩa |
|---|---|
| **Account** | Túi tiền vào/ra. Định danh bằng `id` 128-bit (`UInt128`, UUID/ULID-friendly), thuộc một `ledger`, phân loại bằng `code`. |
| **Ledger** | Ranh giới một loại tiền/đơn vị. Một transfer chỉ di chuyển giữa hai account **cùng** ledger (vd `ledger=704` = VND; `840` = USD-cents). |
| **Code** | Phân loại do người dùng định nghĩa (vd "ví khách", "phí", "settlement"). Phải khác 0. |
| **Transfer** | Một chuyển động cân bằng: nợ một account và có một account khác cùng `amount`. |
| **Amount** | u64 minor unit (vd đồng/cent). Coi như unsigned; max 9.2e18. |

**Double-entry.** Mỗi transfer có **đúng một vế nợ và một vế có bằng nhau**.
Không có API để "chỉ đổi số dư". Vì vậy tổng nợ luôn bằng tổng có trên một ledger,
theo cấu trúc.

**Balances.** Mỗi account theo dõi bốn counter:

| Counter | Ý nghĩa |
|---|---|
| `debitsPosted` | giá trị đã chốt phía nợ |
| `creditsPosted` | giá trị đã chốt phía có |
| `debitsPending` | giá trị bị giữ bởi pending chưa chốt (vế nợ) |
| `creditsPending` | giá trị bị giữ bởi pending chưa chốt (vế có) |

Net position tùy bản chất kế toán của account: dùng
`AccountSnapshot.creditBalance()` (`creditsPosted − debitsPosted`) cho account
credit-normal (liability/income/equity) và `debitBalance()` cho account
debit-normal (asset/expense). `balance()` là alias của `creditBalance()`.

### Cờ giới hạn số dư

Account không có "loại" first-class — integrator tự mã hóa qua `code` và chọn cờ
giới hạn đúng bản chất:

| Loại account | Bản chất | Cờ cần đặt | Đọc số dư |
|---|---|---|---|
| Asset / Expense | nợ (debit) | `CREDITS_MUST_NOT_EXCEED_DEBITS` | `debitBalance()` |
| Liability / Income / Equity | có (credit) | `DEBITS_MUST_NOT_EXCEED_CREDITS` | `creditBalance()` |
| Clearing / suspense | hai chiều | *(không cờ)* | tùy |

- `DEBITS_MUST_NOT_EXCEED_CREDITS` — từ chối (`EXCEEDS_DEBITS_LIMIT`) nếu
  `debitsPosted + debitsPending + amount > creditsPosted` (chống âm ví).
- `CREDITS_MUST_NOT_EXCEED_DEBITS` — gương lại (`EXCEEDS_CREDITS_LIMIT`).

Cờ là **tùy chọn**: bỏ trống cho account hợp lệ swing cả hai chiều (clearing,
một số equity). Đặt **sai** cờ sẽ làm các posting hợp lệ bị từ chối — nên việc map
chart of accounts là trách nhiệm của integrator, không phải engine.

### Các loại transfer

Ba cờ two-phase loại trừ lẫn nhau.

- **Standard** (không cờ đặc biệt): chuyển ngay `amount` từ nợ sang có dưới dạng
  *posted*. Chịu kiểm tra giới hạn số dư và account đóng/ledger.
- **Two-phase (pending)** cho workflow giữ tiền rồi chốt sau (authorization,
  escrow):
  1. **PENDING** — giữ `amount` (tăng `debitsPending`/`creditsPending`). Có thể
     đặt `timeoutSeconds` để hết hạn.
  2. **POST_PENDING_TRANSFER** — chốt pending theo `pendingId`: nhả phần giữ và
     post `amount` đã chốt (có thể ≤ gốc; phần dư tự nhả). Post nhiều hơn gốc bị
     từ chối (`EXCEEDS_PENDING_TRANSFER_AMOUNT`).
  3. **VOID_PENDING_TRANSFER** — hủy pending: nhả phần giữ, không post gì.

  Một pending chỉ chốt/hủy được một lần. Hết hạn được cưỡng chế hai cách: *lazy*
  (post/void một pending hết hạn bị từ chối `PENDING_TRANSFER_EXPIRED`) và *chủ
  động* qua sweep định kỳ (`ledger.expiry-sweep-seconds`) tự auto-void và nhả tiền
  — journal hóa thành lệnh `EXPIRE` tất định nên replay được.
- **Linked atomic batch**: transfer có cờ `LINKED` được nối với transfer **kế
  tiếp** trong batch. Một chuỗi là một run tối đa kết thúc ở transfer đầu tiên
  *không* mang cờ. Chuỗi là **all-or-nothing**: nếu một thành viên fail, cả chuỗi
  rollback và mọi thành viên trả `LINKED_EVENT_FAILED` (mã lỗi thật báo ở thành
  viên gây lỗi). `EXISTS` (idempotent) tính là thành công trong chuỗi.

### Idempotency

Account và transfer đều idempotent theo `id`. Retry create với field giống hệt →
`EXISTS` (không đổi gì); field khác → `EXISTS_WITH_DIFFERENT_FIELDS` (từ chối).
Đây là nền cho at-least-once delivery từ hệ thống thượng nguồn.

### Bảo đảm & invariant

- **Atomicity:** một transfer atomic; một chuỗi `LINKED` atomic; batch không-linked
  **không** atomic toàn cục.
- **Durability:** chỉ trả kết quả sau khi lệnh đã fsync vào journal. Crash mất tối
  đa phần đuôi chưa ack.
- **Idempotency:** retry an toàn theo `id`.
- **Determinism:** cùng dòng lệnh luôn ra cùng state — nền cho recovery và audit.
- **Double-entry:** nợ và có luôn bằng và đối nhau; trial balance per-ledger
  (`GET /v1/trial-balance`) làm điều này quan sát được.
- **Overflow-safe:** transfer làm tràn counter u64 bị từ chối (`OVERFLOWS_*`) thay
  vì âm thầm hỏng số dư.
- **Pending expiry:** pending hết hạn được auto-void và nhả tiền, tất định
  (replay được).

## HTTP API

| Method & path | Mục đích |
|---|---|
| `POST /v1/accounts` | Tạo lô account — trả mã kết quả mỗi account |
| `POST /v1/transfers` | Tạo lô transfer (có thể chứa linked chain) — trả mã mỗi transfer |
| `GET /v1/accounts/{id}` | Snapshot account (số dư + field), hoặc 404 |
| `GET /v1/transfers/{id}` | Một transfer, hoặc 404 |
| `GET /v1/accounts/{id}/transfers?limit=N` | Lịch sử giao dịch, mới nhất trước (mặc định 100, tối đa 1000) |
| `GET /v1/trial-balance` | Tổng counter theo ledger; `balanced` = posted debits có khớp posted credits |

Endpoint batch trả về **tên** mã kết quả mỗi phần tử, đúng thứ tự request. Batch
**không** atomic toàn cục — chỉ chuỗi `LINKED` bên trong mới atomic.

> `id` (account/transfer/pendingId) là **chuỗi**: số thập phân không dấu hoặc UUID
> (vd `"1001"` hoặc `"6f9619ff-8b86-d011-b42d-00cf4fc964ff"`). `pendingId` để `"0"`
> nếu không phải post/void. Cờ transfer: `LINKED=1`, `PENDING=2`, `POST_PENDING=4`,
> `VOID_PENDING=8`.

### Mã kết quả

`CreateAccountResult` / `CreateTransferResult` theo bộ mã double-entry chuẩn. Nhóm
mã transfer:

- **Kết quả:** `OK`, `EXISTS`, `EXISTS_WITH_DIFFERENT_FIELDS`, `LINKED_EVENT_FAILED`
- **Validate field:** `*_MUST_NOT_BE_ZERO`, `ACCOUNTS_MUST_BE_DIFFERENT`,
  `FLAGS_ARE_MUTUALLY_EXCLUSIVE`
- **Tham chiếu:** `DEBIT/CREDIT_ACCOUNT_NOT_FOUND`,
  `ACCOUNTS_MUST_HAVE_SAME_LEDGER`, `TRANSFER_MUST_HAVE_SAME_LEDGER_AS_ACCOUNTS`
- **Trạng thái account:** `DEBIT/CREDIT_ACCOUNT_CLOSED`
- **Số dư:** `EXCEEDS_DEBITS_LIMIT`, `EXCEEDS_CREDITS_LIMIT`, `OVERFLOWS_*`
- **Two-phase:** `PENDING_TRANSFER_NOT_FOUND`, `PENDING_TRANSFER_NOT_PENDING`,
  `PENDING_TRANSFER_EXPIRED`, `EXCEEDS_PENDING_TRANSFER_AMOUNT`,
  `PENDING_TRANSFER_HAS_DIFFERENT_*`

Một số cờ/mã được khai báo cho tương thích giao thức nhưng **chưa enforce**:
`BALANCING_DEBIT/CREDIT`, `CLOSING_DEBIT/CREDIT`, `HISTORY` (CDC), và
`PENDING_TRANSFER_HAS_DIFFERENT_*`. Chi tiết & gap: xem
[ARCHITECTURE.md → Tích hợp nền tảng](ARCHITECTURE.md#11-tích-hợp-nền-tảng-platform-integration).

### Ví dụ (ledger VND = 704)

```bash
# 1) Tạo 2 account (ví khách; flags:0 cho gọn — production nên đặt cờ giới hạn)
curl -s -X POST localhost:8081/v1/accounts -H 'Content-Type: application/json' \
  -d '{"accounts":[
    {"id":"1001","ledger":704,"code":10,"flags":0,"userData64":0,"userData32":0},
    {"id":"1002","ledger":704,"code":10,"flags":0,"userData64":0,"userData32":0}
  ]}'
# → ["OK","OK"]   (gọi lại với cùng field → ["EXISTS","EXISTS"])

# 2) Chuyển 1000 từ 1001 → 1002
curl -s -X POST localhost:8081/v1/transfers -H 'Content-Type: application/json' \
  -d '{"transfers":[
    {"id":"5001","debitAccountId":"1001","creditAccountId":"1002","amount":1000,
     "pendingId":"0","userData64":0,"userData32":0,"timeoutSeconds":0,
     "ledger":704,"code":720,"flags":0}
  ]}'
# → ["OK"]

# 3) Xem số dư, lịch sử, trial balance
curl -s localhost:8081/v1/accounts/1001                       # debitsPosted=1000
curl -s 'localhost:8081/v1/accounts/1001/transfers?limit=10'  # [transfer 5001]
curl -s localhost:8081/v1/trial-balance                       # ledger 704: balanced=true

# 4) Hai pha — authorize rồi capture một phần (giữ 1000, chốt 800, nhả 200)
curl -s -X POST localhost:8081/v1/transfers -H 'Content-Type: application/json' \
  -d '{"transfers":[{"id":"6001","debitAccountId":"1001","creditAccountId":"1002",
     "amount":1000,"pendingId":"0","userData64":0,"userData32":0,
     "timeoutSeconds":0,"ledger":704,"code":720,"flags":2}]}'   # PENDING (flags=2)
curl -s -X POST localhost:8081/v1/transfers -H 'Content-Type: application/json' \
  -d '{"transfers":[{"id":"6002","debitAccountId":"1001","creditAccountId":"1002",
     "amount":800,"pendingId":"6001","userData64":0,"userData32":0,
     "timeoutSeconds":0,"ledger":704,"code":720,"flags":4}]}'   # POST_PENDING (flags=4)
# → 1001: debitsPending=0, debitsPosted += 800
```

### Xem OpenAPI / Swagger

Đặc tả OpenAPI 3 đầy đủ: [`docs/openapi.yaml`](docs/openapi.yaml).

- **Online:** mở <https://editor.swagger.io> → *File → Import file* → chọn `docs/openapi.yaml`.
- **Swagger UI (Docker):**
  ```bash
  docker run --rm -p 8088:8080 -e SWAGGER_JSON=/spec/openapi.yaml \
    -v "$PWD/docs:/spec" swaggerapi/swagger-ui   # mở http://localhost:8088
  ```
- **Redoc:** `npx --yes @redocly/cli preview-docs docs/openapi.yaml`

## Hiệu năng

Một writer thread + fsync gộp. Throughput bị chặn bởi:

1. **Độ trễ fsync** — NVMe cục bộ ≈ 50-100 µs/fsync. Batch 100 → 1k batch/s × 100
   = **100k transfer/s**.
2. **Throughput Disruptor** — trên 1M event/s trên phần cứng phổ thông. Không phải
   nút cổ chai.
3. **State mutation** — thao tác primitive array + 1 HashMap lookup ≈ 50 ns/transfer.
   Không phải nút cổ chai.

Tắt fsync (`fsyncBatch: false`, chỉ để test không bền) đẩy throughput vượt
500k/s. Production thật có fsync = kỳ vọng ~100-300k transfer/s tùy đĩa.

## Khi nào dùng cái này vs thay thế

- **Dùng cái này** nếu muốn bảo đảm kiểu ledger trong Java thuần, dữ liệu vừa RAM,
  chấp nhận single-node (hoặc bật module Raft cho HA). Command journaling,
  LSM-backed snapshot, lịch sử account đã có sẵn.
- **Dùng ledger/CSDL phân tán trưởng thành** nếu cần consensus replication tích
  hợp, LSM on-disk, và tính đúng đã kiểm bằng simulator mà không phải tự xây.
- **Dùng Postgres** nếu throughput dưới 5k tps và đội muốn scale theo vận hành
  thay vì đưa hạ tầng tùy biến vào.

## Hạn chế vận hành cần lấp

1. **HA** — bật module Raft (`raft/`) hoặc bọc primary-backup dùng journal làm
   stream replication. (Raft đã implement & test failover; chưa wire vào HTTP.)
2. **Cắt journal** — command log lớn vô hạn; một checkpoint làm mọi thứ trước
   offset của nó dư thừa, có thể thu hồi segment cũ sau snapshot. Chưa làm.
3. **Checkpoint ngoài thread writer** — `runCheckpoint` hiện snapshot trên thread
   writer, làm dừng ghi trong thời gian đó. Production cần copy-on-write/background.
4. **CDC** — thêm listener khi commit, bơm event sang Kafka. Hook ở cuối
   `LedgerEventHandler.onEvent` (endOfBatch).
5. **Metrics** — Micrometer có sẵn qua actuator; instrument state machine bằng
   counter/histogram.
6. **Recovery testing** — fault injection trong WAL (lật bit, cắt đuôi, replay
   segment) để xác minh CRC thực sự bắt được hỏng.

## Tài liệu

- [ARCHITECTURE.md](ARCHITECTURE.md) — nội bộ engine, mô hình thread, recovery,
  LSM, quyết định thiết kế & lý do, HA Raft, và map nền tảng thanh toán vào ledger
  (chart of accounts, business flow → bút toán, sequence diagram, walkthrough,
  verification & gap).
- [docs/openapi.yaml](docs/openapi.yaml) — đặc tả HTTP API.

## License

Đây là code mẫu. Coi như điểm khởi đầu, chưa sẵn sàng production. Triển khai
production thật cần lấp các khoảng trống ở trên cộng với testing, observability và
runbook vận hành của riêng bạn.
