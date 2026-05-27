# Getting started

Chạy thử ledger core tại máy và gọi HTTP API bằng `curl`.

## Yêu cầu

- **JDK 21** (project dùng record + pattern hiện đại; build sẽ báo "release version
  21 not supported" nếu JDK mặc định cũ hơn).
- **Maven 3.9+** (chưa có Maven wrapper trong repo).

```bash
# macOS — trỏ JAVA_HOME sang JDK 21 nếu mặc định đang là bản cũ:
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
java -version   # phải in 21.x
```

## Chạy service

```bash
# cách 1: chạy trực tiếp
mvn spring-boot:run

# cách 2: đóng gói rồi chạy jar
mvn clean package
java -jar target/htt-core-payment-0.1.0-SNAPSHOT.jar
```

Khi log in `LedgerEngine ready: accounts=... ringBufferSize=...` là service đã sẵn
sàng. Kiểm tra health:

```bash
curl -s localhost:8081/actuator/health      # {"status":"UP"}
```

## Cấu hình chính

Mặc định trong [`application.yml`](../src/main/resources/application.yml) (override
bằng env/flag Spring Boot):

| Khóa | Mặc định | Ý nghĩa |
|---|---|---|
| `server.port` | `8081` | cổng HTTP |
| `ledger.data-dir` | `./data` | nơi lưu journal (WAL) + snapshot. Xóa thư mục này để reset sạch state |
| `ledger.ring-buffer-size` | `1048576` | số slot Disruptor (đầy → HTTP 429) |
| `ledger.max-accounts` / `max-transfers` | 10M / 100M | dung lượng store cấp phát trước |
| `ledger.snapshot.interval-seconds` | `300` | chu kỳ checkpoint |

## Endpoints

| Method & path | Mục đích |
|---|---|
| `POST /v1/accounts` | Tạo lô account — trả mã kết quả mỗi account |
| `POST /v1/transfers` | Tạo lô transfer (có thể chứa linked chain) — trả mã mỗi transfer |
| `GET /v1/accounts/{id}` | Snapshot account (số dư + field), hoặc 404 |
| `GET /v1/transfers/{id}` | Một transfer, hoặc 404 |
| `GET /v1/accounts/{id}/transfers?limit=N` | Lịch sử giao dịch của account, mới nhất trước (mặc định 100, tối đa 1000) |
| `GET /v1/trial-balance` | Tổng counter theo ledger; `balanced` = posted debits có khớp posted credits |

> `id` (account/transfer/pendingId) là **chuỗi**: số thập phân không dấu hoặc UUID
> (vd `"1001"` hoặc `"6f9619ff-8b86-d011-b42d-00cf4fc964ff"`). `pendingId` để `"0"`
> nếu không phải post/void. Cờ transfer: `LINKED=1`, `PENDING=2`, `POST_PENDING=4`,
> `VOID_PENDING=8`.

## Thử nhanh (ledger VND = 704)

**1. Tạo 2 account** (ví khách; ở đây để `flags:0` cho gọn — production nên đặt cờ
giới hạn số dư, xem [chart of accounts](ledger-core/primitives.md)):

```bash
curl -s -X POST localhost:8081/v1/accounts \
  -H 'Content-Type: application/json' \
  -d '{"accounts":[
    {"id":"1001","ledger":704,"code":10,"flags":0,"userData64":0,"userData32":0},
    {"id":"1002","ledger":704,"code":10,"flags":0,"userData64":0,"userData32":0}
  ]}'
# → ["OK","OK"]   (gọi lại lần 2 với cùng field → ["EXISTS","EXISTS"])
```

**2. Chuyển 1000 từ 1001 → 1002:**

```bash
curl -s -X POST localhost:8081/v1/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transfers":[
    {"id":"5001","debitAccountId":"1001","creditAccountId":"1002","amount":1000,
     "pendingId":"0","userData64":0,"userData32":0,"timeoutSeconds":0,
     "ledger":704,"code":720,"flags":0}
  ]}'
# → ["OK"]
```

**3. Xem số dư, lịch sử, trial balance:**

```bash
curl -s localhost:8081/v1/accounts/1001                       # debitsPosted=1000
curl -s 'localhost:8081/v1/accounts/1001/transfers?limit=10'  # [transfer 5001]
curl -s localhost:8081/v1/trial-balance                       # ledger 704: balanced=true
```

**4. Hai pha — authorize rồi capture một phần** (giữ 1000, chốt 800, nhả 200):

```bash
# PENDING (flags=2) — giữ tiền:
curl -s -X POST localhost:8081/v1/transfers -H 'Content-Type: application/json' \
  -d '{"transfers":[
    {"id":"6001","debitAccountId":"1001","creditAccountId":"1002","amount":1000,
     "pendingId":"0","userData64":0,"userData32":0,"timeoutSeconds":0,
     "ledger":704,"code":720,"flags":2}
  ]}'
# → ["OK"]

# POST_PENDING (flags=4) — chốt 800, phần dư 200 tự nhả:
curl -s -X POST localhost:8081/v1/transfers -H 'Content-Type: application/json' \
  -d '{"transfers":[
    {"id":"6002","debitAccountId":"1001","creditAccountId":"1002","amount":800,
     "pendingId":"6001","userData64":0,"userData32":0,"timeoutSeconds":0,
     "ledger":704,"code":720,"flags":4}
  ]}'
# → ["OK"]   (account 1001: debitsPending=0, debitsPosted += 800)
```

## Điều cần nhớ

- **Batch không nguyên tử toàn cục** — chỉ `LINKED` chain bên trong batch mới
  all-or-nothing. Mỗi phần tử có mã kết quả riêng theo đúng thứ tự gửi lên.
- **Idempotency** — gửi lại cùng `id` + cùng field → `EXISTS` (không áp dụng 2 lần);
  khác field → `EXISTS_WITH_DIFFERENT_FIELDS` (từ chối). An toàn để retry.
- **HTTP 429** — ring buffer đầy (writer đang chậm) → client nên backoff và thử lại.
- Ý nghĩa từng flag/mã kết quả: xem [business.md](business.md). Map vào business flow
  của nền tảng: xem [Ledger core — use cases & flows](ledger-core/README.md).
