# Ledger Core — Use-case mapping & verification

Tài liệu này xác định **ranh giới của repo** và **verify** rằng ledger core (repo
`hft-core-payment`) biểu diễn được mọi business flow của nền tảng thanh toán
(hệ sinh thái Vin) bằng các nguyên thủy (primitive) sẵn có — hoặc chỉ rõ gap.

> Phạm vi: repo này **chỉ là LEDGER CORE** — nguồn sự thật về số dư, double-entry,
> deterministic. Các module **Payment Orchestration**, **Bank/Core-Banking
> Connector**, **Identity/KYC**, **Merchant onboarding**, **Integration Hub**
> nằm ở các repo khác. Ở đây chúng chỉ xuất hiện ở mức *ranh giới* (ai gọi vào,
> ledger trả về gì).

---

## 1. Bức tranh tổng thể (mức ranh giới)

```
   Identity/KYC ──(KYC tier ⇒ hạn mức)──┐
                                        ▼
 Platform services ─▶ Payment Orchestration ─▶  ┌────────────────────┐
                          │  (saga, outbox)      │  LEDGER CORE       │
                          │                      │  (repo này)        │
 Bank/Core Banking ◀──────┤  lệnh đi tiền        │  - tài khoản+số dư │
 Billers / Insurance ◀────┤  (async, dễ lỗi)     │  - double-entry    │
                          │                      │  - pending 2 pha   │
                          └──ghi kết quả bằng───▶ │  - linked chain    │
                             bút toán             │  = NGUỒN SỰ THẬT   │
                                                  └─────────┬──────────┘
 Reconciliation ◀── đối soát ledger ⇄ bank/provider ────────┘
```

**Hợp đồng (contract) của ledger core với thế giới bên ngoài:**
- Đầu vào: lệnh tạo `Account` / `Transfer` theo lô (batch), idempotent theo `id`.
- Đầu ra: mã kết quả cho từng phần tử (`OK` / `EXISTS` / mã lỗi).
- Ledger **không** gọi ra ngoài, **không** đọc đồng hồ thực, **không** biết về
  bank/provider. Mọi việc async do Orchestration lo; ledger chỉ *ghi sự thật*.

---

## 2. Nguyên thủy mà ledger core cung cấp (đã verify trong code)

| Primitive | Mô tả | Vị trí | Trạng thái |
|---|---|---|---|
| **Standard transfer** | 1 debit + 1 credit, posted ngay | `LedgerStateMachine.tryApplyTransfer` | ✅ |
| **Two-phase pending** | PENDING giữ tiền → POST_PENDING chốt (cho phép chốt một phần) / VOID_PENDING nhả | `applyPostOrVoid` | ✅ |
| **Linked chain** | chuỗi transfer all-or-nothing, rollback đầy đủ | `processChain` + `ChainUndo` | ✅ |
| **Balance limit** | chặn âm/quá hạn mức (`EXCEEDS_*_LIMIT`) | `AccountStore.wouldExceed*Limit` | ✅ |
| **Overflow guard** | từ chối transfer làm tràn u64 | `addOverflows` | ✅ |
| **Idempotency** | `id` trùng + cùng field → `EXISTS` | `tryApplyTransfer` (so sánh `.equals`) | ✅ |
| **Pending expiry** | auto-void pending hết hạn, replay được (lệnh `EXPIRE`) | `expirePending` | ✅ |
| **Account history** | secondary index `account → transfers`, newest-first | `AccountTransferIndex` | ✅ |
| **Trial balance** | tổng counter theo ledger (kiểm money conservation) | `trialBalance` | ✅ |

Ghi chú: `id` là **UInt128** (UUID/ULID-friendly) → cho phép Orchestration sinh id
bên ngoài, không cần khóa tuần tự tập trung. `amount` là **u64** (đủ cho VND).
Mỗi **currency = một `ledger` id** (vd VND = 704); một transfer chỉ di chuyển trong
cùng một ledger.

---

## 3. Chart of accounts đề xuất (mapping cho nền tảng)

Ledger không có "loại tài khoản" first-class — integrator tự mã hóa qua `code` +
chọn cờ giới hạn số dư đúng bản chất (xem business.md §2).

| Tài khoản | Bản chất | Cờ cần đặt | Đọc số dư |
|---|---|---|---|
| `wallet:{userId}` | Liability (credit-normal) | `DEBITS_MUST_NOT_EXCEED_CREDITS` → **chống âm ví** | `creditBalance()` |
| `merchant:{id}` | Liability | `DEBITS_MUST_NOT_EXCEED_CREDITS` | `creditBalance()` |
| `bank_settlement` (tài khoản đảm bảo) | Asset (debit-normal) | `CREDITS_MUST_NOT_EXCEED_DEBITS` | `debitBalance()` |
| `revenue:fee` | Income | `DEBITS_MUST_NOT_EXCEED_CREDITS` | `creditBalance()` |
| `clearing:payout` / `clearing:deposit` | Trung gian | *(không cờ — swing 2 chiều)* | tùy |
| `provider_clearing:{biller/insurer}` | Trung gian | *(không cờ)* | tùy |
| `suspense` | Trung gian (tiền lệch) | *(không cờ)* | tùy |

**Invariant money conservation** (kiểm qua `GET /v1/trial-balance`):
`Σ wallet + Σ merchant + Σ revenue (liability/income) ≈ Σ bank_settlement (asset) ± Σ clearing`.

---

## 4. Verify từng business flow → bút toán ledger

Ký hiệu: `Dr X / Cr Y (amount) [flag]`. Mỗi dòng trong một lô; nhiều dòng `[LINKED]`
liên tiếp = một chuỗi nguyên tử.

### 4.1 Nạp tiền (deposit qua Virtual Account / VietQR) — ✅
Tiền *đã* về tài khoản đảm bảo rồi mới ghi → không cần pending.
```
Dr bank_settlement / Cr wallet:user   (số nạp)
```
Primitive: **standard transfer**. Bank webhook do Connector nhận; Orchestration gọi ledger.

### 4.2 Thanh toán dịch vụ nội bộ bằng ví, có phí — ✅
```
Dr wallet:user   / Cr merchant:m     (giá)  [LINKED]
Dr merchant:m    / Cr revenue:fee    (phí)
```
Primitive: **linked chain** (atomic). Âm ví bị chặn bởi cờ trên `wallet` →
`EXCEEDS_DEBITS_LIMIT`. Nếu leg phí fail, cả chuỗi rollback (đã verify ở
`LedgerStateMachineTest.failedLinkedChainRollsBackEverything`).

### 4.3 Giữ tiền / pre-authorization (đặt cọc, tạm giữ) — ✅
```
PENDING:  Dr wallet:user / Cr merchant_clearing  (số giữ) [PENDING, timeoutSeconds=...]
Capture:  POST_PENDING  pendingId=...  (≤ số giữ)   → phần dư tự nhả
Release:  VOID_PENDING  pendingId=...
```
Primitive: **two-phase**. Chốt một phần được hỗ trợ (business.md §3, ví dụ "authorize then capture").

### 4.4 Rút tiền / payout ra bank thật (async) — ✅ (2 bút toán)
```
① Reserve (lúc nhận lệnh):
   Dr wallet:user / Cr clearing:payout   (số rút) [PENDING]
② Connector gọi bank (repo khác) ───────────────────────────
③a Bank OK:
   POST_PENDING pendingId=①            → chốt wallet→clearing:payout
   Dr clearing:payout / Cr bank_settlement (số rút)  ← tiền thật rời tài khoản đảm bảo
③b Bank FAIL/timeout:
   VOID_PENDING pendingId=①            → nhả tiền lại ví khách
```
Primitive: **two-phase + standard**. Đây là chỗ pending phát huy: khách không bị
"bốc hơi" tiền khi bank đang xử lý; thất bại thì hoàn nguyên tự động.

### 4.5 Hoàn tiền (refund một giao dịch đã posted) — ✅
Ledger append-only ⇒ **không sửa/void transfer posted cũ**; refund là bút toán đảo MỚI:
```
Dr merchant:m / Cr wallet:user   (số hoàn)
```
(VOID chỉ dành cho *pending*; refund của giao dịch đã chốt = transfer ngược.)

### 4.6 Chuyển tiền P2P (ví → ví) — ✅
```
Dr wallet:A / Cr wallet:B   (số chuyển)
```
Âm ví A bị chặn bởi cờ giới hạn.

### 4.7 Thanh toán hóa đơn (biller) / mua bảo hiểm (provider async) — ✅
Giống payout nhưng đích là clearing của provider:
```
PENDING: Dr wallet:user / Cr provider_clearing:{x}  [PENDING]
OK → POST_PENDING ;  FAIL → VOID_PENDING
```
Primitive: **two-phase**.

### 4.8 Settlement merchant (gom doanh thu → payout T+n) — ✅
Số dư `merchant:m` tích lũy theo thời gian; đến kỳ chạy flow payout (4.4) với
nguồn là `merchant:m`. **Merchant nội bộ Vin** có thể chỉ netting nội bộ (bút toán
giữa các tài khoản nội bộ, không đi tiền ra bank) → tiết kiệm phí giao dịch.

### 4.9 Đa tiền tệ / FX (nếu cần: VND ↔ ngoại tệ / điểm thưởng) — ⚠️ qua pattern
Một transfer chỉ trong **cùng ledger**. FX = **linked 2 leg khác ledger**:
```
Dr wallet:user(VND) / Cr fx_clearing(VND)  (X) [LINKED]   ← ledger VND
Dr fx_clearing(USD) / Cr wallet:user(USD)  (Y)            ← ledger USD
```
Mỗi leg cùng-ledger nên hợp lệ; `processChain` xử lý từng transfer độc lập nên chuỗi
spanning 2 ledger vẫn nguyên tử. Đây là **pattern**, không phải FX atomic gốc — chấp
nhận được, nhưng cần Orchestration đảm bảo tỷ giá X/Y nhất quán.

---

## 5. Bảng tổng hợp verify

| Business flow | Primitive | Hỗ trợ |
|---|---|---|
| Nạp tiền (deposit) | standard | ✅ |
| Thanh toán nội bộ + phí | linked chain | ✅ |
| Giữ tiền / pre-auth / capture một phần | two-phase | ✅ |
| Rút tiền / payout ra bank | two-phase + standard | ✅ |
| Hoàn tiền (refund) | standard (đảo) | ✅ |
| P2P ví → ví | standard | ✅ |
| Hóa đơn / bảo hiểm (provider async) | two-phase | ✅ |
| Settlement merchant / netting nội bộ | tích lũy + payout | ✅ |
| Chống âm ví / hạn mức | balance-limit flag | ✅ |
| Idempotent retry (at-least-once từ Orchestration) | `EXISTS` theo id | ✅ |
| Pending hết hạn tự nhả | `EXPIRE` (replay được) | ✅ |
| Đa tiền tệ / FX | linked 2-leg pattern | ⚠️ qua pattern |
| Đóng ví/tài khoản bằng transfer | `CLOSING_DEBIT/CREDIT` | ❌ gap |
| "Quét" hết số dư khả dụng | `BALANCING_DEBIT/CREDIT` | ❌ gap |
| Stream giao dịch ra ngoài (CDC) | `HISTORY` | ❌ gap |

---

## 6. Gap analysis — cần bổ sung ở ledger core để adapt đầy đủ

Sắp theo mức ưu tiên cho nền tảng:

1. **`CLOSING_DEBIT` / `CLOSING_CREDIT` (đóng tài khoản)** — *khai báo, chưa enforce*.
   Cần cho đóng ví/tài khoản khi user/merchant rời nền tảng. Hiện `CLOSED` chỉ đặt
   được lúc tạo. → cần xử lý cờ này trong `tryApplyTransfer` để set `CLOSED` qua một
   transfer.
2. **Validate `PENDING_TRANSFER_HAS_DIFFERENT_*`** — *chưa produce*. Khi POST/VOID,
   ledger lấy thẳng account/ledger của transfer gốc thay vì đối chiếu giá trị bên gọi
   gửi lên. Nên đối chiếu để bắt lỗi Orchestration gửi sai (an toàn hơn cho tiền thật).
3. **`HISTORY` + CDC stream** — *chưa có*. Reconciliation/notification/audit downstream
   cần stream sự kiện. Hiện history chỉ truy vấn được qua secondary index (đủ cho
   lookup, không phải stream). → cần outbox/CDC từ journal.
4. **HA / replication** — single-node. Quy mô hệ sinh thái Vin cần replication
   (Raft/Apache Ratis) hoặc active-passive + snapshot DR. *Không ảnh hưởng tính đúng
   của flow*, nhưng bắt buộc cho production.
5. **`BALANCING_DEBIT` / `BALANCING_CREDIT`** — *khai báo, chưa enforce*. Cho phép
   "chuyển tối đa theo số dư khả dụng" (auto top-up, quét sạch ví khi đóng). Nice-to-have.
6. **FX atomic gốc** — hiện chỉ qua linked 2-leg. Cân nhắc khi đa tiền tệ thành yêu cầu
   thật (design.md §5 ghi lý do hoãn u128 *amounts*/FX).

Những phần **không** cần làm trong repo này (thuộc repo khác): orchestration saga,
outbox gọi bank, KYC-tier→hạn mức (ledger chỉ enforce balance-limit; hạn mức theo
KYC do Orchestration kiểm trước khi gọi vào), connector core banking, đối soát.

---

## 7. Kết luận

Ledger core **đáp ứng được toàn bộ flow tiền cốt lõi** (nạp/chi/giữ/rút/hoàn/P2P/
settlement) bằng 3 primitive: standard transfer, two-phase pending, linked chain —
cộng balance-limit để chống âm và idempotency để retry an toàn. Các điểm còn thiếu
(`CLOSING`, validate pending-different, CDC, HA, `BALANCING`) đều là **bổ sung gia
cố**, không phải thay đổi mô hình. Mô hình double-entry + determinism là nền vững để
mở rộng.
