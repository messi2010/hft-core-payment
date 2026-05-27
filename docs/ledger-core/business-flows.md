# Business flows → bút toán ledger

Ký hiệu: `Dr X / Cr Y (amount) [flag]`. Mỗi dòng trong một lô; nhiều dòng `[LINKED]`
liên tiếp = một chuỗi nguyên tử. Sơ đồ tuần tự tương ứng: xem
[Sequence diagrams](sequence-diagrams.md).

## 4.1 Nạp tiền (deposit qua Virtual Account / VietQR) — ✅
Tiền *đã* về tài khoản đảm bảo rồi mới ghi → không cần pending.
```
Dr bank_settlement / Cr wallet:user   (số nạp)
```
Primitive: **standard transfer**. Bank webhook do Connector nhận; Orchestration gọi ledger.

## 4.2 Thanh toán dịch vụ nội bộ bằng ví, có phí — ✅
```
Dr wallet:user   / Cr merchant:m     (giá)  [LINKED]
Dr merchant:m    / Cr revenue:fee    (phí)
```
Primitive: **linked chain** (atomic). Âm ví bị chặn bởi cờ trên `wallet` →
`EXCEEDS_DEBITS_LIMIT`. Nếu leg phí fail, cả chuỗi rollback (đã verify ở
`LedgerStateMachineTest.failedLinkedChainRollsBackEverything`).

## 4.3 Giữ tiền / pre-authorization (đặt cọc, tạm giữ) — ✅
```
PENDING:  Dr wallet:user / Cr merchant_clearing  (số giữ) [PENDING, timeoutSeconds=...]
Capture:  POST_PENDING  pendingId=...  (≤ số giữ)   → phần dư tự nhả
Release:  VOID_PENDING  pendingId=...
```
Primitive: **two-phase**. Chốt một phần được hỗ trợ (business.md §3, ví dụ "authorize then capture").

## 4.4 Rút tiền / payout ra bank thật (async) — ✅ (2 bút toán)
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

## 4.5 Hoàn tiền (refund một giao dịch đã posted) — ✅
Ledger append-only ⇒ **không sửa/void transfer posted cũ**; refund là bút toán đảo MỚI:
```
Dr merchant:m / Cr wallet:user   (số hoàn)
```
(VOID chỉ dành cho *pending*; refund của giao dịch đã chốt = transfer ngược.)

## 4.6 Chuyển tiền P2P (ví → ví) — ✅
```
Dr wallet:A / Cr wallet:B   (số chuyển)
```
Âm ví A bị chặn bởi cờ giới hạn.

## 4.7 Thanh toán hóa đơn (biller) / mua bảo hiểm (provider async) — ✅
Giống payout nhưng đích là clearing của provider:
```
PENDING: Dr wallet:user / Cr provider_clearing:{x}  [PENDING]
OK → POST_PENDING ;  FAIL → VOID_PENDING
```
Primitive: **two-phase**.

## 4.8 Settlement merchant (gom doanh thu → payout T+n) — ✅
Số dư `merchant:m` tích lũy theo thời gian; đến kỳ chạy flow payout (4.4) với
nguồn là `merchant:m`. **Merchant nội bộ** có thể chỉ netting nội bộ (bút toán
giữa các tài khoản nội bộ, không đi tiền ra bank) → tiết kiệm phí giao dịch.

## 4.9 Đa tiền tệ / FX (nếu cần: VND ↔ ngoại tệ / điểm thưởng) — ⚠️ qua pattern
Một transfer chỉ trong **cùng ledger**. FX = **linked 2 leg khác ledger**:
```
Dr wallet:user(VND) / Cr fx_clearing(VND)  (X) [LINKED]   ← ledger VND
Dr fx_clearing(USD) / Cr wallet:user(USD)  (Y)            ← ledger USD
```
Mỗi leg cùng-ledger nên hợp lệ; `processChain` xử lý từng transfer độc lập nên chuỗi
spanning 2 ledger vẫn nguyên tử. Đây là **pattern**, không phải FX atomic gốc — chấp
nhận được, nhưng cần Orchestration đảm bảo tỷ giá X/Y nhất quán.
