# Ledger primitives & accounts

## 1. Nguyên thủy mà ledger core cung cấp (đã verify trong code)

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

## 2. Chart of accounts đề xuất (mapping cho nền tảng)

Ledger không có "loại tài khoản" first-class — integrator tự mã hóa qua `code` +
chọn cờ giới hạn số dư đúng bản chất (xem [business.md](../business.md) §2).

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
