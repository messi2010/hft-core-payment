# Ledger core — use cases & flows

Phần này xác định **ranh giới của repo** và **verify** rằng ledger core (repo
`hft-core-payment`) biểu diễn được mọi business flow của nền tảng thanh toán
bằng các nguyên thủy (primitive) sẵn có — hoặc chỉ rõ gap.

> Phạm vi: repo này **chỉ là LEDGER CORE** — nguồn sự thật về số dư, double-entry,
> deterministic. Các module **Payment Orchestration**, **Bank/Core-Banking
> Connector**, **Identity/KYC**, **Merchant onboarding**, **Integration Hub**
> nằm ở các repo khác. Ở đây chúng chỉ xuất hiện ở mức *ranh giới* (ai gọi vào,
> ledger trả về gì).

## Bức tranh tổng thể (mức ranh giới)

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

## Trong phần này

* [Ledger primitives & accounts](primitives.md) — primitive sẵn có (đã verify trong code) + chart of accounts đề xuất.
* [Business flows → postings](business-flows.md) — map từng business flow vào bút toán double-entry.
* [Sequence diagrams](sequence-diagrams.md) — sơ đồ tuần tự từng flow (Mermaid).
* [Verification & gaps](verification.md) — bảng tổng hợp verify, gap analysis, kết luận.
