# Sequence diagrams từng flow (Mermaid)

Các diagram dưới đây tập trung vào **tương tác với ledger core** (chi tiết bút toán:
xem [Business flows → postings](business-flows.md)). Quy ước participant:
`Svc` = platform service, `Orch` = Payment Orchestration (repo khác),
`Ledger` = ledger core (repo này), `Bank` = Bank Connector (repo khác),
`Prov` = Biller/Insurer Connector (repo khác). Mọi lời gọi vào ledger đều là
`createTransfers([...])` idempotent theo transfer id; bút toán đặt trong `Note`.

## 8.1 Nạp tiền (deposit)

```mermaid
sequenceDiagram
    autonumber
    participant Bank
    participant Orch
    participant Ledger
    Bank->>Orch: webhook "tiền về" (user, amount)
    Orch->>Ledger: createTransfers (deposit)
    Note over Ledger: standard transfer<br/>Dr bank_settlement / Cr wallet:user (amount)
    Ledger-->>Orch: [OK]
    Orch-->>Bank: 200 ack
```

## 8.2 Thanh toán nội bộ + phí (linked, atomic)

```mermaid
sequenceDiagram
    autonumber
    participant Svc
    participant Orch
    participant Ledger
    Svc->>Orch: thanh toán dịch vụ (user, merchant, price, fee)
    Orch->>Ledger: createTransfers (linked chain)
    Note over Ledger: [LINKED] Dr wallet:user / Cr merchant:m (price)<br/>Dr merchant:m / Cr revenue:fee (fee)
    alt đủ số dư
        Note over Ledger: cả chuỗi posted nguyên tử
        Ledger-->>Orch: [OK, OK]
        Orch-->>Svc: thành công
    else thiếu số dư ví
        Note over Ledger: leg1 EXCEEDS_DEBITS_LIMIT<br/>→ rollback toàn chuỗi
        Ledger-->>Orch: [EXCEEDS_DEBITS_LIMIT, LINKED_EVENT_FAILED]
        Orch-->>Svc: từ chối
    end
```

## 8.3 Giữ tiền / pre-auth → capture / release (two-phase)

```mermaid
sequenceDiagram
    autonumber
    participant Svc
    participant Orch
    participant Ledger
    Svc->>Orch: giữ tiền (user, amount, timeout)
    Orch->>Ledger: createTransfers (PENDING)
    Note over Ledger: Dr wallet:user / Cr merchant_clearing (amount) [PENDING]<br/>wallet.debitsPending += amount
    Ledger-->>Orch: [OK] pendingId=P
    alt capture (chốt, có thể ≤ amount)
        Svc->>Orch: capture(P, captureAmount)
        Orch->>Ledger: createTransfers (POST_PENDING P)
        Note over Ledger: nhả pending, post captureAmount<br/>phần dư tự nhả
        Ledger-->>Orch: [OK]
    else release (hủy)
        Svc->>Orch: void(P)
        Orch->>Ledger: createTransfers (VOID_PENDING P)
        Note over Ledger: nhả toàn bộ pending
        Ledger-->>Orch: [OK]
    else hết hạn (không ai chốt)
        Note over Ledger: sweep EXPIRE auto-void → nhả tiền (replay được)
    end
```

## 8.4 Rút tiền / payout ra bank (async)

```mermaid
sequenceDiagram
    autonumber
    participant Svc
    participant Orch
    participant Ledger
    participant Bank
    Svc->>Orch: rút tiền (user, amount)
    Orch->>Ledger: createTransfers (PENDING — giữ tiền)
    Note over Ledger: Dr wallet:user / Cr clearing:payout (amount) [PENDING]
    Ledger-->>Orch: [OK] pendingId=P
    Orch->>Bank: chuyển khoản (amount) [async]
    alt bank OK
        Bank-->>Orch: success
        Orch->>Ledger: createTransfers (POST_PENDING + đẩy tiền rời bank)
        Note over Ledger: [LINKED] POST_PENDING P (amount)<br/>Dr clearing:payout / Cr bank_settlement (amount)
        Ledger-->>Orch: [OK, OK]
    else bank FAIL / timeout
        Bank-->>Orch: failure
        Orch->>Ledger: createTransfers (VOID_PENDING P)
        Note over Ledger: nhả tiền lại ví khách
        Ledger-->>Orch: [OK]
    end
```

## 8.5 Hoàn tiền (refund — bút toán đảo, KHÔNG void)

```mermaid
sequenceDiagram
    autonumber
    participant Svc
    participant Orch
    participant Ledger
    Svc->>Orch: hoàn tiền (giao dịch gốc, refundAmount)
    Note over Orch: giao dịch gốc đã posted → không void được<br/>tạo bút toán đảo MỚI (id mới)
    Orch->>Ledger: createTransfers (reverse)
    Note over Ledger: Dr merchant:m / Cr wallet:user (refundAmount)
    Ledger-->>Orch: [OK]
    Orch-->>Svc: hoàn tất
```

## 8.6 Chuyển tiền P2P (ví → ví)

```mermaid
sequenceDiagram
    autonumber
    participant Svc
    participant Orch
    participant Ledger
    Svc->>Orch: chuyển P2P (A, B, amount)
    Orch->>Ledger: createTransfers (P2P)
    Note over Ledger: Dr wallet:A / Cr wallet:B (amount)<br/>âm ví A bị chặn (EXCEEDS_DEBITS_LIMIT)
    Ledger-->>Orch: [OK] hoặc [EXCEEDS_DEBITS_LIMIT]
```

## 8.7 Hóa đơn / bảo hiểm (provider async)

```mermaid
sequenceDiagram
    autonumber
    participant Svc
    participant Orch
    participant Ledger
    participant Prov
    Svc->>Orch: thanh toán hóa đơn / mua bảo hiểm (user, amount)
    Orch->>Ledger: createTransfers (PENDING)
    Note over Ledger: Dr wallet:user / Cr provider_clearing (amount) [PENDING]
    Ledger-->>Orch: [OK] pendingId=P
    Orch->>Prov: gọi nhà cung cấp (amount) [async]
    alt provider OK
        Prov-->>Orch: success
        Orch->>Ledger: createTransfers (POST_PENDING + tiền rời bank)
        Note over Ledger: [LINKED] POST_PENDING P<br/>Dr provider_clearing / Cr bank_settlement (amount)
        Ledger-->>Orch: [OK, OK]
    else provider FAIL
        Prov-->>Orch: failure
        Orch->>Ledger: createTransfers (VOID_PENDING P)
        Note over Ledger: nhả tiền lại ví khách
        Ledger-->>Orch: [OK]
    end
```

## 8.8 Settlement merchant (T+n: payout ngoài vs netting nội bộ)

```mermaid
sequenceDiagram
    autonumber
    participant Cron as Scheduler
    participant Orch
    participant Ledger
    participant Bank
    Cron->>Orch: kỳ settlement (merchant m)
    Orch->>Ledger: lookupAccount(merchant:m)
    Ledger-->>Orch: creditBalance = S
    alt merchant ngoài → payout ra bank
        Orch->>Ledger: createTransfers (PENDING từ merchant)
        Note over Ledger: Dr merchant:m / Cr clearing:payout (S) [PENDING]
        Ledger-->>Orch: [OK] pendingId=P
        Orch->>Bank: chi trả (S)
        Bank-->>Orch: success
        Orch->>Ledger: createTransfers (POST_PENDING + tiền rời bank)
        Note over Ledger: [LINKED] POST_PENDING P<br/>Dr clearing:payout / Cr bank_settlement (S)
        Ledger-->>Orch: [OK, OK]
    else merchant nội bộ → netting
        Orch->>Ledger: createTransfers (nội bộ)
        Note over Ledger: Dr merchant:m / Cr internal_treasury (S)<br/>không đi tiền ra bank
        Ledger-->>Orch: [OK]
    end
```

## 8.9 Đổi tiền / FX (linked 2-leg cross-ledger)

```mermaid
sequenceDiagram
    autonumber
    participant Svc
    participant Orch
    participant Ledger
    Svc->>Orch: đổi tiền (user, X VND → Y USD theo rate)
    Orch->>Ledger: createTransfers (linked 2-leg, 2 ledger)
    Note over Ledger: [LINKED] Dr wallet:user(VND) / Cr fx_clearing(VND) (X)  — ledger=VND<br/>Dr fx_clearing(USD) / Cr wallet:user(USD) (Y)  — ledger=USD
    Note over Ledger: mỗi leg cùng-ledger nên hợp lệ;<br/>chuỗi spanning 2 ledger vẫn nguyên tử
    Ledger-->>Orch: [OK, OK]
```
