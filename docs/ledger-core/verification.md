# Verification & gap analysis

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
4. **HA / replication** — single-node. Quy mô nền tảng lớn cần replication
   (Raft/Apache Ratis) hoặc active-passive + snapshot DR. *Không ảnh hưởng tính đúng
   của flow*, nhưng bắt buộc cho production.
5. **`BALANCING_DEBIT` / `BALANCING_CREDIT`** — *khai báo, chưa enforce*. Cho phép
   "chuyển tối đa theo số dư khả dụng" (auto top-up, quét sạch ví khi đóng). Nice-to-have.
6. **FX atomic gốc** — hiện chỉ qua linked 2-leg. Cân nhắc khi đa tiền tệ thành yêu cầu
   thật ([design.md](../design.md) §5 ghi lý do hoãn u128 *amounts*/FX).

Những phần **không** cần làm trong repo này (thuộc repo khác): orchestration saga,
outbox gọi bank, KYC-tier→hạn mức (ledger chỉ enforce balance-limit; hạn mức theo
KYC do Orchestration kiểm trước khi gọi vào), connector core banking, đối soát.

## 7. Kết luận

Ledger core **đáp ứng được toàn bộ flow tiền cốt lõi** (nạp/chi/giữ/rút/hoàn/P2P/
settlement) bằng 3 primitive: standard transfer, two-phase pending, linked chain —
cộng balance-limit để chống âm và idempotency để retry an toàn. Các điểm còn thiếu
(`CLOSING`, validate pending-different, CDC, HA, `BALANCING`) đều là **bổ sung gia
cố**, không phải thay đổi mô hình. Mô hình double-entry + determinism là nền vững để
mở rộng.
