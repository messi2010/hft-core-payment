package com.payments.ledger.engine;

import com.payments.ledger.domain.Account;
import com.payments.ledger.domain.AccountFlags;
import com.payments.ledger.domain.AccountSnapshot;
import com.payments.ledger.domain.CreateAccountResult;
import com.payments.ledger.domain.CreateTransferResult;
import com.payments.ledger.domain.LedgerBalance;
import com.payments.ledger.domain.Transfer;
import com.payments.ledger.domain.TransferFlags;
import com.payments.ledger.domain.UInt128;
import com.payments.ledger.storage.AccountStore;
import com.payments.ledger.storage.PendingTransferIndex;
import com.payments.ledger.storage.TransferStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.payments.ledger.domain.CreateTransferResult.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * BDD-style behaviour spec for the deterministic ledger core. Each nested class
 * is a business use case from the platform flows; each test reads Given / When /
 * Then. Runs against {@link LedgerStateMachine} directly (pure, no I/O) so the
 * accounting logic is verified precisely and fast.
 *
 * <p>Chart of accounts used here (ledger VND = 704):
 * <ul>
 *   <li>{@code wallet*} — liability, flag DEBITS_MUST_NOT_EXCEED_CREDITS (chống âm ví)</li>
 *   <li>{@code merchant*} — liability, same flag</li>
 *   <li>{@code bank/clearing/revenue} — plain (clearing/asset/income, no balance-limit)</li>
 * </ul>
 */
@DisplayName("Ledger behaviour — business use cases (BDD)")
class LedgerBehaviourSpec {

    private AccountStore accounts;
    private TransferStore transfers;
    private PendingTransferIndex pending;
    private LedgerStateMachine sm;
    private long clock;

    @BeforeEach
    void setUp() {
        accounts = new AccountStore(100);
        transfers = new TransferStore(100);
        pending = new PendingTransferIndex();
        sm = new LedgerStateMachine(accounts, transfers, pending);
        clock = 1_000;
    }

    // ----- builders & helpers (keep the scenarios declarative) -----

    private static UInt128 id(long v) { return UInt128.of(v); }

    private static Account walletAcct(long id) {
        return new Account(id(id), 704, (short) 10, (short) AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS, 0, 0, 0);
    }
    private static Account plainAcct(long id) { return plainAcct(id, 704); }
    private static Account plainAcct(long id, int ledger) {
        return new Account(id(id), ledger, (short) 10, (short) 0, 0, 0, 0);
    }

    private static Transfer tf(long id, long d, long c, long amt, long pendingId, int timeout, int ledger, int flags) {
        return new Transfer(id(id), id(d), id(c), amt,
                pendingId == 0 ? UInt128.ZERO : id(pendingId), 0, 0, timeout, ledger, (short) 1, (short) flags, 0);
    }
    private static Transfer std(long id, long d, long c, long amt) { return tf(id, d, c, amt, 0, 0, 704, 0); }
    private static Transfer linked(long id, long d, long c, long amt) { return tf(id, d, c, amt, 0, 0, 704, TransferFlags.LINKED); }
    private static Transfer pend(long id, long d, long c, long amt, int timeout) { return tf(id, d, c, amt, 0, timeout, 704, TransferFlags.PENDING); }
    private static Transfer post(long id, long d, long c, long pendingId, long amt) { return tf(id, d, c, amt, pendingId, 0, 704, TransferFlags.POST_PENDING_TRANSFER); }
    private static Transfer voidp(long id, long d, long c, long pendingId) { return tf(id, d, c, 0, pendingId, 0, 704, TransferFlags.VOID_PENDING_TRANSFER); }

    private long reserve(int n) { long b = clock; clock += n; return b; }

    private void givenAccounts(Account... a) {
        assertThat(sm.createAccounts(List.of(a), reserve(a.length)))
                .allMatch(r -> r == CreateAccountResult.OK);
    }
    private List<CreateTransferResult> when(Transfer... ts) { return sm.createTransfers(List.of(ts), reserve(ts.length)); }
    private List<CreateTransferResult> whenAt(long base, Transfer... ts) { return sm.createTransfers(List.of(ts), base); }
    private AccountSnapshot acc(long id) { return sm.lookupAccount(id(id)); }

    // ======================================================================
    @Nested
    @DisplayName("Use case 1 — Nạp tiền vào ví (deposit)")
    class Deposit {
        @Test
        @DisplayName("tiền về làm tăng số dư ví, ghi nợ tài khoản đảm bảo")
        void depositCreditsWallet() {
            // Given: ví + tài khoản đảm bảo
            givenAccounts(walletAcct(1001), plainAcct(9001));
            // When: nạp 1.000.000 (Dr bank / Cr wallet)
            var r = when(std(5001, 9001, 1001, 1_000_000));
            // Then: ví có số dư khả dụng, bank ghi nợ
            assertThat(r).containsExactly(OK);
            assertThat(acc(1001).creditBalance()).isEqualTo(1_000_000);
            assertThat(acc(9001).debitBalance()).isEqualTo(1_000_000);
        }
    }

    // ======================================================================
    @Nested
    @DisplayName("Use case 2 — Mua hàng bằng ví, có phí (linked chain, atomic)")
    class InternalPaymentWithFee {
        @Test
        @DisplayName("đủ số dư: trừ ví + cộng merchant + thu phí, nguyên tử")
        void succeedsAtomically() {
            // Given: ví đã nạp 1.000.000
            givenAccounts(walletAcct(1001), walletAcct(2001), plainAcct(9002), plainAcct(9001));
            when(std(5001, 9001, 1001, 1_000_000));
            // When: mua 300.000, phí 5.000 trong một chuỗi linked
            var r = when(
                    linked(5002, 1001, 2001, 300_000),   // Dr wallet / Cr merchant [LINKED]
                    std(5003, 2001, 9002, 5_000));        // Dr merchant / Cr revenue:fee
            // Then
            assertThat(r).containsExactly(OK, OK);
            assertThat(acc(1001).creditBalance()).isEqualTo(700_000);
            assertThat(acc(2001).creditBalance()).isEqualTo(295_000);
            assertThat(acc(9002).creditBalance()).isEqualTo(5_000);
        }

        @Test
        @DisplayName("thiếu số dư: cả chuỗi rollback, không gì thay đổi")
        void rollsBackWhenInsufficient() {
            // Given: ví chỉ có 100.000
            givenAccounts(walletAcct(1001), walletAcct(2001), plainAcct(9002), plainAcct(9001));
            when(std(5001, 9001, 1001, 100_000));
            // When: cố mua 300.000 + phí 5.000
            var r = when(
                    linked(5002, 1001, 2001, 300_000),
                    std(5003, 2001, 9002, 5_000));
            // Then: leg gây lỗi báo đúng mã, các leg khác LINKED_EVENT_FAILED, state nguyên vẹn
            assertThat(r).containsExactly(EXCEEDS_DEBITS_LIMIT, LINKED_EVENT_FAILED);
            assertThat(acc(1001).creditBalance()).isEqualTo(100_000);
            assertThat(acc(2001).creditBalance()).isZero();
            assertThat(acc(9002).creditBalance()).isZero();
            assertThat(transfers.exists(id(5002))).isFalse();
            assertThat(transfers.exists(id(5003))).isFalse();
        }
    }

    // ======================================================================
    @Nested
    @DisplayName("Use case 3 — Chống âm ví (overdraft protection)")
    class Overdraft {
        @Test
        @DisplayName("ví chưa có tiền không thể chi")
        void cannotSpendWithoutBalance() {
            // Given: ví số dư 0 (có cờ chống âm) + tài khoản nhận
            givenAccounts(walletAcct(1001), plainAcct(2001));
            // When: chi 50.000
            var r = when(std(5001, 1001, 2001, 50_000));
            // Then: bị từ chối, số dư giữ nguyên 0
            assertThat(r).containsExactly(EXCEEDS_DEBITS_LIMIT);
            assertThat(acc(1001).creditBalance()).isZero();
            assertThat(acc(2001).creditBalance()).isZero();
        }
    }

    // ======================================================================
    @Nested
    @DisplayName("Use case 4 — Hai pha: giữ tiền → chốt / huỷ / hết hạn")
    class TwoPhase {
        @Test
        @DisplayName("authorize giữ tiền (debitsPending), chưa posted")
        void pendingHoldsFunds() {
            givenAccounts(plainAcct(1001), plainAcct(2001));
            // When: PENDING giữ 1.000
            var r = when(pend(6001, 1001, 2001, 1_000, 0));
            // Then
            assertThat(r).containsExactly(OK);
            assertThat(acc(1001).debitsPending()).isEqualTo(1_000);
            assertThat(acc(1001).debitsPosted()).isZero();
            assertThat(acc(2001).creditsPending()).isEqualTo(1_000);
        }

        @Test
        @DisplayName("capture một phần: post 800, nhả 200")
        void partialCaptureReleasesRemainder() {
            givenAccounts(plainAcct(1001), plainAcct(2001));
            when(pend(6001, 1001, 2001, 1_000, 0));
            // When: POST 800
            var r = when(post(6002, 1001, 2001, 6001, 800));
            // Then: pending nhả hết, posted = 800
            assertThat(r).containsExactly(OK);
            assertThat(acc(1001).debitsPending()).isZero();
            assertThat(acc(1001).debitsPosted()).isEqualTo(800);
            assertThat(acc(2001).creditsPosted()).isEqualTo(800);
        }

        @Test
        @DisplayName("void: nhả toàn bộ khoản giữ, không post gì")
        void voidReleasesHold() {
            givenAccounts(plainAcct(1001), plainAcct(2001));
            when(pend(6001, 1001, 2001, 1_000, 0));
            // When: VOID
            var r = when(voidp(6002, 1001, 2001, 6001));
            // Then
            assertThat(r).containsExactly(OK);
            assertThat(acc(1001).debitsPending()).isZero();
            assertThat(acc(1001).debitsPosted()).isZero();
        }

        @Test
        @DisplayName("không thể post quá số đã giữ")
        void cannotPostMoreThanPending() {
            givenAccounts(plainAcct(1001), plainAcct(2001));
            when(pend(6001, 1001, 2001, 1_000, 0));
            // When: post 1.500
            var r = when(post(6002, 1001, 2001, 6001, 1_500));
            // Then
            assertThat(r).containsExactly(EXCEEDS_PENDING_TRANSFER_AMOUNT);
        }

        @Test
        @DisplayName("không thể chốt hai lần (đã posted thì không còn OPEN)")
        void cannotPostTwice() {
            givenAccounts(plainAcct(1001), plainAcct(2001));
            when(pend(6001, 1001, 2001, 1_000, 0));
            when(post(6002, 1001, 2001, 6001, 1_000));
            // When: post lại
            var r = when(post(6003, 1001, 2001, 6001, 1_000));
            // Then
            assertThat(r).containsExactly(PENDING_TRANSFER_NOT_PENDING);
        }

        @Test
        @DisplayName("hết hạn: sweep tự void và nhả tiền, đúng mốc thời gian")
        void expirySweepReleasesAfterTimeout() {
            givenAccounts(plainAcct(1001), plainAcct(2001));
            // Given: PENDING tại ts=5000, timeout 1s → expiry = 5000 + 1e9
            whenAt(5_000, pend(6001, 1001, 2001, 500, 1));
            assertThat(acc(1001).debitsPending()).isEqualTo(500);
            // When: sweep trước hạn → không có gì
            assertThat(sm.expirePending(5_000 + 500_000_000L)).isZero();
            assertThat(acc(1001).debitsPending()).isEqualTo(500);
            // When: sweep tại/đến hạn → void, nhả tiền
            assertThat(sm.expirePending(5_000 + 1_000_000_000L)).isEqualTo(1);
            assertThat(acc(1001).debitsPending()).isZero();
            assertThat(acc(2001).creditsPending()).isZero();
        }

        @Test
        @DisplayName("không thể post một pending đã hết hạn")
        void cannotPostExpiredPending() {
            givenAccounts(plainAcct(1001), plainAcct(2001));
            whenAt(5_000, pend(6001, 1001, 2001, 500, 1));
            // When: post tại thời điểm sau hạn
            var r = whenAt(5_000 + 2_000_000_000L, post(6002, 1001, 2001, 6001, 500));
            // Then
            assertThat(r).containsExactly(PENDING_TRANSFER_EXPIRED);
        }
    }

    // ======================================================================
    @Nested
    @DisplayName("Use case 5 — Hoàn tiền (refund = bút toán đảo)")
    class Refund {
        @Test
        @DisplayName("hoàn tiền đưa số dư về như trước giao dịch")
        void refundReversesPostedTransfer() {
            // Given: ví nạp 1.000, mua 300 từ merchant
            givenAccounts(walletAcct(1001), walletAcct(2001), plainAcct(9001));
            when(std(5001, 9001, 1001, 1_000));
            when(std(5002, 1001, 2001, 300));
            assertThat(acc(1001).creditBalance()).isEqualTo(700);
            // When: hoàn 300 (Dr merchant / Cr wallet), id mới
            var r = when(std(5003, 2001, 1001, 300));
            // Then: ví về 1.000, merchant về 0
            assertThat(r).containsExactly(OK);
            assertThat(acc(1001).creditBalance()).isEqualTo(1_000);
            assertThat(acc(2001).creditBalance()).isZero();
        }
    }

    // ======================================================================
    @Nested
    @DisplayName("Use case 6 — Chuyển tiền P2P (ví → ví)")
    class P2P {
        @Test
        @DisplayName("chuyển hợp lệ giữa hai ví")
        void transferBetweenWallets() {
            givenAccounts(walletAcct(1001), walletAcct(1002), plainAcct(9001));
            when(std(5001, 9001, 1001, 500)); // nạp A
            // When: A chuyển 200 cho B
            var r = when(std(5002, 1001, 1002, 200));
            // Then
            assertThat(r).containsExactly(OK);
            assertThat(acc(1001).creditBalance()).isEqualTo(300);
            assertThat(acc(1002).creditBalance()).isEqualTo(200);
        }
    }

    // ======================================================================
    @Nested
    @DisplayName("Use case 7 — Idempotency (retry an toàn)")
    class Idempotency {
        @Test
        @DisplayName("gửi lại cùng id + cùng field → EXISTS, không áp dụng hai lần")
        void duplicateReturnsExists() {
            givenAccounts(plainAcct(1001), plainAcct(1002));
            when(std(5001, 1001, 1002, 100));
            // When: gửi lại y hệt
            var r = when(std(5001, 1001, 1002, 100));
            // Then
            assertThat(r).containsExactly(EXISTS);
            assertThat(acc(1001).debitsPosted()).isEqualTo(100); // không thành 200
        }

        @Test
        @DisplayName("cùng id nhưng field khác → EXISTS_WITH_DIFFERENT_FIELDS (từ chối)")
        void sameIdDifferentFieldsRejected() {
            givenAccounts(plainAcct(1001), plainAcct(1002));
            when(std(5001, 1001, 1002, 100));
            // When: cùng id, amount khác
            var r = when(std(5001, 1001, 1002, 999));
            // Then
            assertThat(r).containsExactly(EXISTS_WITH_DIFFERENT_FIELDS);
            assertThat(acc(1001).debitsPosted()).isEqualTo(100);
        }
    }

    // ======================================================================
    @Nested
    @DisplayName("Use case 8 — Đổi tiền / FX (linked 2-leg, hai ledger)")
    class Fx {
        @Test
        @DisplayName("chuỗi spanning 2 ledger vẫn nguyên tử")
        void crossLedgerSwapIsAtomic() {
            // Given: ví VND + clearing VND (704), clearing USD + ví USD (840)
            givenAccounts(plainAcct(1001, 704), plainAcct(9003, 704),
                          plainAcct(9004, 840), plainAcct(2002, 840));
            // When: đổi 100 VND lấy 5 USD (mỗi leg cùng-ledger)
            var r = when(
                    tf(7001, 1001, 9003, 100, 0, 0, 704, TransferFlags.LINKED),
                    tf(7002, 9004, 2002, 5, 0, 0, 840, 0));
            // Then
            assertThat(r).containsExactly(OK, OK);
            assertThat(acc(1001).debitsPosted()).isEqualTo(100);   // ví VND giảm
            assertThat(acc(2002).creditsPosted()).isEqualTo(5);    // ví USD tăng
        }
    }

    // ======================================================================
    @Nested
    @DisplayName("Use case 9 — Bảo toàn tiền (trial balance balanced)")
    class Conservation {
        @Test
        @DisplayName("tổng nợ = tổng có theo từng ledger")
        void trialBalanceIsBalanced() {
            givenAccounts(walletAcct(1001), walletAcct(2001), plainAcct(9001), plainAcct(9002));
            when(std(5001, 9001, 1001, 1_000));
            when(linked(5002, 1001, 2001, 300), std(5003, 2001, 9002, 5));
            // When
            List<LedgerBalance> tb = sm.trialBalance();
            // Then: ledger 704 cân
            LedgerBalance l704 = tb.stream().filter(b -> b.ledger() == 704).findFirst().orElseThrow();
            assertThat(l704.balanced()).isTrue();
            assertThat(l704.debitsPosted()).isEqualTo(l704.creditsPosted());
        }
    }

    // ======================================================================
    @Nested
    @DisplayName("Use case 10 — Validation guards (đầu vào sai bị chặn)")
    class Validation {
        @Test
        @DisplayName("amount = 0 bị từ chối")
        void zeroAmountRejected() {
            givenAccounts(plainAcct(1001), plainAcct(1002));
            assertThat(when(std(5001, 1001, 1002, 0))).containsExactly(AMOUNT_MUST_NOT_BE_ZERO);
        }

        @Test
        @DisplayName("nợ và có cùng một tài khoản bị từ chối")
        void sameDebitCreditRejected() {
            givenAccounts(plainAcct(1001));
            assertThat(when(std(5001, 1001, 1001, 10))).containsExactly(ACCOUNTS_MUST_BE_DIFFERENT);
        }

        @Test
        @DisplayName("tài khoản nợ không tồn tại bị từ chối")
        void missingDebitAccountRejected() {
            givenAccounts(plainAcct(1002));
            assertThat(when(std(5001, 1001, 1002, 10))).containsExactly(DEBIT_ACCOUNT_NOT_FOUND);
        }

        @Test
        @DisplayName("hai tài khoản khác ledger bị từ chối")
        void differentLedgerRejected() {
            givenAccounts(plainAcct(1001, 704), plainAcct(1002, 840));
            assertThat(when(tf(5001, 1001, 1002, 10, 0, 0, 704, 0)))
                    .containsExactly(ACCOUNTS_MUST_HAVE_SAME_LEDGER);
        }

        @Test
        @DisplayName("transfer id = 0 bị từ chối")
        void zeroIdRejected() {
            givenAccounts(plainAcct(1001), plainAcct(1002));
            assertThat(when(std(0, 1001, 1002, 10))).containsExactly(ID_MUST_NOT_BE_ZERO);
        }
    }
}
