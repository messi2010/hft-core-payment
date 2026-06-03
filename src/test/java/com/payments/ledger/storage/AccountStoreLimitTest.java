package com.payments.ledger.storage;

import com.payments.ledger.domain.AccountFlags;
import com.payments.ledger.domain.UInt128;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AccountStoreLimitTest {

    @Test
    void wouldExceedDebitLimit_detectsOverflowInPendingPlusPostedSum() {
        AccountStore store = new AccountStore(1);
        UInt128 id = UInt128.of(1);
        store.insert(id, 1, (short) 1, (short) AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS, 0, 0, 0L);
        int slot = store.slotOf(id);

        // debitsPosted  = 2^63  (Long.MIN_VALUE in two's complement)
        // debitsPending = 2^63 + 1
        // Java sum: Long.MIN_VALUE + (Long.MIN_VALUE + 1) + 1 = 2  (wraps!)
        // True unsigned sum: 2^64 + 2, which far exceeds creditsPosted=100
        // Bug: compareUnsigned(2, 100) → false  — limit NOT detected  ← this is what we're fixing
        store.addDebitsPosted(slot, Long.MIN_VALUE);
        store.addDebitsPending(slot, Long.MIN_VALUE + 1L);
        store.addCreditsPosted(slot, 100L);

        assertThat(store.wouldExceedDebitLimit(slot, 1L))
                .as("overflow in posted+pending sum must be detected as limit exceeded")
                .isTrue();
    }

    @Test
    void wouldExceedCreditLimit_detectsOverflowInPendingPlusPostedSum() {
        AccountStore store = new AccountStore(1);
        UInt128 id = UInt128.of(2);
        store.insert(id, 1, (short) 1, (short) AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS, 0, 0, 0L);
        int slot = store.slotOf(id);

        store.addCreditsPosted(slot, Long.MIN_VALUE);
        store.addCreditsPending(slot, Long.MIN_VALUE + 1L);
        store.addDebitsPosted(slot, 100L);

        assertThat(store.wouldExceedCreditLimit(slot, 1L))
                .as("overflow in posted+pending sum must be detected as limit exceeded")
                .isTrue();
    }

    @Test
    void wouldExceedDebitLimit_allowsNormalTransferBelowLimit() {
        AccountStore store = new AccountStore(1);
        UInt128 id = UInt128.of(3);
        store.insert(id, 1, (short) 1, (short) AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS, 0, 0, 0L);
        int slot = store.slotOf(id);

        store.addCreditsPosted(slot, 1_000L);
        store.addDebitsPosted(slot, 400L);
        store.addDebitsPending(slot, 300L);

        // 400 + 300 + 200 = 900 ≤ 1000 → should NOT exceed
        assertThat(store.wouldExceedDebitLimit(slot, 200L)).isFalse();
        // 400 + 300 + 301 = 1001 > 1000 → should exceed
        assertThat(store.wouldExceedDebitLimit(slot, 301L)).isTrue();
    }
}
