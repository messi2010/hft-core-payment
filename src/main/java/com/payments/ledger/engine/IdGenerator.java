package com.payments.ledger.engine;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Monotonic clock used by the state machine to timestamp accounts and
 * transfers. Strictly increasing even if {@code System.nanoTime()} repeats --
 * guarantees a total order on all writes so consumers can detect gaps.
 *
 * <p>Only the writer thread calls {@link #next}. The atomic is overkill for
 * single-threaded use but cheap (no contention).
 */
public final class IdGenerator {

    private final AtomicLong lastTimestamp = new AtomicLong(0);

    public long next() {
        return reserve(1);
    }

    /**
     * Reserve a contiguous block of {@code n} strictly increasing timestamps and
     * return its base. The caller owns {@code [base, base + n)} and assigns
     * {@code base + i} to the i-th record in a command.
     *
     * <p>Reserving the whole command's range up front is what makes replay
     * deterministic: the journal records only {@code base}, and re-applying the
     * same command derives identical per-record timestamps -- no wall-clock call
     * happens during replay.
     */
    public long reserve(int n) {
        if (n < 1) throw new IllegalArgumentException("n must be >= 1");
        long now = System.nanoTime();
        while (true) {
            long prev = lastTimestamp.get();
            long base = Math.max(now, prev + 1);
            if (lastTimestamp.compareAndSet(prev, base + n - 1)) return base;
        }
    }

    /**
     * Advance the clock so the next reservation is strictly greater than
     * {@code ts}. Called during recovery after replaying journaled commands so
     * post-recovery writes never reuse a recovered timestamp.
     */
    public void ensureAfter(long ts) {
        while (true) {
            long prev = lastTimestamp.get();
            if (prev >= ts) return;
            if (lastTimestamp.compareAndSet(prev, ts)) return;
        }
    }
}
