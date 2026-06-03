package com.payments.ledger.cluster;

/**
 * Pure-counter monotonic timestamp source for the cluster state machine.
 *
 * <p>Under Aeron Cluster every node calls {@code onSessionMessage} in the same
 * committed order, so a strictly-incrementing counter advances identically on
 * leader, followers, and any later restart that replays the log. <b>No wall
 * clock is read</b> — replaying the log reproduces the same per-record ts.
 *
 * <p>The counter is owned by the writer (Aeron cluster service agent), so no
 * synchronization is needed. It is snapshot-resident: {@link #position()}
 * is written by {@code onTakeSnapshot} and restored by {@code onStart}.
 */
public final class ClusterIdGenerator {

    private long last;

    public ClusterIdGenerator() { this(0L); }
    public ClusterIdGenerator(long initial) { this.last = initial; }

    /** Reserve {@code n} contiguous timestamps, return the base. Records get {@code base..base+n-1}. */
    public long reserve(int n) {
        if (n < 1) throw new IllegalArgumentException("n must be >= 1");
        long base = last + 1;
        last = base + n - 1;
        return base;
    }

    /** Greatest timestamp issued so far (0 if nothing issued). */
    public long position() { return last; }

    /** Restore from snapshot. New {@link #reserve(int)} calls will be > {@code pos}. */
    public void restore(long pos) { this.last = pos; }

    /** Ensure subsequent reservations are > {@code pos}; called on role change to be safe. */
    public void ensureAfter(long pos) { if (pos > last) last = pos; }
}
