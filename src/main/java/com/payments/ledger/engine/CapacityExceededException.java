package com.payments.ledger.engine;

/**
 * Thrown when the Disruptor ring buffer is full — the single writer is behind
 * and there is no free slot to publish into.
 *
 * <p>This is the engine's backpressure signal. The publish path is non-blocking
 * ({@code tryNext()}), so instead of parking the calling thread (fatal on a Netty
 * event loop) it fails fast; the API layer maps this to HTTP 429 so the client
 * retries. Surfacing backpressure beats silently queueing unbounded work.
 */
public final class CapacityExceededException extends RuntimeException {
    public CapacityExceededException() {
        super("Ledger ring buffer is full; retry later");
    }
}
