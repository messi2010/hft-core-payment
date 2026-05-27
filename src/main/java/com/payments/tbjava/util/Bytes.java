package com.payments.tbjava.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Helpers for little-endian binary I/O. Java's ByteBuffer defaults to big-endian. */
public final class Bytes {
    private Bytes() {}

    public static ByteBuffer allocate(int size) {
        return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    }

    public static ByteBuffer allocateDirect(int size) {
        return ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN);
    }

    public static ByteBuffer wrap(byte[] data) {
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
    }
}
