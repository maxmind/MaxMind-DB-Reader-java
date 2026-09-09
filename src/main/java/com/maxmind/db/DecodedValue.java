package com.maxmind.db;

/**
 * An opaque decoded value and its resource costs, produced by {@link NodeCache.Loader}.
 * Caches retain this instance unchanged for its original key. See {@link NodeCache}.
 */
public final class DecodedValue {
    private static final int PAYLOAD_SHIFT = 8;
    private static final int VALUES_SHIFT = 30;
    private static final long PAYLOAD_MASK = (1L << 22) - 1;

    // Final fields preserve their initialized values when a cache publishes this object.
    final Object value;
    private final long costs;

    DecodedValue(Object value, int values, long payloadBytes, int depth) {
        this.value = value;
        this.costs = ((long) values << VALUES_SHIFT)
            | (payloadBytes << PAYLOAD_SHIFT)
            | depth;
    }

    Object value() {
        return value;
    }

    static int values(long costs) {
        return (int) (costs >>> VALUES_SHIFT);
    }

    static long payloadBytes(long costs) {
        return (costs >>> PAYLOAD_SHIFT) & PAYLOAD_MASK;
    }

    static int depth(long costs) {
        return (int) (costs & 0xFF);
    }

    long costs() {
        return this.costs;
    }
}
