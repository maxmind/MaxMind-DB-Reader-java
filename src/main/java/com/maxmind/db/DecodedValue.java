package com.maxmind.db;

/**
 * {@code DecodedValue} is a wrapper for the decoded value.
 */
public final class DecodedValue {
    private static final int PAYLOAD_SHIFT = 8;
    private static final int VALUES_SHIFT = 30;
    private static final long PAYLOAD_MASK = (1L << 22) - 1;

    final Object value;
    private long costs;

    DecodedValue(Object value) {
        this.value = value;
    }

    Object value() {
        return value;
    }

    int values() {
        return values(costs());
    }

    static int values(long costs) {
        return (int) (costs >>> VALUES_SHIFT);
    }

    long payloadBytes() {
        return payloadBytes(costs());
    }

    static long payloadBytes(long costs) {
        return (costs >>> PAYLOAD_SHIFT) & PAYLOAD_MASK;
    }

    int depth() {
        return depth(costs());
    }

    static int depth(long costs) {
        return (int) (costs & 0xFF);
    }

    DecodedValue costs(int values, long payloadBytes, int depth) {
        this.costs = ((long) values << VALUES_SHIFT)
            | (payloadBytes << PAYLOAD_SHIFT)
            | depth;
        return this;
    }

    long costs() {
        return this.costs;
    }
}
