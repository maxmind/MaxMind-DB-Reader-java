package com.maxmind.db;

/**
 * {@code DecodedValue} is a wrapper for the decoded value.
 */
public final class DecodedValue {
    private static final int PAYLOAD_SHIFT = 8;
    private static final int VALUES_SHIFT = 30;
    private static final long PAYLOAD_MASK = (1L << 22) - 1;

    final Object value;
    // A NodeCache is user-supplied and may publish this instance to another
    // thread without a happens-before edge. Set the costs in the constructor
    // so a reader cannot see a zero budget charge for a non-empty value.
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

    long costs() {
        return this.costs;
    }
}
