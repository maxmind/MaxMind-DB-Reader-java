package com.maxmind.db;

/**
 * {@code DecodedValue} is a wrapper for the decoded value.
 */
public final class DecodedValue {
    private static final int PAYLOAD_SHIFT = 8;
    private static final int VALUES_SHIFT = 30;
    private static final long PAYLOAD_MASK = (1L << 22) - 1;

    Object value;

    DecodedValue(Object value) {
        this.value = value;
    }

    Object value() {
        if (value instanceof CostedValue costedValue) {
            return costedValue.value();
        }
        return value;
    }

    int values() {
        return (int) (costs() >>> VALUES_SHIFT);
    }

    long payloadBytes() {
        return (costs() >>> PAYLOAD_SHIFT) & PAYLOAD_MASK;
    }

    int depth() {
        return (int) (costs() & 0xFF);
    }

    DecodedValue costs(int values, long payloadBytes, int depth) {
        var costs = ((long) values << VALUES_SHIFT)
            | (payloadBytes << PAYLOAD_SHIFT)
            | depth;
        this.value = new CostedValue(this.value, costs);
        return this;
    }

    private long costs() {
        if (value instanceof CostedValue costedValue) {
            return costedValue.costs();
        }
        return 0;
    }

    private record CostedValue(Object value, long costs) {
    }
}
