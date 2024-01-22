package com.maxmind.db;

/**
 * {@code DecodedValue} is a wrapper for the decoded value and the number of bytes used
 * to decode it.
 */
public final class DecodedValue {
    final Object value;

    DecodedValue(Object value) {
        this.value = value;
    }

    Object getValue() {
        return value;
    }
}
