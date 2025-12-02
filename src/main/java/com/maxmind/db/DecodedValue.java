package com.maxmind.db;

/**
 * {@code DecodedValue} is a wrapper for the decoded value.
 */
public final class DecodedValue {
    final Object value;

    DecodedValue(Object value) {
        this.value = value;
    }

    Object value() {
        return value;
    }
}
