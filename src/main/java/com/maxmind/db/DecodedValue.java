package com.maxmind.db;

public final class DecodedValue {
     final Object value;

    DecodedValue(Object value) {
        this.value = value;
    }

    Object getValue() {
        return value;
    }
}
