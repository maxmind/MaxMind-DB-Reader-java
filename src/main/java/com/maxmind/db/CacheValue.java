package com.maxmind.db;

public final class CacheValue<T> {
    final int finalOffset;
    final Object value;
    final Type type;

    CacheValue(Type type, Object value, int finalOffset) {
        this.finalOffset = finalOffset;
        this.type =type;
        this.value = value;
    }
}
