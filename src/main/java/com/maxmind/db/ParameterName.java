package com.maxmind.db;

import java.nio.ByteBuffer;

public final class ParameterName {
    private final ByteBuffer buffer;
    private final int hashCode;

    ParameterName(ByteBuffer buffer) {
        this.buffer = buffer;
        this.hashCode = buffer.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        ParameterName other = (ParameterName) o;

        return this.buffer.equals(other.buffer);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
