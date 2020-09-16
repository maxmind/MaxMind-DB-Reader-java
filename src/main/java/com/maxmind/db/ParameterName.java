package com.maxmind.db;

import java.nio.ByteBuffer;

public final class ParameterName {
    private final ByteBuffer buffer;
    private final ByteBuffer parent;
    private final int offset;
    private final int hashCode;

    ParameterName(ByteBuffer buffer) {
        this(buffer, null, 0);
    }

    ParameterName(ByteBuffer buffer, ByteBuffer parent, int offset) {
        this.buffer = buffer;
        this.hashCode = buffer.hashCode();
        this.parent = parent;
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        ParameterName other = (ParameterName) o;

        // Fast path when both are from the database.
        if ( parent != null && parent == other.parent &&
            offset == other.offset ) {
            return true;
        }

        return this.buffer.equals(other.buffer);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
