package com.maxmind.db;

import java.util.Arrays;

/**
 * This is just for testing. It returns the position as the byte value.
 */
public class PositionByteReader extends PositionedByteReader {
    public PositionByteReader(long position, long capacity) {
        super(position, capacity);
    }

    public PositionByteReader(long capacity) {
        super(capacity);
    }

    @Override
    protected byte getUnsafe(long index) {
        return (byte) index;
    }

    @Override
    protected byte[] getBytesUnsafe(final long position, final int length) {
        final byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) position);
        return bytes;
    }

    @Override
    public ByteReader duplicate() {
        return new PositionByteReader(position, capacity);
    }
}
