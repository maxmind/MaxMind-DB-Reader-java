package com.maxmind.db;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * This is a combination of {@link PositionedByteReader} and {@link IntLimitedByteReader}.
 */
public abstract class PositionedIntLimitedByteReader extends IntLimitedByteReader {
    protected int position;
    protected final int capacity;

    protected PositionedIntLimitedByteReader(
        final int position,
        final int capacity
    ) {
        if (capacity < 0) {
            throw PositionedByteReader.createCapacityException(capacity);
        }
        this.position = position;
        this.capacity = capacity;
    }

    protected PositionedIntLimitedByteReader(final int capacity) {
        this(0, capacity);
    }

    // copied from Buffer.java
    /**
     * {@link ByteBuffer#nextGetIndex(int)}
     */
    protected int nextGetIndex(long nb) {                    // package-private
        int p = position;
        if (capacity - p < nb) {
            throw new BufferUnderflowException();
        }
        position = p + (int) nb;
        return p;
    }

    // copied from Buffer.java
    /**
     * {@link java.nio.Buffer#checkIndex(int)}
     */
    protected int checkIndex(long i) {                       // package-private
        if ((i < 0) || (i >= capacity)) {
            throw new IndexOutOfBoundsException();
        }
        return (int) i;
    }

    /**
     * See {@link ByteBuffer#position(int)}
     */
    @Override
    protected ByteReader position(final int newPosition) {
        final boolean isLarge = newPosition >= capacity;
        if (isLarge || newPosition < 0) {
            throw PositionedByteReader.createPositionException(capacity, newPosition, isLarge);
        }
        this.position = newPosition;
        return this;
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public long capacity() {
        return capacity;
    }

    /**
     * Get the byte with the assumption that the index is valid.
     */
    protected abstract byte getUnsafe(int index);

    /**
     * Get the byte at {@link ByteReader#position()}. See {@link ByteBuffer#get()}.
     */
    public byte get() {
        return getUnsafe(nextGetIndex(1));
    }

    /**
     * Get the byte at the given index. See {@link ByteBuffer#get(int)}
     */
    public byte get(long index) {
        return getUnsafe(checkIndex(index));
    }

    /**
     * Get the bytes with the assumption that the bounds checks already passed.
     */
    protected abstract byte[] getBytesUnsafe(
        final int position,
        final int length
    );

    /**
     * Read the requested number of bytes starting at {@link ByteReader#position()}.
     * It moves {@link ByteReader#position()} forward by the same amount.
     */
    public byte[] getBytes(final int length) {
        return getBytesUnsafe(nextGetIndex(length), length);
    }
}
