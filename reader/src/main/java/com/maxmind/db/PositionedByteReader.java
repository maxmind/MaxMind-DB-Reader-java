package com.maxmind.db;

import java.nio.BufferUnderflowException;

/**
 * This is a {@link ByteReader} that tracks its position. For instance,
 * {@link java.nio.ByteBuffer}s track their position, so when using that, this class isn't
 * necessary. If implemented with something like an array, which doesn't have its own pointer,
 * this class is helpful.
 */
public abstract class PositionedByteReader implements ByteReader {
    // Copied from java.nio.Buffer
    /**
     * {@link java.nio.ByteBuffer#createCapacityException(int)}
     */
    static IllegalArgumentException createCapacityException(long capacity) {
        assert capacity < 0 : "capacity expected to be negative";
        return new IllegalArgumentException("capacity < 0: ("
            + capacity + " < 0)");
    }

    // copied from Buffer.java
    /**
     * {@link java.nio.Buffer#createPositionException(int)}
     */
    static IllegalArgumentException createPositionException(
        final long capacity,
        final long newPosition,
        final boolean isLarge
    ) {
        String msg = null;

        if (isLarge) {
            msg = "newPosition > limit: (" + newPosition + " > " + capacity + ")";
        } else { // assume negative
            assert newPosition < 0 : "newPosition expected to be negative";
            msg = "newPosition < 0: (" + newPosition + " < 0)";
        }

        return new IllegalArgumentException(msg);
    }

    protected long position;
    protected final long capacity;

    protected PositionedByteReader(
        final long position,
        final long capacity
    ) {
        if (capacity < 0) {
            throw createCapacityException(capacity);
        }
        this.position = position;
        this.capacity = capacity;
    }

    protected PositionedByteReader(final long capacity) {
        this(0, capacity);
    }

    // copied from Buffer.java
    /**
     * {@link java.nio.ByteBuffer#nextGetIndex(int)}
     */
    protected long nextGetIndex(long nb) {                    // package-private
        long p = position();
        if (capacity() - p < nb) {
            throw new BufferUnderflowException();
        }
        position = p + nb;
        return p;
    }

    // copied from Buffer.java
    /**
     * {@link java.nio.Buffer#checkIndex(int)}
     */
    protected long checkIndex(long i) {                       // package-private
        if ((i < 0) || (i >= capacity())) {
            throw new IndexOutOfBoundsException();
        }
        return i;
    }

    /**
     * See {@link java.nio.ByteBuffer#position(int)}
     */
    @Override
    public ByteReader position(final long newPosition) {
        final boolean isLarge = newPosition >= capacity();
        if (isLarge || newPosition < 0) {
            throw createPositionException(capacity, newPosition, isLarge);
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
    protected abstract byte getUnsafe(long index);

    /**
     * Get the byte at {@link ByteReader#position()}. See {@link java.nio.ByteBuffer#get()}
     */
    @Override
    public byte get() {
        return getUnsafe(nextGetIndex(1));
    }

    /**
     * Get the byte at the given index. See {@link java.nio.ByteBuffer#get(int)}
     */
    @Override
    public byte get(long index) {
        return getUnsafe(checkIndex(index));
    }

    /**
     * Get the bytes with the assumption that the bounds checks already passed.
     */
    protected abstract byte[] getBytesUnsafe(
        final long position,
        final int length
    );

    /**
     * Read the requested number of bytes starting at {@link ByteReader#position()}.
     * It moves {@link ByteReader#position()} forward by the same amount.
     */
    @Override
    public byte[] getBytes(final int length) {
        return getBytesUnsafe(nextGetIndex(length), length);
    }
}
