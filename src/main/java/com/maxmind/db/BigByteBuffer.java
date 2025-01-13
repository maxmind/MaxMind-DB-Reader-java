package com.maxmind.db;

import it.unimi.dsi.fastutil.bytes.ByteBigList;
import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import it.unimi.dsi.fastutil.longs.Long2LongFunction;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * This provides minimal functionality from {@link ByteBuffer} that is required for this library,
 * but it is modified to work with files larger than Java's normal memory mapped file size limit.
 */
final class BigByteBuffer {

    public static final boolean NATIVE_IS_BIG_ENDIAN =
        ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    public static BigByteBuffer wrap(ByteBigList bytes) {
        return new BigByteBuffer(bytes, 0, bytes.size64(), bytes.size64());
    }

    // copied from Unsafe.java
    private static long toUnsignedLong(byte n) {
        return n & 0xffL;
    }

    // copied from Unsafe.java
    private static int toUnsignedInt(byte n) {
        return n & 0xff; 
    }

    private static final BiInt2IntFunction PICK_POS =
        NATIVE_IS_BIG_ENDIAN
            ? (int top, int pos) -> top - pos
            : (int top, int pos) -> pos;

    private static final Int2IntFunction CONV_ENDIAN_INT =
        NATIVE_IS_BIG_ENDIAN
            ? Int2IntFunction.identity()
            : Integer::reverseBytes;

    private static final Long2LongFunction CONV_ENDIAN_LONG =
        NATIVE_IS_BIG_ENDIAN
            ? Long2LongFunction.identity()
            : Long::reverseBytes;

    final ByteBigList bytes;
    final long capacity;
    long position;
    long limit;

    private BigByteBuffer(
        ByteBigList bytes,
        long position,
        long limit,
        long capacity
    ) {
        this.bytes = bytes;
        this.capacity = capacity;
        this.limit = limit;
        this.position = position;
    }

    /**
     * {@link ByteBuffer#get()}
     */
    public byte get() {
        return bytes.getByte(nextGetIndex(1));
    }

    /**
     * {@link ByteBuffer#get(int)}
     */
    public byte get(long index) {
        return bytes.getByte(checkIndex(index));
    }

    /**
     * {@link ByteBuffer#get(byte[])}
     */
    public BigByteBuffer get(byte[] dst) {
        return get(dst, 0, dst.length);
    }

    /**
     * {@link ByteBuffer#get(byte[], int, int)} )}
     */
    public BigByteBuffer get(byte[] dst, int offset, int length) {
        Objects.checkFromIndexSize(offset, length, dst.length);
        long pos = position();
        if (length > limit() - pos) {
            throw new BufferUnderflowException();
        }

        this.bytes.getElements(pos, dst, offset, length);

        position(pos + length);
        return this;
    }

    public long getLong() {
        long index = nextGetIndex(8);
        byte i0 = get(index);
        byte i1 = get(++index);
        byte i2 = get(++index);
        byte i3 = get(++index);
        byte i4 = get(++index);
        byte i5 = get(++index);
        byte i6 = get(++index);
        byte i7 = get(++index);
        return CONV_ENDIAN_LONG.applyAsLong(((toUnsignedLong(i0) << PICK_POS.apply(56, 0))
            | (toUnsignedLong(i1) << PICK_POS.apply(56, 8))
            | (toUnsignedLong(i2) << PICK_POS.apply(56, 16))
            | (toUnsignedLong(i3) << PICK_POS.apply(56, 24))
            | (toUnsignedLong(i4) << PICK_POS.apply(56, 32))
            | (toUnsignedLong(i5) << PICK_POS.apply(56, 40))
            | (toUnsignedLong(i6) << PICK_POS.apply(56, 48))
            | (toUnsignedLong(i7) << PICK_POS.apply(56, 56))));
    }

    public int getInt() {
        long index = nextGetIndex(4);
        byte i0 = get(index);
        byte i1 = get(++index);
        byte i2 = get(++index);
        byte i3 = get(++index);
        return CONV_ENDIAN_INT.applyAsInt(((toUnsignedInt(i0) << PICK_POS.apply(24, 0))
            | (toUnsignedInt(i1) << PICK_POS.apply(24, 8))
            | (toUnsignedInt(i2) << PICK_POS.apply(24, 16))
            | (toUnsignedInt(i3) << PICK_POS.apply(24, 24))));
    }

    /**
     * {@link ByteBuffer#getDouble()}
     */
    public double getDouble() {
        return Double.longBitsToDouble(getLong());
    }

    /**
     * {@link ByteBuffer#getFloat()}
     */
    public float getFloat() {
        return Float.intBitsToFloat(getInt());
    }

    /**
     * {@link ByteBuffer#position()}
     */
    public long position() {
        return position;
    }

    // copied from Buffer.java
    /**
     * {@link ByteBuffer#position(int)}
     */
    public BigByteBuffer position(final long newPosition) {
        if (newPosition > limit | newPosition < 0) {
            throw createPositionException(newPosition);
        }
        position = newPosition;
        return this;
    }

    /**
     * {@link ByteBuffer#limit()}
     */
    public long limit() {
        return limit;
    }

    /**
     * {@link ByteBuffer#limit(int)}
     */
    public BigByteBuffer limit(final long newLimit) {
        if (newLimit > capacity | newLimit < 0) {
            throw createLimitException(newLimit);
        }
        limit = newLimit;
        if (position > newLimit) {
            position = newLimit;
        }
        return this;
    }

    /**
     * {@link ByteBuffer#capacity()}
     */
    long capacity() {
        return capacity;
    }

    // copied from Buffer.java
    /**
     * Verify that {@code 0 < newPosition <= limit}
     *
     * @param newPosition
     *        The new position value
     *
     * @throws IllegalArgumentException
     *         If the specified position is out of bounds.
     */
    private IllegalArgumentException createPositionException(long newPosition) {
        String msg = null;

        if (newPosition > limit) {
            msg = "newPosition > limit: (" + newPosition + " > " + limit + ")";
        } else { // assume negative
            assert newPosition < 0 : "newPosition expected to be negative";
            msg = "newPosition < 0: (" + newPosition + " < 0)";
        }

        return new IllegalArgumentException(msg);
    }

    // copied from Buffer.java
    /**
     * Verify that {@code 0 < newLimit <= capacity}
     *
     * @param newLimit
     *        The new limit value
     *
     * @throws IllegalArgumentException
     *         If the specified limit is out of bounds.
     */
    private IllegalArgumentException createLimitException(long newLimit) {
        String msg = null;

        if (newLimit > capacity) {
            msg = "newLimit > capacity: (" + newLimit + " > " + capacity + ")";
        } else { // assume negative
            assert newLimit < 0 : "newLimit expected to be negative";
            msg = "newLimit < 0: (" + newLimit + " < 0)";
        }

        return new IllegalArgumentException(msg);
    }

    // copied from Buffer.java
    /**
     * {@link ByteBuffer#nextGetIndex(int)}
     */
    long nextGetIndex(long nb) {                    // package-private
        long p = position;
        if (limit - p < nb) {
            throw new BufferUnderflowException();
        }
        position = p + nb;
        return p;
    }

    // copied from Buffer.java
    /**
     * Checks the given index against the limit, throwing an {@link
     * IndexOutOfBoundsException} if it is not smaller than the limit
     * or is smaller than zero.
     */
    long checkIndex(long i) {                       // package-private
        if ((i < 0) || (i >= limit)) {
            throw new IndexOutOfBoundsException();
        }
        return i;
    }

    /**
     * Get a {@link ByteBuffer} for the current buffer's position. {@link #position()}
     * is forwarded by the number of bytes.
     *
     * @param limit is the number of bytes from {@link #position()} to put into the
     *              {@link ByteBuffer}.
     * @throws BufferUnderflowException if there aren't enough bytes remaining
     */
    public ByteBuffer getByteBuffer(int limit) {
        /*
        A more optimal solution might be to create a ByteBuffer implementation that
        is backed by bytes.subList(position, limit).
         */
        final long position = nextGetIndex(limit);
        final byte[] bufferBytes = new byte[limit];
        bytes.getElements(position, bufferBytes, 0, limit);
        return ByteBuffer.wrap(bufferBytes);
    }

    /**
     * {@link ByteBuffer#duplicate()}
     */
    public BigByteBuffer duplicate() {
        return new BigByteBuffer(
            bytes,
            position(),
            limit(),
            capacity());
    }

}
