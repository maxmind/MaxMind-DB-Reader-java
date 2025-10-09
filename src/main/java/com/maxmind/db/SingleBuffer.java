package com.maxmind.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;

/**
 * A {@link Buffer} implementation backed by a single {@link ByteBuffer}.
 *
 * <p>This implementation is limited to capacities up to
 * {@link Integer#MAX_VALUE}, as {@link ByteBuffer} cannot exceed that size.
 */
class SingleBuffer implements Buffer {

    private final ByteBuffer buffer;

    /**
     * Creates a new {@code SingleBuffer} with the given capacity.
     *
     * @param capacity the capacity in bytes (must be <= Integer.MAX_VALUE)
     * @throws IllegalArgumentException if the capacity exceeds
     *                                  {@link Integer#MAX_VALUE}
     */
    public SingleBuffer(long capacity) {
        if (capacity > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "SingleBuffer cannot exceed Integer.MAX_VALUE capacity"
            );
        }
        this.buffer = ByteBuffer.allocate((int) capacity);
    }

    /**
     * Creates a new {@code SingleBuffer} wrapping the given {@link ByteBuffer}.
     *
     * @param buffer the underlying buffer
     */
    private SingleBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /** {@inheritDoc} */
    @Override
    public long capacity() {
        return buffer.capacity();
    }

    /** {@inheritDoc} */
    @Override
    public long position() {
        return buffer.position();
    }

    /** {@inheritDoc} */
    @Override
    public SingleBuffer position(long newPosition) {
        buffer.position((int) newPosition);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public long limit() {
        return buffer.limit();
    }

    /** {@inheritDoc} */
    @Override
    public SingleBuffer limit(long newLimit) {
        buffer.limit((int) newLimit);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public byte get() {
        return buffer.get();
    }

    /** {@inheritDoc} */
    @Override
    public SingleBuffer get(byte[] dst) {
        buffer.get(dst);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public byte get(long index) {
        return buffer.get((int) index);
    }

    /** {@inheritDoc} */
    @Override
    public double getDouble() {
        return buffer.getDouble();
    }

    /** {@inheritDoc} */
    @Override
    public float getFloat() {
        return buffer.getFloat();
    }

    /** {@inheritDoc} */
    @Override
    public SingleBuffer duplicate() {
        return new SingleBuffer(this.buffer.duplicate());
    }

    /** {@inheritDoc} */
    @Override
    public long readFrom(FileChannel channel) throws IOException {
        return channel.read(buffer);
    }

    /** {@inheritDoc} */
    @Override
    public String decode(CharsetDecoder decoder)
        throws CharacterCodingException {
        return decoder.decode(buffer).toString();
    }

    /**
     * Wraps the given byte array in a new {@code SingleBuffer}.
     *
     * @param array the byte array to wrap
     * @return a new {@code SingleBuffer} backed by the array
     */
    public static SingleBuffer wrap(byte[] array) {
        return new SingleBuffer(ByteBuffer.wrap(array));
    }

    /**
     * Creates a read-only {@code SingleBuffer} by memory-mapping the given
     * {@link FileChannel}.
     *
     * @param channel the file channel to map
     * @return a new read-only {@code SingleBuffer}
     * @throws IOException if an I/O error occurs
     */
    public static SingleBuffer mapFromChannel(FileChannel channel)
        throws IOException {
        ByteBuffer buffer = channel.map(MapMode.READ_ONLY, 0, channel.size());
        return new SingleBuffer(buffer.asReadOnlyBuffer());
    }
}
