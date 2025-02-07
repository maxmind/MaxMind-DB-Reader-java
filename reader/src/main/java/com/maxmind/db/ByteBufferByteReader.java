package com.maxmind.db;

import java.nio.ByteBuffer;

/**
 * A {@link ByteReader} implemented by a {@link ByteBuffer}.
 */
public class ByteBufferByteReader extends IntLimitedByteReader {
    protected final ByteBuffer bytes;

    /**
     * Create a {@link ByteReader} from a {@link ByteBuffer}.
     */
    public ByteBufferByteReader(final ByteBuffer bytes) {
        this.bytes = bytes;
    }

    @Override
    public byte[] getBytes(final int length) {
        final byte[] dst = new byte[length];
        bytes.get(dst);
        return dst;
    }

    @Override
    public byte get() {
        return bytes.get();
    }

    @Override
    public byte get(long index) {
        return super.get(index);
    }

    @Override
    protected byte get(final int index) {
        return bytes.get(index);
    }

    @Override
    public long capacity() {
        return bytes.capacity();
    }

    @Override
    protected ByteReader position(final int newPosition) {
        bytes.position(newPosition);
        return this;
    }

    @Override
    public long position() {
        return bytes.position();
    }

    @Override
    public ByteReader duplicate() {
        return new ByteBufferByteReader(bytes.duplicate());
    }
}
