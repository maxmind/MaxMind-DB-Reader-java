package com.maxmind.db;

import java.nio.ByteBuffer;

/**
 * <p>This provides functionality from {@link ByteBuffer} that is required for this library.
 * The default implementation is {@link ByteBufferByteReader}, which just redirects method calls
 * to a {@link ByteBuffer}.Another implementation might be preferable. In that case, you can pass
 * it to {@link Reader#Reader(ByteReader, String, NodeCache)}.</p>
 *
 * <p>Methods in implementations should throw the same exceptions as the methods with the same name
 * in {@link ByteBuffer}.</p>
 *
 * <p>If your implementation can only handle positions of up to <code>Integer.MAX_VALUE</code>,
 * implement {@link IntLimitedByteReader}.</p>
 */
public interface ByteReader {

    /**
     * See {@link ByteBuffer#position()}
     */
    long position();

    /**
     * See {@link ByteBuffer#position(int)}
     */
    ByteReader position(final long newPosition);

    /**
     * See {@link ByteBuffer#capacity()}.
     */
    long capacity();

    /**
     * Get the byte at {@link ByteReader#position()}. See {@link ByteBuffer#get()}
     */
    byte get();

    /**
     * Get the byte at the given index. See {@link ByteBuffer#get(int)}
     */
    byte get(final long index);

    /**
     * Read the requested number of bytes starting at {@link ByteReader#position()}.
     * It moves {@link ByteReader#position()} forward by the same amount.
     */
    byte[] getBytes(final int length);

    /**
     * This is intended for creating a copy of this collection that is safe
     * for use by a new thread.</p>
     *
     * See {@link java.nio.ByteBuffer#duplicate()}
     */
    ByteReader duplicate();
}
