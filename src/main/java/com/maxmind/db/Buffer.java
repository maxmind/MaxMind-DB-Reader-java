package com.maxmind.db;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;

/**
 * A generic buffer abstraction that supports sequential and random access
 * to binary data. Implementations may be backed by a single {@link
 * java.nio.ByteBuffer} or multiple buffers for larger capacities.
 *
 * <p>This interface is designed to provide a long-based API while
 * remaining compatible with the limitations of underlying storage.
 */
interface Buffer {
    /**
     * Returns the total capacity of this buffer in bytes.
     *
     * @return the capacity
     */
    long capacity();

    /**
     * Returns the current position of this buffer.
     *
     * @return the position
     */
    long position();

    /**
     * Sets the buffer's position.
     *
     * @param newPosition the new position
     * @return this buffer
     */
    Buffer position(long newPosition);

    /**
     * Returns the current limit of this buffer.
     *
     * @return the limit
     */
    long limit();

    /**
     * Sets the buffer's limit.
     *
     * @param newLimit the new limit
     * @return this buffer
     */
    Buffer limit(long newLimit);

    /**
     * Reads the next byte at the current position and advances the position.
     *
     * @return the byte value
     */
    byte get();

    /**
     * Reads bytes into the given array and advances the position.
     *
     * @param dst the destination array
     * @return this buffer
     */
    Buffer get(byte[] dst);

    /**
     * Reads a byte at the given absolute index without changing the position.
     *
     * @param index the index to read from
     * @return the byte value
     */
    byte get(long index);

    /**
     * Reads the next 8 bytes as a double and advances the position.
     *
     * @return the double value
     */
    double getDouble();

    /**
     * Reads the next 4 bytes as a float and advances the position.
     *
     * @return the float value
     */
    float getFloat();

    /**
     * Creates a new buffer that shares the same content but has independent
     * position, limit, and mark values.
     *
     * @return a duplicate buffer
     */
    Buffer duplicate();

    /**
     * Reads data from the given channel into this buffer starting at the
     * current position.
     *
     * @param channel the file channel
     * @return the number of bytes read
     * @throws IOException if an I/O error occurs
     */
    long readFrom(FileChannel channel) throws IOException;

    /**
     * Decodes the buffer's content into a string using the given decoder.
     *
     * @param decoder the charset decoder
     * @return the decoded string
     * @throws CharacterCodingException if decoding fails
     */
    String decode(CharsetDecoder decoder) throws CharacterCodingException;

    /**
     * Creates a read-only view of this buffer.
     *
     * @return a read-only buffer
     */
    Buffer asReadOnlyBuffer();
}
