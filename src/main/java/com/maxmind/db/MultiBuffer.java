package com.maxmind.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link Buffer} implementation backed by multiple {@link ByteBuffer}s,
 * allowing support for capacities larger than {@link Integer#MAX_VALUE}.
 *
 * <p>This implementation virtually concatenates several
 * {@link ByteBuffer}s (each up to {@link Integer#MAX_VALUE}) and maintains
 * a single logical position and limit across them.
 *
 * <p>Use this when working with databases/files that may exceed 2GB.
 */
class MultiBuffer implements Buffer {

    /** Maximum size per underlying chunk. */
    static final int CHUNK_SIZE = Integer.MAX_VALUE;

    final List<ByteBuffer> buffers = new ArrayList<>();
    private final long capacity;

    private long position = 0;
    private long limit;

    /**
     * Creates a new {@code MultiBuffer} with the given capacity, backed by
     * heap-allocated {@link ByteBuffer}s.
     *
     * @param capacity the total capacity in bytes
     */
    public MultiBuffer(long capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        this.capacity = capacity;
        this.limit = capacity;

        int fullChunks = (int) (capacity / CHUNK_SIZE);
        int remainder = (int) (capacity % CHUNK_SIZE);
        for (int i = 0; i < fullChunks; i++) {
            buffers.add(ByteBuffer.allocateDirect(CHUNK_SIZE));
        }
        if (remainder > 0) {
            buffers.add(ByteBuffer.allocateDirect(remainder));
        }
    }

    private MultiBuffer(List<ByteBuffer> buffers) {
        this.buffers.addAll(buffers);
        long capacity = buffers.stream().mapToLong(ByteBuffer::capacity).sum();
        this.capacity = capacity;
        this.limit = capacity;
    }

    /** {@inheritDoc} */
    @Override
    public long capacity() {
        return capacity;
    }

    /** {@inheritDoc} */
    @Override
    public long position() {
        return position;
    }

    /** {@inheritDoc} */
    @Override
    public Buffer position(long newPosition) {
        if (newPosition < 0 || newPosition > limit) {
            throw new IllegalArgumentException("Invalid position: " + newPosition);
        }
        this.position = newPosition;
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public long limit() {
        return limit;
    }

    /** {@inheritDoc} */
    @Override
    public Buffer limit(long newLimit) {
        if (newLimit < 0 || newLimit > capacity) {
            throw new IllegalArgumentException("Invalid limit: " + newLimit);
        }
        this.limit = newLimit;
        if (position > limit) {
            position = limit;
        }
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public byte get() {
        byte value = get(position);
        position++;
        return value;
    }

    /** {@inheritDoc} */
    @Override
    public Buffer get(byte[] dst) {
        long pos = position;
        int offset = 0;
        int length = dst.length;
        while (length > 0) {
            int bufIndex = (int) (pos / CHUNK_SIZE);
            int bufOffset = (int) (pos % CHUNK_SIZE);
            ByteBuffer buf = buffers.get(bufIndex).duplicate();
            buf.position(bufOffset);
            int toRead = Math.min(buf.remaining(), length);
            buf.get(dst, offset, toRead);
            pos += toRead;
            offset += toRead;
            length -= toRead;
        }
        position = pos;
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public byte get(long index) {
        if (index < 0 || index >= limit) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }
        int bufIndex = (int) (index / CHUNK_SIZE);
        int offset = (int) (index % CHUNK_SIZE);
        return buffers.get(bufIndex).get(offset);
    }

    /** {@inheritDoc} */
    @Override
    public double getDouble() {
        int bufIndex = (int) (position / CHUNK_SIZE);
        int off = (int) (position % CHUNK_SIZE);
        ByteBuffer buf = buffers.get(bufIndex).duplicate();
        buf.position(off);
        double value;
        if (off + 8 <= buf.remaining()) {
            value = buf.getDouble();
            position += 8;
            return value;
        } else {
            byte[] eight = new byte[8];
            get(eight);
            return ByteBuffer.wrap(eight).getDouble();
        }
    }

    /** {@inheritDoc} */
    @Override
    public float getFloat() {
        int bufIndex = (int) (position / CHUNK_SIZE);
        int off = (int) (position % CHUNK_SIZE);
        ByteBuffer buf = buffers.get(bufIndex).duplicate();
        buf.position(off);
        float value;
        if (off + 4 <= buf.remaining()) {
            value = buf.getFloat();
            position += 4;
            return value;
        } else {
            byte[] four = new byte[4];
            get(four);
            return ByteBuffer.wrap(four).getFloat();
        }
    }

    /** {@inheritDoc} */
    @Override
    public Buffer duplicate() {
        MultiBuffer copy = new MultiBuffer(capacity);
        for (int i = 0; i < buffers.size(); i++) {
            copy.buffers.set(i, buffers.get(i).duplicate());
        }
        copy.position = this.position;
        copy.limit = this.limit;
        return copy;
    }

    /** {@inheritDoc} */
    @Override
    public long readFrom(FileChannel channel) throws IOException {
        long totalRead = 0;
        long pos = position;
        for (int i = (int) (pos / CHUNK_SIZE); i < buffers.size(); i++) {
            ByteBuffer buf = buffers.get(i);
            buf.position((int) (pos % CHUNK_SIZE));
            int read = channel.read(buf);
            if (read == -1) {
                break;
            }
            totalRead += read;
            pos += read;
            if (pos >= limit) {
                break;
            }
        }
        position = pos;
        return totalRead;
    }

    /** {@inheritDoc} */
    @Override
    public String decode(CharsetDecoder decoder)
            throws CharacterCodingException {
        long remainingBytes = limit - position;

        // Cannot allocate more than Integer.MAX_VALUE for CharBuffer
        if (remainingBytes > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                    "Decoding region too large to fit in a CharBuffer: " + remainingBytes
            );
        }

        CharBuffer out = CharBuffer.allocate((int) remainingBytes);
        long pos = position;

        while (remainingBytes > 0) {
            // Locate which underlying buffer we are in
            int bufIndex = (int) (pos / CHUNK_SIZE);
            int bufOffset = (int) (pos % CHUNK_SIZE);

            ByteBuffer srcView = buffers.get(bufIndex).duplicate();
            srcView.position(bufOffset);

            int toRead = (int) Math.min(srcView.remaining(), remainingBytes);
            srcView.limit(bufOffset + toRead);

            CoderResult result = decoder.decode(srcView, out, false);
            if (result.isError()) {
                result.throwException();
            }

            pos += toRead;
            remainingBytes -= toRead;
        }

        // Update this MultiBuffer’s logical position
        this.position = pos;

        out.flip();
        return out.toString();
    }

    /**
     * Wraps the given byte arrays in a new {@code MultiBuffer}.
     *
     * <p>If any array exceeds {@link #CHUNK_SIZE}, it will be split across multiple
     * underlying {@link ByteBuffer}s. The data is copied into new buffers so the
     * returned {@code MultiBuffer} is fully independent.
     *
     * @param chunks the byte arrays to wrap
     * @return a new {@code MultiBuffer} backed by the arrays
     */
    public static MultiBuffer wrap(List<byte[]> chunks) {
        List<ByteBuffer> buffers = new ArrayList<>();

        for (byte[] chunk : chunks) {
            int offset = 0;
            int remaining = chunk.length;

            while (remaining > 0) {
                int toPut = remaining;
                ByteBuffer buf = ByteBuffer.allocate(toPut);
                buf.put(chunk, offset, toPut);
                buf.flip();
                buffers.add(buf);

                offset += toPut;
                remaining -= toPut;
            }
        }

        return new MultiBuffer(buffers);
    }

    /**
     * Creates a read-only {@code MultiBuffer} by memory-mapping the given
     * {@link FileChannel}.
     *
     * @param channel the file channel to map
     * @return a new {@code MultiBuffer} backed by memory-mapped segments
     * @throws IOException if an I/O error occurs
     */
    public static MultiBuffer mapFromChannel(FileChannel channel) throws IOException {
        long size = channel.size();
        if (size <= 0) {
            throw new IllegalArgumentException("File channel has no data");
        }

        MultiBuffer buf = new MultiBuffer(size);
        long remaining = size;

        for (int i = 0; i < buf.buffers.size(); i++) {
            long chunkPos = (long) i * CHUNK_SIZE;
            long chunkSize = Math.min(CHUNK_SIZE, remaining);
            ByteBuffer mapped = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    chunkPos,
                    chunkSize
            );
            buf.buffers.set(i, mapped);
            remaining -= chunkSize;
        }
        return buf;
    }
}