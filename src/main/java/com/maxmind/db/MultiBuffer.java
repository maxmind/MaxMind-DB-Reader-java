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
    static final int CHUNK_SIZE = Integer.MAX_VALUE / 2;

    final ByteBuffer[] buffers;
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
        int totalChunks = fullChunks + (remainder > 0 ? 1 : 0);

        this.buffers = new ByteBuffer[totalChunks];

        for (int i = 0; i < fullChunks; i++) {
            buffers[i] = ByteBuffer.allocateDirect(CHUNK_SIZE);
        }
        if (remainder > 0) {
            buffers[totalChunks - 1] = ByteBuffer.allocateDirect(remainder);
        }
    }

    private MultiBuffer(ByteBuffer[] buffers) {
        this.buffers = buffers.clone();
        long capacity = 0;
        for (ByteBuffer buffer : buffers) {
            capacity += buffer.capacity();
        }
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
        if (position + dst.length > limit) {
            throw new IndexOutOfBoundsException(
                    "Read exceeds limit: position=" + position
                            + ", length=" + dst.length
                            + ", limit=" + limit
            );
        }
        long pos = position;
        int offset = 0;
        int length = dst.length;
        while (length > 0) {
            int bufIndex = (int) (pos / CHUNK_SIZE);
            int bufOffset = (int) (pos % CHUNK_SIZE);
            ByteBuffer buf = buffers[bufIndex];
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
        return buffers[bufIndex].get(offset);
    }

    /** {@inheritDoc} */
    @Override
    public double getDouble() {
        int bufIndex = (int) (position / CHUNK_SIZE);
        int off = (int) (position % CHUNK_SIZE);
        ByteBuffer buf = buffers[bufIndex];
        buf.position(off);
        if (buf.remaining() >= 8) {
            double value = buf.getDouble();
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
        ByteBuffer buf = buffers[bufIndex];
        buf.position(off);
        if (buf.remaining() >= 4) {
            float value = buf.getFloat();
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
        for (int i = 0; i < buffers.length; i++) {
            copy.buffers[i] = buffers[i].duplicate();
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
        for (int i = (int) (pos / CHUNK_SIZE); i < buffers.length; i++) {
            ByteBuffer buf = buffers[i];
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

            ByteBuffer srcView = buffers[bufIndex].duplicate();
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
    public static MultiBuffer wrap(ByteBuffer[] chunks) {
        for (int i = 0; i < chunks.length; i++) {
            ByteBuffer chunk = chunks[i];
            if (chunk.capacity() == CHUNK_SIZE) {
                continue;
            }
            if (i == chunks.length - 1) {
                // The last chunk can have a different size
                continue;
            }
            throw new IllegalArgumentException("Chunk at index " + i
            + " is smaller than expected chunk size");
        }

        return new MultiBuffer(chunks);
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

        for (int i = 0; i < buf.buffers.length; i++) {
            long chunkPos = (long) i * CHUNK_SIZE;
            long chunkSize = Math.min(CHUNK_SIZE, remaining);
            ByteBuffer mapped = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    chunkPos,
                    chunkSize
            );
            buf.buffers[i] = mapped;
            remaining -= chunkSize;
        }
        return buf;
    }
}