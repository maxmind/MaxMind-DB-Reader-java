package com.maxmind.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;

/**
 * A {@link Buffer} implementation backed by multiple {@link ByteBuffer}s,
 * allowing support for capacities larger than {@link Integer#MAX_VALUE}.
 *
 * <p>This implementation virtually concatenates several
 * {@link ByteBuffer}s (each up to {@link Integer#MAX_VALUE}) and maintains
 * a single logical position and limit across them.
 *
 * <p>Use this when working with databases/files that may exceed 2GB.
 *
 * <p>All underlying {@link ByteBuffer}s are read-only to prevent accidental
 * modification of shared data.
 */
final class MultiBuffer implements Buffer {

    /** Default maximum size per underlying chunk. */
    static final int DEFAULT_CHUNK_SIZE = Integer.MAX_VALUE - 8;

    final ByteBuffer[] buffers;
    private final int chunkSize;
    private final long capacity;

    private long position = 0;
    private long limit;

    /**
     * Creates a new {@code MultiBuffer} backed by the given
     * {@link ByteBuffer}s.
     *
     * <p>The total capacity and limit are set to the sum of the
     * buffer capacities.
     *
     * @param buffers   the backing buffers (cloned into an internal array)
     * @param chunkSize the size of each buffer chunk
     */
    MultiBuffer(ByteBuffer[] buffers, int chunkSize) {
        for (int i = 0; i < buffers.length; i++) {
            var chunk = buffers[i];
            if (chunk.capacity() == chunkSize) {
                continue;
            }
            if (i == buffers.length - 1) {
                // The last chunk can have a different size
                continue;
            }
            throw new IllegalArgumentException("Chunk at index " + i
                    + " is smaller than expected chunk size");
        }

        // Make all buffers read-only
        this.buffers = new ByteBuffer[buffers.length];
        for (int i = 0; i < buffers.length; i++) {
            this.buffers[i] = buffers[i].asReadOnlyBuffer();
        }

        var capacity = 0L;
        for (var buffer : this.buffers) {
            capacity += buffer.capacity();
        }
        this.capacity = capacity;
        this.limit = capacity;
        this.chunkSize = chunkSize;
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
        var value = get(position);
        position++;
        return value;
    }

    /** {@inheritDoc} */
    @Override
    public Buffer get(byte[] dst) {
        if (position > limit - dst.length) {
            throw new IndexOutOfBoundsException(
                    "Read exceeds limit: position=" + position
                            + ", length=" + dst.length
                            + ", limit=" + limit
            );
        }
        var pos = position;
        var offset = 0;
        var length = dst.length;
        while (length > 0) {
            var bufIndex = (int) (pos / this.chunkSize);
            var bufOffset = (int) (pos % this.chunkSize);
            var buf = buffers[bufIndex];
            buf.position(bufOffset);
            var toRead = Math.min(buf.remaining(), length);
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
        var bufIndex = (int) (index / this.chunkSize);
        var offset = (int) (index % this.chunkSize);
        return buffers[bufIndex].get(offset);
    }

    /** {@inheritDoc} */
    @Override
    public double getDouble() {
        var bufIndex = (int) (position / this.chunkSize);
        var off = (int) (position % this.chunkSize);
        var buf = buffers[bufIndex];
        buf.position(off);
        if (buf.remaining() >= 8) {
            var value = buf.getDouble();
            position += 8;
            return value;
        } else {
            var eight = new byte[8];
            get(eight);
            return ByteBuffer.wrap(eight).getDouble();
        }
    }

    /** {@inheritDoc} */
    @Override
    public float getFloat() {
        var bufIndex = (int) (position / this.chunkSize);
        var off = (int) (position % this.chunkSize);
        var buf = buffers[bufIndex];
        buf.position(off);
        if (buf.remaining() >= 4) {
            var value = buf.getFloat();
            position += 4;
            return value;
        } else {
            var four = new byte[4];
            get(four);
            return ByteBuffer.wrap(four).getFloat();
        }
    }

    /** {@inheritDoc} */
    @Override
    public Buffer duplicate() {
        var duplicatedBuffers = new ByteBuffer[buffers.length];
        for (int i = 0; i < buffers.length; i++) {
            duplicatedBuffers[i] = buffers[i].duplicate();
        }
        var copy = new MultiBuffer(duplicatedBuffers, chunkSize);
        copy.position = this.position;
        copy.limit = this.limit;
        return copy;
    }

    /** {@inheritDoc} */
    @Override
    public String decode(CharsetDecoder decoder)
            throws CharacterCodingException {
        return this.decode(decoder, Integer.MAX_VALUE);
    }

    String decode(CharsetDecoder decoder, int maximumSize)
            throws CharacterCodingException {
        var remainingBytes = limit - position;

        if (remainingBytes > maximumSize) {
            throw new IllegalStateException(
                    "Decoding region exceeds the maximum size: " + remainingBytes
            );
        }

        if (remainingBytes == 0) {
            return "";
        }

        var bufIndex = (int) (position / this.chunkSize);
        var bufOffset = (int) (position % this.chunkSize);
        var source = buffers[bufIndex];
        if (remainingBytes <= source.limit() - bufOffset) {
            var savedLimit = source.limit();
            source.position(bufOffset);
            source.limit(bufOffset + (int) remainingBytes);
            try {
                var value = decoder.decode(source).toString();
                this.position += remainingBytes;
                return value;
            } finally {
                source.limit(savedLimit);
            }
        }

        var bytes = new byte[(int) remainingBytes];
        var savedPosition = this.position;
        this.get(bytes);
        this.position = savedPosition;
        var value = decoder.decode(ByteBuffer.wrap(bytes)).toString();
        this.position = this.limit;
        return value;
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
        var size = channel.size();
        if (size <= 0) {
            throw new IllegalArgumentException("File channel has no data");
        }

        var fullChunks = (int) (size / DEFAULT_CHUNK_SIZE);
        var remainder = (int) (size % DEFAULT_CHUNK_SIZE);
        var totalChunks = fullChunks + (remainder > 0 ? 1 : 0);

        var buffers = new ByteBuffer[totalChunks];
        var remaining = size;

        for (int i = 0; i < totalChunks; i++) {
            var chunkPos = (long) i * DEFAULT_CHUNK_SIZE;
            var chunkSize = Math.min(DEFAULT_CHUNK_SIZE, remaining);
            buffers[i] = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    chunkPos,
                    chunkSize
            );
            remaining -= chunkSize;
        }
        return new MultiBuffer(buffers, DEFAULT_CHUNK_SIZE);
    }
}
