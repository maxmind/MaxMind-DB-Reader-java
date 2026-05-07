package com.maxmind.db;

import com.maxmind.db.Reader.FileMode;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

final class BufferHolder {
    // DO NOT PASS OUTSIDE THIS CLASS. Doing so will remove thread safety.
    private final Buffer buffer;

    // Reasonable I/O buffer size for reading from InputStream.
    // This is separate from chunk size which determines MultiBuffer chunk allocation.
    private static final int IO_BUFFER_SIZE = 16 * 1024; // 16KB

    BufferHolder(File database, FileMode mode) throws IOException {
        this(database, mode, MultiBuffer.DEFAULT_CHUNK_SIZE);
    }

    BufferHolder(File database, FileMode mode, int chunkSize) throws IOException {
        if (mode == FileMode.MEMORY) {
            // FileInputStream avoids the per-thread direct ByteBuffer cache that
            // FileChannel.read() populates when reading into a heap buffer. That cache
            // retains the largest direct buffer ever requested — under chunked MEMORY
            // mode that would mean chunkSize bytes of off-heap memory held per loader
            // thread for the JVM's lifetime.
            try (FileInputStream stream = new FileInputStream(database)) {
                long size = database.length();
                var name = database.getName();
                if (size <= chunkSize) {
                    this.buffer = SingleBuffer.wrap(readFully(stream, (int) size, name));
                } else {
                    var fullChunks = (int) (size / chunkSize);
                    var remainder = (int) (size % chunkSize);
                    var totalChunks = fullChunks + (remainder > 0 ? 1 : 0);
                    var buffers = new ByteBuffer[totalChunks];
                    for (int i = 0; i < fullChunks; i++) {
                        buffers[i] = ByteBuffer.wrap(readFully(stream, chunkSize, name));
                    }
                    if (remainder > 0) {
                        buffers[totalChunks - 1] = ByteBuffer.wrap(
                                readFully(stream, remainder, name));
                    }
                    this.buffer = new MultiBuffer(buffers, chunkSize);
                }
            }
        } else {
            try (RandomAccessFile file = new RandomAccessFile(database, "r");
                 FileChannel channel = file.getChannel()) {
                long size = channel.size();
                if (size <= chunkSize) {
                    this.buffer = SingleBuffer.mapFromChannel(channel);
                } else {
                    this.buffer = MultiBuffer.mapFromChannel(channel);
                }
            }
        }
    }

    BufferHolder(InputStream stream, int chunkSize) throws IOException {
        if (null == stream) {
            throw new NullPointerException("Unable to use a NULL InputStream");
        }
        this.buffer = readFromStream(stream, chunkSize);
    }

    // Pre-allocates exactly len bytes. Used by file-backed MEMORY mode where the size is
    // known up front, avoiding the transient peak from ByteArrayOutputStream.grow() and
    // the defensive copy in toByteArray().
    private static byte[] readFully(InputStream stream, int len, String name) throws IOException {
        var data = new byte[len];
        var totalRead = 0;
        while (totalRead < len) {
            var n = stream.read(data, totalRead, len - totalRead);
            if (n < 0) {
                throw new IOException("Unable to read "
                        + name
                        + " into memory. Unexpected end of stream.");
            }
            totalRead += n;
        }
        return data;
    }

    private static Buffer readFromStream(InputStream stream, int chunkSize) throws IOException {
        // Read data from the stream in chunks to support databases >2GB.
        // Invariant: All chunks except the last are exactly chunkSize bytes.
        var chunks = new ArrayList<byte[]>();
        var currentChunkStream = new ByteArrayOutputStream();
        var tmp = new byte[IO_BUFFER_SIZE];
        int read;

        while (-1 != (read = stream.read(tmp))) {
            var offset = 0;
            while (offset < read) {
                var spaceInCurrentChunk = chunkSize - currentChunkStream.size();
                var toWrite = Math.min(spaceInCurrentChunk, read - offset);

                currentChunkStream.write(tmp, offset, toWrite);
                offset += toWrite;

                // When chunk is exactly full, save it and start a new one.
                // This guarantees all non-final chunks are exactly chunkSize.
                if (currentChunkStream.size() == chunkSize) {
                    chunks.add(currentChunkStream.toByteArray());
                    currentChunkStream = new ByteArrayOutputStream();
                }
            }
        }

        // Handle last partial chunk (could be empty if total is multiple of chunkSize)
        if (currentChunkStream.size() > 0) {
            chunks.add(currentChunkStream.toByteArray());
        }

        if (chunks.size() == 1) {
            // For databases that fit in a single chunk, use SingleBuffer
            return SingleBuffer.wrap(chunks.get(0));
        }
        // For large databases, wrap chunks in ByteBuffers and use MultiBuffer
        // Guaranteed: chunks[0..n-2] all have length == chunkSize
        // chunks[n-1] may have length < chunkSize
        var buffers = new ByteBuffer[chunks.size()];
        for (var i = 0; i < chunks.size(); i++) {
            buffers[i] = ByteBuffer.wrap(chunks.get(i));
        }
        return new MultiBuffer(buffers, chunkSize);
    }

    /*
     * Returns a duplicate of the underlying Buffer. The returned Buffer
     * should not be shared between threads.
     */
    Buffer get() {
        // The Java API docs for buffer state:
        //
        //     Buffers are not safe for use by multiple concurrent threads. If a buffer is to be
        //     used by more than one thread then access to the buffer should be controlled by
        //     appropriate synchronization.
        //
        // As such, you may think that this should be synchronized. This used to be the case, but
        // we had several complaints about the synchronization causing contention, e.g.:
        //
        // * https://github.com/maxmind/MaxMind-DB-Reader-java/issues/65
        // * https://github.com/maxmind/MaxMind-DB-Reader-java/pull/69
        //
        // Given that we are not modifying the original Buffer in any way and all currently
        // known and most reasonably imaginable implementations of duplicate() only do read
        // operations on the original buffer object, the risk of not synchronizing this call seems
        // relatively low and worth taking for the performance benefit when lookups are being done
        // from many threads.
        return this.buffer.duplicate();
    }
}
