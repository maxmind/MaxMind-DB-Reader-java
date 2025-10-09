package com.maxmind.db;

import com.maxmind.db.Reader.FileMode;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

final class BufferHolder {
    // DO NOT PASS OUTSIDE THIS CLASS. Doing so will remove thread safety.
    private final Buffer buffer;

    BufferHolder(File database, FileMode mode) throws IOException {
        this(database, mode, MultiBuffer.DEFAULT_CHUNK_SIZE);
    }

    BufferHolder(File database, FileMode mode, int chunkSize) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(database, "r");
             FileChannel channel = file.getChannel()) {
            long size = channel.size();
            if (mode == FileMode.MEMORY) {
                if (size <= chunkSize) {
                    // Allocate, read, and make read-only
                    ByteBuffer buffer = ByteBuffer.allocate((int) size);
                    if (channel.read(buffer) != size) {
                        throw new IOException("Unable to read "
                                + database.getName()
                                + " into memory. Unexpected end of stream.");
                    }
                    buffer.flip();
                    this.buffer = new SingleBuffer(buffer);
                } else {
                    // Allocate chunks, read, and make read-only
                    var fullChunks = (int) (size / chunkSize);
                    var remainder = (int) (size % chunkSize);
                    var totalChunks = fullChunks + (remainder > 0 ? 1 : 0);
                    var buffers = new ByteBuffer[totalChunks];

                    for (int i = 0; i < fullChunks; i++) {
                        buffers[i] = ByteBuffer.allocate(chunkSize);
                    }
                    if (remainder > 0) {
                        buffers[totalChunks - 1] = ByteBuffer.allocate(remainder);
                    }

                    var totalRead = 0L;
                    for (var buffer : buffers) {
                        var read = channel.read(buffer);
                        if (read == -1) {
                            break;
                        }
                        totalRead += read;
                        buffer.flip();
                    }

                    if (totalRead != size) {
                        throw new IOException("Unable to read "
                                + database.getName()
                                + " into memory. Unexpected end of stream.");
                    }

                    this.buffer = new MultiBuffer(buffers, chunkSize);
                }
            } else {
                if (size <= chunkSize) {
                    this.buffer = SingleBuffer.mapFromChannel(channel);
                } else {
                    this.buffer = MultiBuffer.mapFromChannel(channel);
                }
            }
        }
    }

    /**
     * Construct a ThreadBuffer from the provided URL.
     *
     * @param stream the source of my bytes.
     * @throws IOException          if unable to read from your source.
     * @throws NullPointerException if you provide a NULL InputStream
     */
    BufferHolder(InputStream stream) throws IOException {
        this(stream, MultiBuffer.DEFAULT_CHUNK_SIZE);
    }

    BufferHolder(InputStream stream, int chunkSize) throws  IOException {
        if (null == stream) {
            throw new NullPointerException("Unable to use a NULL InputStream");
        }
        var chunks = new ArrayList<ByteBuffer>();
        var total = 0L;
        var tmp = new byte[chunkSize];
        int read;

        while (-1 != (read = stream.read(tmp))) {
            var chunk = ByteBuffer.allocate(read);
            chunk.put(tmp, 0, read);
            chunk.flip();
            chunks.add(chunk);
            total += read;
        }

        if (total <= chunkSize) {
            var data = new byte[(int) total];
            var pos = 0;
            for (var chunk : chunks) {
                System.arraycopy(chunk.array(), 0, data, pos, chunk.capacity());
                pos += chunk.capacity();
            }
            this.buffer = SingleBuffer.wrap(data);
        } else {
            this.buffer = new MultiBuffer(chunks.toArray(new ByteBuffer[0]), chunkSize);
        }
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
