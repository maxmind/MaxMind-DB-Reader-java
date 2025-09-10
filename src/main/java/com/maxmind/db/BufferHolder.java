package com.maxmind.db;

import com.maxmind.db.Reader.FileMode;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

final class BufferHolder {
    // DO NOT PASS OUTSIDE THIS CLASS. Doing so will remove thread safety.
    private final Buffer buffer;

    BufferHolder(File database, FileMode mode) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(database, "r");
             FileChannel channel = file.getChannel()) {
            long size = channel.size();
            if (mode == FileMode.MEMORY) {
                Buffer buf;
                if (size <= Integer.MAX_VALUE) {
                    buf = new SingleBuffer(size);
                } else {
                    buf = new MultiBuffer(size);
                }
                if (buf.readFrom(channel) != buf.capacity()) {
                    throw new IOException("Unable to read "
                            + database.getName()
                            + " into memory. Unexpected end of stream.");
                }
                this.buffer = buf;
            } else {
                if (size <= Integer.MAX_VALUE) {
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
        if (null == stream) {
            throw new NullPointerException("Unable to use a NULL InputStream");
        }
        final int chunk_size = 16 * 1024;
        List<byte[]> chunks = new ArrayList<>();
        long total = 0;
        int read;
        byte[] tmp = new byte[chunk_size];

        while (-1 != (read = stream.read(tmp))) {
            byte[] copy = new byte[read];
            System.arraycopy(tmp, 0, copy, 0, read);
            chunks.add(copy);
            total += read;
        }

        if (total <= Integer.MAX_VALUE) {
            byte[] data = new byte[(int) total];
            int pos = 0;
            for (byte[] chunk : chunks) {
                System.arraycopy(chunk, 0, data, pos, chunk.length);
                pos += chunk.length;
            }
            this.buffer = SingleBuffer.wrap(data);
        } else {
            this.buffer = MultiBuffer.wrap(chunks);
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
