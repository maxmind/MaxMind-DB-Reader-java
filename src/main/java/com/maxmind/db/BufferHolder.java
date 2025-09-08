package com.maxmind.db;

import com.maxmind.db.Reader.FileMode;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

final class BufferHolder {
    // DO NOT PASS OUTSIDE THIS CLASS. Doing so will remove thread safety.
    private final Buffer buffer;

    BufferHolder(File database, FileMode mode) throws IOException {
        try (
            final RandomAccessFile file = new RandomAccessFile(database, "r");
            final FileChannel channel = file.getChannel()
        ) {
            if (mode == FileMode.MEMORY) {
                final SingleBuffer buf = SingleBuffer.wrap(new byte[(int) channel.size()]);
                if (buf.readFrom(channel) != buf.capacity()) {
                    throw new IOException("Unable to read "
                        + database.getName()
                        + " into memory. Unexpected end of stream.");
                }
                this.buffer = buf;
            } else {
                this.buffer = SingleBuffer.mapFromChannel(channel);
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
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final byte[] bytes = new byte[16 * 1024];
        int br;
        while (-1 != (br = stream.read(bytes))) {
            baos.write(bytes, 0, br);
        }
        this.buffer = SingleBuffer.wrap(baos.toByteArray());
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
