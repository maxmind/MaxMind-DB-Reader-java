package com.maxmind.db;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import com.maxmind.db.Reader.FileMode;

final class BufferHolder implements Closeable {
    // DO NOT PASS THESE OUTSIDE THIS CLASS. Doing so will remove thread
    // safety.
    private final ByteBuffer buffer;
    private final RandomAccessFile raf;
    private final FileChannel fc;

    BufferHolder(File database, FileMode mode) throws IOException {
        this.raf = new RandomAccessFile(database, "r");
        this.fc = this.raf.getChannel();
        if (mode == FileMode.MEMORY) {
            this.buffer = ByteBuffer.wrap(new byte[(int) this.fc.size()]);
            this.fc.read(this.buffer);
        } else {
            this.buffer = this.fc.map(MapMode.READ_ONLY, 0, this.fc.size());
        }
    }

    /**
     * Construct a ThreadBuffer from the provided URL.
     *
     * @param stream
     *            the source of my bytes.
     * @return a newly constructed instance based on the contents of your URL.
     * @throws IOException
     *             if unable to read from your source.
     * @throws NullPointerException
     *             if you provide a NULL InputStream
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
        this.buffer = ByteBuffer.wrap(baos.toByteArray());
        this.raf = null;
        this.fc = null;
    }

    // This is just to ease unit testing
    BufferHolder(ByteBuffer buffer) {
        this.buffer = buffer;
        this.raf = null;
        this.fc = null;
    }

    synchronized ByteBuffer get() {
        return this.buffer.duplicate();
    }

    @Override
    public void close() throws IOException {
        if (this.fc != null) {
            this.fc.close();
        }
        if (this.raf != null) {
            this.raf.close();
        }
    }
}
