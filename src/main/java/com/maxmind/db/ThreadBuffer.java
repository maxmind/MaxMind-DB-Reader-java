package com.maxmind.db;

import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.ReadableByteChannel;

import com.maxmind.db.Reader.FileMode;

final class ThreadBuffer extends ThreadLocal<ByteBuffer> implements Closeable {
    /**
     * Construct a ThreadBuffer from the provided URL.
     * @param source the URL that I will read in and use as the source of my
     *               bytes.
     * @return a newly constructed instance based on the contents of your URL.
     * @throws IOException if unable to read from your source, or if your
     * source does not return all of its contents (short read).
     */
    public static ThreadBuffer newInstance(URL source) throws IOException {
        final URLConnection conn = source.openConnection();
        conn.connect();

        final InputStream stream = conn.getInputStream();
        final int length = conn.getContentLength();
        final ByteBuffer buffer = ByteBuffer.allocate(length);
        final ReadableByteChannel channel = Channels.newChannel(stream);
        int bytesRead = 0;
        while (true) {
            final int br = channel.read(buffer);
            if (-1 == br) {
                throw new IOException(String.format(
                        "Short read from %s, wanted %d got %d", source, length, bytesRead));
            }
            bytesRead += br;
            if (length == bytesRead) {
                break;
            }
        }
        return new ThreadBuffer(buffer);
    }

    // DO NOT PASS THESE OUTSIDE THIS CLASS. Doing so will remove thread
    // safety.
    private final ByteBuffer buffer;
    private final RandomAccessFile raf;
    private final FileChannel fc;

    ThreadBuffer(File database, FileMode mode) throws IOException {
        this.raf = new RandomAccessFile(database, "r");
        this.fc = this.raf.getChannel();
        if (mode == FileMode.MEMORY) {
            this.buffer = ByteBuffer.wrap(new byte[(int) this.fc.size()]);
            this.fc.read(this.buffer);
        } else {
            this.buffer = this.fc.map(MapMode.READ_ONLY, 0, this.fc.size());
        }
    }

    // This is just to ease unit testing
    ThreadBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
        this.raf = null;
        this.fc = null;
    }

    @Override
    protected synchronized ByteBuffer initialValue() {
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
