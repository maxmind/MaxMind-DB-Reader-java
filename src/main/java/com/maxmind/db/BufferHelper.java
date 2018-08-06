package com.maxmind.db;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import com.maxmind.db.Reader.FileMode;

final class BufferHelper {
    static public ByteBuffer open(File database, FileMode mode) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(database, "r")) {
            final FileChannel channel = file.getChannel();
            if (mode == FileMode.MEMORY) {
                ByteBuffer buffer = ByteBuffer.wrap(new byte[(int) channel.size()]);
                if (channel.read(buffer) != buffer.capacity()) {
                    throw new IOException("Unable to read "
                            + database.getName()
                            + " into memory. Unexpected end of stream.");
                }
                return buffer;
            } else {
                return channel.map(MapMode.READ_ONLY, 0, channel.size());
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
    static public ByteBuffer open(InputStream stream) throws IOException {
        if (null == stream) {
            throw new NullPointerException("Unable to use a NULL InputStream");
        }
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final byte[] bytes = new byte[16 * 1024];
        int br;
        while (-1 != (br = stream.read(bytes))) {
            baos.write(bytes, 0, br);
        }
        return ByteBuffer.wrap(baos.toByteArray());
    }
}
