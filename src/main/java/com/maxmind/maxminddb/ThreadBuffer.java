package com.maxmind.maxminddb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import com.maxmind.maxminddb.Reader.FileMode;

final class ThreadBuffer extends ThreadLocal<ByteBuffer> {
    // XXX - DO NOT PASS THIS OUTSIDE THIS CLASS.
    private final ByteBuffer buffer;
    private final RandomAccessFile raf;
    private final FileChannel fc;

    ThreadBuffer(File database, FileMode mode) throws IOException {
        this.raf = new RandomAccessFile(database, "r");
        this.fc = this.raf.getChannel();
        if (mode == FileMode.IN_MEMORY) {
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

    public void close() throws IOException {
        if (this.fc != null) {
            this.fc.close();
        }
        if (this.raf != null) {
            this.raf.close();
        }
    }
}