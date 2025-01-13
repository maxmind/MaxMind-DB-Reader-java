package com.maxmind.db;

import com.maxmind.db.Reader.FileMode;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteBigArrayBigList;
import it.unimi.dsi.fastutil.bytes.ByteBigList;
import it.unimi.dsi.fastutil.bytes.ByteBigLists;
import it.unimi.dsi.fastutil.bytes.ByteList;
import it.unimi.dsi.fastutil.bytes.ByteMappedBigList;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

final class BufferHolder {
    // DO NOT PASS OUTSIDE THIS CLASS. Doing so will remove thread safety.
    private final BigByteBuffer buffer;

    BufferHolder(File database, FileMode mode) throws IOException {
        final ByteBigList list;
        try (
            final RandomAccessFile file = new RandomAccessFile(database, "r");
            final FileChannel channel = file.getChannel()
        ) {
            final ByteBigList mapped =
                ByteMappedBigList.map(channel, ByteOrder.BIG_ENDIAN, MapMode.READ_ONLY);
            if (mode == FileMode.MEMORY) {
                list = ByteBigLists.unmodifiable(new ByteBigArrayBigList(mapped));
            } else {
                list = mapped;
            }
        }
        this.buffer = BigByteBuffer.wrap(list);
    }

    /**
     * Construct a {@link BufferHolder} from the provided {@link InputStream}.
     *
     * @param stream the source of my bytes.
     * @throws IOException          if unable to read from your source.
     * @throws NullPointerException if you provide a {@code null} InputStream
     */
    BufferHolder(InputStream stream) throws IOException {
        if (null == stream) {
            throw new NullPointerException("Unable to use a NULL InputStream");
        }
        final ByteBigArrayBigList bigList = new ByteBigArrayBigList();
        final byte[] bytesForStream = new byte[16 * 1024];
        int br;
        final ByteList bytesForBigList = ByteArrayList.wrap(bytesForStream);
        while (-1 != (br = stream.read(bytesForStream))) {
            bigList.addAll(bytesForBigList.subList(0, br));
        }
        bigList.trim();
        this.buffer = BigByteBuffer.wrap(ByteBigLists.unmodifiable(bigList));
    }

    /*
     * Returns a duplicate of the underlying ByteBuffer. The returned ByteBuffer
     * should not be shared between threads.
     */
    BigByteBuffer get() {
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
        // Given that we are not modifying the original ByteBuffer in any way and all currently
        // known and most reasonably imaginable implementations of duplicate() only do read
        // operations on the original buffer object, the risk of not synchronizing this call seems
        // relatively low and worth taking for the performance benefit when lookups are being done
        // from many threads.
        return buffer.duplicate();
    }

}
