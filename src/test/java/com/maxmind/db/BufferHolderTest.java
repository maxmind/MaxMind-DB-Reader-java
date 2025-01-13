package com.maxmind.db;

import com.maxmind.db.Reader.FileMode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BufferHolderTest {

    static Path createLargeFile() throws IOException {
        final ByteBuffer oneByte = ByteBuffer.wrap(new byte[] {1});
        final Path temp = Files.createTempFile(BigByteBufferTest.class.getName(), "createLargeBuffer");
        temp.toFile().deleteOnExit();
        try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.SPARSE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            Assertions.assertEquals(0, channel.size());
            channel.position(BigByteBufferTest.SIZE - oneByte.capacity());
            channel.write(oneByte);
            channel.close();
            final long size = Files.size(temp);
            Assertions.assertEquals(BigByteBufferTest.SIZE, size);
            Assertions.assertTrue(size > Integer.MAX_VALUE);
            return temp;
        }
    }

    @Test
    public void testMemoryMap() throws IOException {
        final Path temp = createLargeFile();
        final BufferHolder holder = new BufferHolder(temp.toFile(), FileMode.MEMORY_MAPPED);
        final BigByteBuffer bigByteBuffer = holder.get();
        Assertions.assertEquals((byte) 0, bigByteBuffer.get());
        bigByteBuffer.position(BigByteBufferTest.SIZE - 1);
        Assertions.assertEquals((byte) 1, bigByteBuffer.get());
    }

    @Test
    public void testThreadSafe() throws IOException {
        final BufferHolder holder =
            new BufferHolder(
                new ByteArrayInputStream(new byte[] {0, 1})
            );
        final BigByteBuffer bigByteBuffer0 = holder.get();
        bigByteBuffer0.get();
        final BigByteBuffer bigByteBuffer1 = holder.get();
        Assertions.assertNotEquals(bigByteBuffer0.position(), bigByteBuffer1.position());
    }

}
