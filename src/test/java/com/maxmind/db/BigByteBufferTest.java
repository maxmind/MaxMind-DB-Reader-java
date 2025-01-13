package com.maxmind.db;

import it.unimi.dsi.fastutil.bytes.ByteBigArrayBigList;
import it.unimi.dsi.fastutil.bytes.ByteMappedBigList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BigByteBufferTest {

    // past 2 GiB
    static final long SIZE = (long) Integer.MAX_VALUE + 1;

    static BigByteBuffer createLargeBuffer() throws IOException {
        final ByteBuffer oneByte = ByteBuffer.wrap(new byte[] {1});
        final Path temp = Files.createTempFile(BigByteBufferTest.class.getName(), "createLargeBuffer");
        temp.toFile().deleteOnExit();
        try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.SPARSE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            Assertions.assertEquals(0L, channel.size());
            channel.position(SIZE - oneByte.capacity());
            channel.write(oneByte);
            Assertions.assertEquals(SIZE, channel.size());
            Assertions.assertTrue(channel.size() > Integer.MAX_VALUE);
            channel.position(0L);
            return BigByteBuffer.wrap(ByteMappedBigList.map(channel));
        }
    }

    static Stream<Arguments> intsProvider() {
        return IntStream.of(
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            0,
            1,
            -1
        ).mapToObj(
            i -> {
                byte[] bytes = new byte[4];
                ByteBuffer b = ByteBuffer.wrap(bytes);
                b.putInt(i);
                return Arguments.arguments(i, bytes);
            }
        );
    }

    static Stream<Arguments> longsProvider() {
        return LongStream.of(
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            0L,
            1L,
            -1L
        ).mapToObj(
            i -> {
                byte[] bytes = new byte[8];
                ByteBuffer b = ByteBuffer.wrap(bytes);
                b.putLong(i);
                return Arguments.arguments(i, bytes);
            }
        );
    }

    @Test
    public void testLargeFile() throws IOException {
        final BigByteBuffer buffer = createLargeBuffer();
        Assertions.assertEquals((byte) 0, buffer.get(SIZE - 2));
        Assertions.assertEquals((byte) 1, buffer.get(SIZE - 1));
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> buffer.get(SIZE));
    }

    @ParameterizedTest
    @MethodSource("intsProvider")
    public final void testGetInt(int i, byte[] bytes) {
        final BigByteBuffer buffer = BigByteBuffer.wrap(new ByteBigArrayBigList(new byte[][] {bytes}));
        final int actual = buffer.getInt();
        Assertions.assertEquals(i, actual);
    }


    @ParameterizedTest
    @MethodSource("longsProvider")
    public final void testGetLong(long i, byte[] bytes) {
        final BigByteBuffer buffer = BigByteBuffer.wrap(new ByteBigArrayBigList(new byte[][] {bytes}));
        final long actual = buffer.getLong();
        Assertions.assertEquals(i, actual);
    }

    @Test
    public final void testGetDouble() {
        for (Map.Entry<Double, byte[]> entry: DecoderTest.doubles().entrySet()) {
            final BigByteBuffer buffer = BigByteBuffer.wrap(new ByteBigArrayBigList(new byte[][] {entry.getValue()}));
            // skip the type byte
            buffer.position(1);
            final double actual = buffer.getDouble();
            Assertions.assertEquals(entry.getKey(), actual);
        }
    }

    @Test
    public final void testGetFloat() {
        for (Map.Entry<Float, byte[]> entry: DecoderTest.floats().entrySet()) {
            final BigByteBuffer buffer = BigByteBuffer.wrap(new ByteBigArrayBigList(new byte[][] {entry.getValue()}));
            // skip the type byte and extended type byte
            buffer.position(2);
            final float actual = buffer.getFloat();
            Assertions.assertEquals(entry.getKey(), actual);
        }
    }

}
