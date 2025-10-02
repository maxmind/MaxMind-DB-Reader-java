package com.maxmind.db;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class MultiBufferTest {
    static MultiBuffer createBuffer(int chunkSize) {
        try {
            Path tmpFile = Files.createTempFile("test-data", ".bin");
            byte[] data = new byte[]{
                // uint16: 500
                (byte) 0xa2, 0x1, (byte) 0xf4,

                // uint32: 10872
                (byte) 0xc2, 0x2a, 0x78,

                // int32: 500
                0x2, 0x1, 0x1, (byte) 0xf4,

                // boolean: true
                0x1, 0x7,

                // double: 3.14159265359
                0x68, 0x40, 0x9, 0x21, (byte) 0xFB, 0x54, 0x44, 0x2E, (byte) 0xEA,

                // float: 3.14f
                0x40, 0x48, (byte) 0xF5, (byte) 0xC3,

                // string: "123"
                0x43, 0x31, 0x32, 0x33,

                // pointer: 3017
                0x28, 0x3, (byte) 0xc9,

                // array: ["Foo", "人"]
                0x2, 0x4,
                0x43, 0x46, 0x6f, 0x6f,  // "Foo"
                0x43, (byte) 0xe4, (byte) 0xba, (byte) 0xba,  // "人"

                // map: {"en": "Foo", "zh": "人"}
                (byte) 0xe2,
                0x42, 0x65, 0x6e,  // "en"
                0x43, 0x46, 0x6f, 0x6f,  // "Foo"
                0x42, 0x7a, 0x68,  // "zh"
                0x43, (byte) 0xe4, (byte) 0xba, (byte) 0xba,  // "人"

                // uint64: large value (2^16 - 1)
                0x2, 0x2, (byte) 0xff, (byte) 0xff,

                // longer string: "123456789012345678901234567"
                0x5b, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x30,
                0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x30,
                0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37
            };
            Files.write(tmpFile, data);

            try (RandomAccessFile file = new RandomAccessFile(tmpFile.toFile(), "r");
                FileChannel channel = file.getChannel()) {

                MultiBuffer buffer = new MultiBuffer(channel.size(), chunkSize);

                buffer.readFrom(channel, chunkSize);
                buffer.position(0);
                buffer.limit(channel.size());

                return buffer;
            }
        } catch (IOException e) {
            fail("Could not create test buffer: " + e.getMessage());
            return null;
        }
    }

    @Test
    public void testPositionSetter() {
        MultiBuffer buffer = new MultiBuffer(1000);
        buffer.position(500);
        assertEquals(500, buffer.position());
    }

    @Test
    public void testPositionSetterInvalidNegative() {
        MultiBuffer buffer = new MultiBuffer(1000);
        assertThrows(IllegalArgumentException.class, () -> buffer.position(-1));
    }

    @Test
    public void testPositionSetterExceedsLimit() {
        MultiBuffer buffer = new MultiBuffer(1000);
        buffer.limit(500);
        assertThrows(IllegalArgumentException.class, () -> buffer.position(600));
    }

    @Test
    public void testLimitSetter() {
        MultiBuffer buffer = new MultiBuffer(1000);
        buffer.limit(500);
        assertEquals(500, buffer.limit());
    }

    @Test
    public void testLimitSetterInvalidNegative() {
        MultiBuffer buffer = new MultiBuffer(1000);
        assertThrows(IllegalArgumentException.class, () -> buffer.limit(-1));
    }

    @Test
    public void testLimitSetterExceedsCapacity() {
        MultiBuffer buffer = new MultiBuffer(1000);
        assertThrows(IllegalArgumentException.class, () -> buffer.limit(1001));
    }

    @Test
    public void testLimitSetterAdjustsPosition() {
        MultiBuffer buffer = new MultiBuffer(1000);
        buffer.position(800);
        buffer.limit(500);
        assertEquals(500, buffer.position());
    }

    @Test
    public void testGetByIndex() {
        MultiBuffer buffer = createBuffer(24);
        assertEquals(0x2a, buffer.get(4));
        assertEquals(0x1, buffer.get(10));
    }

    @Test
    public void testGetByIndexOutOfBounds() {
        MultiBuffer buffer = createBuffer(24);
        buffer.limit(50);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.get(50));
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.get(-1));
    }

    @Test
    public void testGetSingleByte() {
        MultiBuffer buffer = createBuffer(24);
         assertEquals((byte) 0xa2, buffer.get());
         assertEquals(1, buffer.position());
    }

    @Test
    public void testGetByteArray() {
        MultiBuffer buffer = createBuffer(24);
        byte[] dst = new byte[10];
        buffer.position(32);
        buffer.get(dst);
        byte[] expectedBytes = new byte[]{
                0x2, 0x4,
                0x43, 0x46, 0x6f, 0x6f,
                0x43, (byte) 0xe4, (byte) 0xba, (byte) 0xba};
        assertArrayEquals(expectedBytes, dst);
        assertEquals(42, buffer.position());
    }

    @Test
    public void testGetByteArrayExceedsLimit() {
        MultiBuffer buffer = new MultiBuffer(100);
        buffer.limit(5);
        byte[] dst = new byte[10];
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.get(dst));
    }

    @Test
    public void testGetByteArrayAcrossChunks() {
        MultiBuffer buffer = createBuffer(35);
        byte[] dst = new byte[10];
        buffer.position(32);
        buffer.get(dst);
        byte[] expectedBytes = new byte[]{
                0x2, 0x4,
                0x43, 0x46, 0x6f, 0x6f,
                0x43, (byte) 0xe4, (byte) 0xba, (byte) 0xba};
        assertArrayEquals(expectedBytes, dst);
        assertEquals(42, buffer.position());
    }

    @Test
    public void testGetDouble() {
        MultiBuffer buffer = createBuffer(24);
        buffer.position(13);
        assertEquals(3.14159265359, buffer.getDouble());
        assertEquals(21, buffer.position());
    }

    @Test
    public void testGetDoubleAcrossChunks() {
        MultiBuffer buffer = createBuffer(16);
        buffer.position(13);
        assertEquals(3.14159265359, buffer.getDouble());
        assertEquals(21, buffer.position());
    }

    @Test
    public void testGetFloat() {
        MultiBuffer buffer = createBuffer(26);
        buffer.position(21);
        assertEquals(3.14f, buffer.getFloat());
        assertEquals(25, buffer.position());
    }

    @Test
    public void testGetFloatAcrossChunks() {
        MultiBuffer buffer = createBuffer(22);
        buffer.position(21);
        assertEquals(3.14f, buffer.getFloat());
        assertEquals(25, buffer.position());
    }

    @Test
    public void testDuplicate() {
        MultiBuffer original = new MultiBuffer(1000);
        original.position(100);
        original.limit(800);

        MultiBuffer duplicate = (MultiBuffer) original.duplicate();

        assertEquals(original.capacity(), duplicate.capacity());
        assertEquals(original.position(), duplicate.position());
        assertEquals(original.limit(), duplicate.limit());
        assertEquals(original.buffers.length, duplicate.buffers.length);

        duplicate.position(200);
        assertEquals(100, original.position());
        assertEquals(200, duplicate.position());
    }

    @Test
    public void testWrapValidChunks() {
        ByteBuffer[] chunks = new ByteBuffer[] {
                ByteBuffer.allocate(8),
                ByteBuffer.allocate(3)
        };

        MultiBuffer buffer = new MultiBuffer(chunks, 8);
        assertEquals(11, buffer.capacity());
    }

    @Test
    public void testWrapInvalidChunkSize() {
        ByteBuffer[] chunks = new ByteBuffer[] {
                ByteBuffer.allocate(3),
                ByteBuffer.allocate(8)
        };

        assertThrows(IllegalArgumentException.class, () -> new MultiBuffer(chunks, 8));
    }

    @Test
    public void testReadFromFileChannel(@TempDir Path tempDir) throws IOException {
        // Create test file
        Path testFile = tempDir.resolve("test.dat");
        byte[] testData = new byte[]{
                (byte) 0xa2, 0x1, (byte) 0xf4,
                (byte) 0xc2, 0x2a, 0x78,
                0x2, 0x1, 0x1, (byte) 0xf4,
                0x1, 0x7,
                0x68, 0x40, 0x9, 0x21, (byte) 0xFB, 0x54, 0x44, 0x2E, (byte) 0xEA,
        };
        Files.write(testFile, testData);

        try (FileChannel channel = FileChannel.open(testFile, StandardOpenOption.READ)) {
            MultiBuffer buffer = new MultiBuffer(testData.length);
            long bytesRead = buffer.readFrom(channel);
            assertEquals(21, bytesRead);
            assertEquals(21, buffer.position());
        }
    }

    @Test
    public void testMapFromChannel(@TempDir Path tempDir) throws IOException {
        // Create test file
        Path testFile = tempDir.resolve("test.dat");
        byte[] testData = new byte[]{
                (byte) 0xa2, 0x1, (byte) 0xf4,
                (byte) 0xc2, 0x2a, 0x78,
                0x2, 0x1, 0x1, (byte) 0xf4,
                0x1, 0x7,
                0x68, 0x40, 0x9, 0x21, (byte) 0xFB, 0x54, 0x44, 0x2E, (byte) 0xEA,
        };
        Files.write(testFile, testData);

        try (FileChannel channel = FileChannel.open(testFile, StandardOpenOption.READ)) {
            MultiBuffer buffer = MultiBuffer.mapFromChannel(channel);
            assertEquals(21, buffer.capacity());
        }
    }

    @Test
    public void testMapFromEmptyChannel(@TempDir Path tempDir) throws IOException {
        Path emptyFile = tempDir.resolve("empty.dat");
        Files.createFile(emptyFile);

        try (FileChannel channel = FileChannel.open(emptyFile, StandardOpenOption.READ)) {
            assertThrows(IllegalArgumentException.class, () -> MultiBuffer.mapFromChannel(channel));
        }
    }

    @Test
    public void testDecodeString() throws CharacterCodingException {
        MultiBuffer buffer = createBuffer(22);
        buffer.position(26);
        buffer.limit(29);
        String result = buffer.decode(StandardCharsets.UTF_8.newDecoder());
        assertEquals("123", result);
        assertEquals(29, buffer.position());
    }

    @Test
    public void testDecodeStringTooLarge() {
        MultiBuffer buffer = createBuffer(65);
        buffer.position(62);
        buffer.limit(89);
        assertThrows(IllegalStateException.class, () ->
                buffer.decode(StandardCharsets.UTF_8.newDecoder(), 20));
    }

    @Test
    public void testDecodeAcrossChunks() throws CharacterCodingException {
        MultiBuffer buffer = createBuffer(65);
        buffer.position(62);
        buffer.limit(89);
        String result = buffer.decode(StandardCharsets.UTF_8.newDecoder());
        assertEquals("123456789012345678901234567", result);
        assertEquals(89, buffer.position());
    }
}
