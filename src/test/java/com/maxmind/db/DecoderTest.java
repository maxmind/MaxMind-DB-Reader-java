package com.maxmind.db;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@SuppressWarnings({"boxing", "static-method"})
public class DecoderTest {

    private static Map<Integer, byte[]> int32() {
        int max = (2 << 30) - 1;
        HashMap<Integer, byte[]> int32 = new HashMap<Integer, byte[]>();

        int32.put(0, new byte[]{0x0, 0x1});
        int32.put(-1, new byte[]{0x4, 0x1, (byte) 0xff, (byte) 0xff,
                (byte) 0xff, (byte) 0xff});
        int32.put((2 << 7) - 1, new byte[]{0x1, 0x1, (byte) 0xff});
        int32.put(1 - (2 << 7), new byte[]{0x4, 0x1, (byte) 0xff,
                (byte) 0xff, (byte) 0xff, 0x1});
        int32.put(500, new byte[]{0x2, 0x1, 0x1, (byte) 0xf4});

        int32.put(-500, new byte[]{0x4, 0x1, (byte) 0xff, (byte) 0xff,
                (byte) 0xfe, 0xc});

        int32.put((2 << 15) - 1, new byte[]{0x2, 0x1, (byte) 0xff,
                (byte) 0xff});
        int32.put(1 - (2 << 15), new byte[]{0x4, 0x1, (byte) 0xff,
                (byte) 0xff, 0x0, 0x1});
        int32.put((2 << 23) - 1, new byte[]{0x3, 0x1, (byte) 0xff,
                (byte) 0xff, (byte) 0xff});
        int32.put(1 - (2 << 23), new byte[]{0x4, 0x1, (byte) 0xff, 0x0, 0x0,
                0x1});
        int32.put(max, new byte[]{0x4, 0x1, 0x7f, (byte) 0xff, (byte) 0xff,
                (byte) 0xff});
        int32.put(-max, new byte[]{0x4, 0x1, (byte) 0x80, 0x0, 0x0, 0x1});
        return int32;
    }

    private static Map<Long, byte[]> uint32() {
        long max = (((long) 1) << 32) - 1;
        HashMap<Long, byte[]> uint32s = new HashMap<Long, byte[]>();

        uint32s.put((long) 0, new byte[]{(byte) 0xc0});
        uint32s.put((long) ((1 << 8) - 1), new byte[]{(byte) 0xc1,
                (byte) 0xff});
        uint32s.put((long) 500, new byte[]{(byte) 0xc2, 0x1, (byte) 0xf4});
        uint32s.put((long) 10872, new byte[]{(byte) 0xc2, 0x2a, 0x78});
        uint32s.put((long) ((1 << 16) - 1), new byte[]{(byte) 0xc2,
                (byte) 0xff, (byte) 0xff});
        uint32s.put((long) ((1 << 24) - 1), new byte[]{(byte) 0xc3,
                (byte) 0xff, (byte) 0xff, (byte) 0xff});
        uint32s.put(max, new byte[]{(byte) 0xc4, (byte) 0xff, (byte) 0xff,
                (byte) 0xff, (byte) 0xff});

        return uint32s;
    }

    private static Map<Integer, byte[]> uint16() {
        int max = (1 << 16) - 1;

        Map<Integer, byte[]> uint16s = new HashMap<Integer, byte[]>();

        uint16s.put(0, new byte[]{(byte) 0xa0});
        uint16s.put((1 << 8) - 1, new byte[]{(byte) 0xa1, (byte) 0xff});
        uint16s.put(500, new byte[]{(byte) 0xa2, 0x1, (byte) 0xf4});
        uint16s.put(10872, new byte[]{(byte) 0xa2, 0x2a, 0x78});
        uint16s.put(max, new byte[]{(byte) 0xa2, (byte) 0xff, (byte) 0xff});
        return uint16s;
    }

    private static Map<BigInteger, byte[]> largeUint(int bits) {
        Map<BigInteger, byte[]> uints = new HashMap<BigInteger, byte[]>();

        byte ctrlByte = (byte) (bits == 64 ? 0x2 : 0x3);

        uints.put(BigInteger.valueOf(0), new byte[]{0x0, ctrlByte});
        uints.put(BigInteger.valueOf(500), new byte[]{0x2, ctrlByte, 0x1,
                (byte) 0xf4});
        uints.put(BigInteger.valueOf(10872), new byte[]{0x2, ctrlByte, 0x2a,
                0x78});

        for (int power = 1; power <= bits / 8; power++) {

            BigInteger key = BigInteger.valueOf(2).pow(8 * power)
                    .subtract(BigInteger.valueOf(1));

            byte[] value = new byte[2 + power];
            value[0] = (byte) power;
            value[1] = ctrlByte;
            for (int i = 2; i < value.length; i++) {
                value[i] = (byte) 0xff;
            }
            uints.put(key, value);
        }
        return uints;

    }

    private static Map<Long, byte[]> pointers() {
        Map<Long, byte[]> pointers = new HashMap<Long, byte[]>();

        pointers.put((long) 0, new byte[]{0x20, 0x0});
        pointers.put((long) 5, new byte[]{0x20, 0x5});
        pointers.put((long) 10, new byte[]{0x20, 0xa});
        pointers.put((long) ((1 << 10) - 1), new byte[]{0x23, (byte) 0xff,});
        pointers.put((long) 3017, new byte[]{0x28, 0x3, (byte) 0xc9});
        pointers.put((long) ((1 << 19) - 5), new byte[]{0x2f, (byte) 0xf7,
                (byte) 0xfb});
        pointers.put((long) ((1 << 19) + (1 << 11) - 1), new byte[]{0x2f,
                (byte) 0xff, (byte) 0xff});
        pointers.put((long) ((1 << 27) - 2), new byte[]{0x37, (byte) 0xf7,
                (byte) 0xf7, (byte) 0xfe});
        pointers.put((((long) 1) << 27) + (1 << 19) + (1 << 11) - 1,
                new byte[]{0x37, (byte) 0xff, (byte) 0xff, (byte) 0xff});

        pointers.put((((long) 1) << 31) - 1, new byte[]{0x38, (byte) 0x7f,
                (byte) 0xff, (byte) 0xff, (byte) 0xff});

        return pointers;
    }

    private static Map<String, byte[]> strings() {
        Map<String, byte[]> strings = new HashMap<String, byte[]>();

        DecoderTest.addTestString(strings, (byte) 0x40, "");
        DecoderTest.addTestString(strings, (byte) 0x41, "1");
        DecoderTest.addTestString(strings, (byte) 0x43, "人");
        DecoderTest.addTestString(strings, (byte) 0x43, "123");
        DecoderTest.addTestString(strings, (byte) 0x5b,
                "123456789012345678901234567");
        DecoderTest.addTestString(strings, (byte) 0x5c,
                "1234567890123456789012345678");
        DecoderTest.addTestString(strings, (byte) 0x5c,
                "1234567890123456789012345678");
        DecoderTest.addTestString(strings, new byte[]{0x5d, 0x0},
                "12345678901234567890123456789");
        DecoderTest.addTestString(strings, new byte[]{0x5d, 0x1},
                "123456789012345678901234567890");

        DecoderTest
                .addTestString(strings, new byte[]{0x5e, 0x0, (byte) 0xd7},
                        DecoderTest.xString(500));
        DecoderTest.addTestString(strings,
                new byte[]{0x5e, 0x6, (byte) 0xb3},
                DecoderTest.xString(2000));
        DecoderTest.addTestString(strings,
                new byte[]{0x5f, 0x0, 0x10, 0x53,},
                DecoderTest.xString(70000));

        return strings;

    }

    private static Map<byte[], byte[]> bytes() {
        Map<byte[], byte[]> bytes = new HashMap<byte[], byte[]>();

        Map<String, byte[]> strings = DecoderTest.strings();

        for (String s : strings.keySet()) {
            byte[] ba = strings.get(s);
            ba[0] ^= 0xc0;

            bytes.put(s.getBytes(Charset.forName("UTF-8")), ba);
        }

        return bytes;
    }

    private static String xString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append("x");
        }
        return sb.toString();
    }

    private static void addTestString(Map<String, byte[]> tests, byte ctrl,
                                      String str) {
        DecoderTest.addTestString(tests, new byte[]{ctrl}, str);
    }

    private static void addTestString(Map<String, byte[]> tests, byte ctrl[],
                                      String str) {

        byte[] sb = str.getBytes(Charset.forName("UTF-8"));
        byte[] bytes = new byte[ctrl.length + sb.length];

        System.arraycopy(ctrl, 0, bytes, 0, ctrl.length);
        System.arraycopy(sb, 0, bytes, ctrl.length, sb.length);
        tests.put(str, bytes);
    }

    private static Map<Double, byte[]> doubles() {
        Map<Double, byte[]> doubles = new HashMap<Double, byte[]>();
        doubles.put(0.0, new byte[]{0x68, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
                0x0});
        doubles.put(0.5, new byte[]{0x68, 0x3F, (byte) 0xE0, 0x0, 0x0, 0x0,
                0x0, 0x0, 0x0});
        doubles.put(3.14159265359, new byte[]{0x68, 0x40, 0x9, 0x21,
                (byte) 0xFB, 0x54, 0x44, 0x2E, (byte) 0xEA});
        doubles.put(123.0, new byte[]{0x68, 0x40, 0x5E, (byte) 0xC0, 0x0,
                0x0, 0x0, 0x0, 0x0});
        doubles.put(1073741824.12457, new byte[]{0x68, 0x41, (byte) 0xD0,
                0x0, 0x0, 0x0, 0x7, (byte) 0xF8, (byte) 0xF4});
        doubles.put(-0.5, new byte[]{0x68, (byte) 0xBF, (byte) 0xE0, 0x0,
                0x0, 0x0, 0x0, 0x0, 0x0});
        doubles.put(-3.14159265359, new byte[]{0x68, (byte) 0xC0, 0x9, 0x21,
                (byte) 0xFB, 0x54, 0x44, 0x2E, (byte) 0xEA});
        doubles.put(-1073741824.12457, new byte[]{0x68, (byte) 0xC1,
                (byte) 0xD0, 0x0, 0x0, 0x0, 0x7, (byte) 0xF8, (byte) 0xF4});

        return doubles;
    }

    private static Map<Float, byte[]> floats() {
        Map<Float, byte[]> floats = new HashMap<Float, byte[]>();
        floats.put((float) 0.0, new byte[]{0x4, 0x8, 0x0, 0x0, 0x0, 0x0});
        floats.put((float) 1.0, new byte[]{0x4, 0x8, 0x3F, (byte) 0x80, 0x0,
                0x0});
        floats.put((float) 1.1, new byte[]{0x4, 0x8, 0x3F, (byte) 0x8C,
                (byte) 0xCC, (byte) 0xCD});
        floats.put((float) 3.14, new byte[]{0x4, 0x8, 0x40, 0x48,
                (byte) 0xF5, (byte) 0xC3});
        floats.put((float) 9999.99, new byte[]{0x4, 0x8, 0x46, 0x1C, 0x3F,
                (byte) 0xF6});
        floats.put((float) -1.0, new byte[]{0x4, 0x8, (byte) 0xBF,
                (byte) 0x80, 0x0, 0x0});
        floats.put((float) -1.1, new byte[]{0x4, 0x8, (byte) 0xBF,
                (byte) 0x8C, (byte) 0xCC, (byte) 0xCD});
        floats.put((float) -3.14, new byte[]{0x4, 0x8, (byte) 0xC0, 0x48,
                (byte) 0xF5, (byte) 0xC3});
        floats.put((float) -9999.99, new byte[]{0x4, 0x8, (byte) 0xC6, 0x1C,
                0x3F, (byte) 0xF6});

        return floats;
    }

    private static Map<Boolean, byte[]> booleans() {
        Map<Boolean, byte[]> booleans = new HashMap<Boolean, byte[]>();

        booleans.put(Boolean.FALSE, new byte[]{0x0, 0x7});
        booleans.put(Boolean.TRUE, new byte[]{0x1, 0x7});
        return booleans;
    }

    private static Map<ObjectNode, byte[]> maps() {
        Map<ObjectNode, byte[]> maps = new HashMap<ObjectNode, byte[]>();

        ObjectMapper om = new ObjectMapper();

        ObjectNode empty = om.createObjectNode();
        maps.put(empty, new byte[]{(byte) 0xe0});

        ObjectNode one = om.createObjectNode();
        one.put("en", "Foo");
        maps.put(one, new byte[]{(byte) 0xe1, /* en */0x42, 0x65, 0x6e,
        /* Foo */0x43, 0x46, 0x6f, 0x6f});

        ObjectNode two = om.createObjectNode();
        two.put("en", "Foo");
        two.put("zh", "人");
        maps.put(two, new byte[]{(byte) 0xe2,
        /* en */
                0x42, 0x65, 0x6e,
        /* Foo */
                0x43, 0x46, 0x6f, 0x6f,
        /* zh */
                0x42, 0x7a, 0x68,
        /* 人 */
                0x43, (byte) 0xe4, (byte) 0xba, (byte) 0xba});

        ObjectNode nested = om.createObjectNode();
        nested.set("name", two);

        maps.put(nested, new byte[]{(byte) 0xe1, /* name */
                0x44, 0x6e, 0x61, 0x6d, 0x65, (byte) 0xe2,/* en */
                0x42, 0x65, 0x6e,
        /* Foo */
                0x43, 0x46, 0x6f, 0x6f,
        /* zh */
                0x42, 0x7a, 0x68,
        /* 人 */
                0x43, (byte) 0xe4, (byte) 0xba, (byte) 0xba});

        ObjectNode guess = om.createObjectNode();
        ArrayNode languages = om.createArrayNode();
        languages.add("en");
        languages.add("zh");
        guess.set("languages", languages);
        maps.put(guess, new byte[]{(byte) 0xe1,/* languages */
                0x49, 0x6c, 0x61, 0x6e, 0x67, 0x75, 0x61, 0x67, 0x65, 0x73,
        /* array */
                0x2, 0x4,
        /* en */
                0x42, 0x65, 0x6e,
        /* zh */
                0x42, 0x7a, 0x68});

        return maps;
    }

    private static Map<ArrayNode, byte[]> arrays() {
        Map<ArrayNode, byte[]> arrays = new HashMap<ArrayNode, byte[]>();
        ObjectMapper om = new ObjectMapper();

        ArrayNode f1 = om.createArrayNode();
        f1.add("Foo");
        arrays.put(f1, new byte[]{0x1, 0x4,
        /* Foo */
                0x43, 0x46, 0x6f, 0x6f});

        ArrayNode f2 = om.createArrayNode();
        f2.add("Foo");
        f2.add("人");
        arrays.put(f2, new byte[]{0x2, 0x4,
        /* Foo */
                0x43, 0x46, 0x6f, 0x6f,
        /* 人 */
                0x43, (byte) 0xe4, (byte) 0xba, (byte) 0xba});

        ArrayNode empty = om.createArrayNode();
        arrays.put(empty, new byte[]{0x0, 0x4});

        return arrays;
    }

    @Test
    public void testUint16() throws IOException {
        DecoderTest.testTypeDecoding(Decoder.Type.UINT16, uint16());
    }

    @Test
    public void testUint32() throws IOException {
        DecoderTest.testTypeDecoding(Decoder.Type.UINT32, uint32());
    }

    @Test
    public void testInt32() throws IOException {
        DecoderTest.testTypeDecoding(Decoder.Type.INT32, int32());
    }

    @Test
    public void testUint64() throws IOException {
        DecoderTest.testTypeDecoding(Decoder.Type.UINT64, largeUint(64));
    }

    @Test
    public void testUint128() throws IOException {
        DecoderTest.testTypeDecoding(Decoder.Type.UINT128, largeUint(128));
    }

    @Test
    public void testDoubles() throws IOException {
        DecoderTest
                .testTypeDecoding(Decoder.Type.DOUBLE, DecoderTest.doubles());
    }

    @Test
    public void testFloats() throws IOException {
        DecoderTest.testTypeDecoding(Decoder.Type.FLOAT, DecoderTest.floats());
    }

    @Test
    public void testPointers() throws IOException {
        DecoderTest.testTypeDecoding(Decoder.Type.POINTER, pointers());
    }

    @Test
    public void testStrings() throws IOException {
        DecoderTest.testTypeDecoding(Decoder.Type.UTF8_STRING,
                DecoderTest.strings());
    }

    @Test
    public void testBooleans() throws IOException {
        DecoderTest.testTypeDecoding(Decoder.Type.BOOLEAN,
                DecoderTest.booleans());
    }

    @Test
    public void testBytes() throws IOException {
        DecoderTest.testTypeDecoding(Decoder.Type.BYTES, DecoderTest.bytes());
    }

    @Test
    public void testMaps() throws IOException {
        DecoderTest.testTypeDecoding(Decoder.Type.MAP, DecoderTest.maps());
    }

    @Test
    public void testArrays() throws IOException {
        DecoderTest.testTypeDecoding(Decoder.Type.ARRAY, DecoderTest.arrays());
    }

    private static <T> void testTypeDecoding(Decoder.Type type, Map<T, byte[]> tests)
            throws IOException {

        NodeCache cache = new CHMCache();

        for (Map.Entry<T, byte[]> entry : tests.entrySet()) {
            T expect = entry.getKey();
            byte[] input = entry.getValue();

            String desc = "decoded " + type.name() + " - " + expect;
            FileChannel fc = DecoderTest.getFileChannel(input);
            MappedByteBuffer mmap = fc.map(MapMode.READ_ONLY, 0, fc.size());
            try {

                Decoder decoder = new Decoder(cache, mmap, 0);
                decoder.POINTER_TEST_HACK = true;

                // XXX - this could be streamlined
                if (type.equals(Decoder.Type.BYTES)) {
                    assertArrayEquals(desc, (byte[]) expect, decoder.decode(0).binaryValue());
                } else if (type.equals(Decoder.Type.ARRAY)) {
                    assertEquals(desc, expect, decoder.decode(0));
                } else if (type.equals(Decoder.Type.UINT16)
                        || type.equals(Decoder.Type.INT32)) {
                    assertEquals(desc, expect, decoder.decode(0).asInt());
                } else if (type.equals(Decoder.Type.UINT32)
                        || type.equals(Decoder.Type.POINTER)) {
                    assertEquals(desc, expect, decoder.decode(0).asLong());
                } else if (type.equals(Decoder.Type.UINT64)
                        || type.equals(Decoder.Type.UINT128)) {
                    assertEquals(desc, expect, decoder.decode(0).bigIntegerValue());
                } else if (type.equals(Decoder.Type.DOUBLE)) {
                    assertEquals(desc, expect, decoder.decode(0).asDouble());
                } else if (type.equals(Decoder.Type.FLOAT)) {
                    assertEquals(desc, new FloatNode((Float) expect), decoder.decode(0));
                } else if (type.equals(Decoder.Type.UTF8_STRING)) {
                    assertEquals(desc, expect, decoder.decode(0).asText());
                } else if (type.equals(Decoder.Type.BOOLEAN)) {
                    assertEquals(desc, expect, decoder.decode(0).asBoolean());
                } else {
                    assertEquals(desc, expect, decoder.decode(0));
                }
            } finally {
                fc.close();
            }
        }
    }

    /*
     * I really didn't want to create temporary files for these tests, but it is
     * pretty hard to abstract away from the file io system in a way that is
     * Java 6 compatible
     */
    private static FileChannel getFileChannel(byte[] data) throws IOException {
        File file = File.createTempFile(UUID.randomUUID().toString(), "tmp");
        file.deleteOnExit();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel fc = raf.getChannel();
        fc.write(ByteBuffer.wrap(data));
        raf.close();
        fc.close();
        raf = new RandomAccessFile(file, "r");
        return raf.getChannel();
    }

}
