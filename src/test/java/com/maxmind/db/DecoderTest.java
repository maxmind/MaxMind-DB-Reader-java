package com.maxmind.db;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"boxing", "static-method"})
public class DecoderTest {

    private static Map<Integer, byte[]> int32() {
        int max = (2 << 30) - 1;
        HashMap<Integer, byte[]> int32 = new HashMap<>();

        int32.put(0, new byte[] {0x0, 0x1});
        int32.put(-1, new byte[] {0x4, 0x1, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff});
        int32.put((2 << 7) - 1, new byte[] {0x1, 0x1, (byte) 0xff});
        int32.put(1 - (2 << 7), new byte[] {0x4, 0x1, (byte) 0xff,
            (byte) 0xff, (byte) 0xff, 0x1});
        int32.put(500, new byte[] {0x2, 0x1, 0x1, (byte) 0xf4});

        int32.put(-500, new byte[] {0x4, 0x1, (byte) 0xff, (byte) 0xff,
            (byte) 0xfe, 0xc});

        int32.put((2 << 15) - 1, new byte[] {0x2, 0x1, (byte) 0xff,
            (byte) 0xff});
        int32.put(1 - (2 << 15), new byte[] {0x4, 0x1, (byte) 0xff,
            (byte) 0xff, 0x0, 0x1});
        int32.put((2 << 23) - 1, new byte[] {0x3, 0x1, (byte) 0xff,
            (byte) 0xff, (byte) 0xff});
        int32.put(1 - (2 << 23), new byte[] {0x4, 0x1, (byte) 0xff, 0x0, 0x0,
            0x1});
        int32.put(max, new byte[] {0x4, 0x1, 0x7f, (byte) 0xff, (byte) 0xff,
            (byte) 0xff});
        int32.put(-max, new byte[] {0x4, 0x1, (byte) 0x80, 0x0, 0x0, 0x1});
        return int32;
    }

    private static Map<Long, byte[]> uint32() {
        long max = (((long) 1) << 32) - 1;
        HashMap<Long, byte[]> uint32s = new HashMap<>();

        uint32s.put((long) 0, new byte[] {(byte) 0xc0});
        uint32s.put((long) ((1 << 8) - 1), new byte[] {(byte) 0xc1,
            (byte) 0xff});
        uint32s.put((long) 500, new byte[] {(byte) 0xc2, 0x1, (byte) 0xf4});
        uint32s.put((long) 10872, new byte[] {(byte) 0xc2, 0x2a, 0x78});
        uint32s.put((long) ((1 << 16) - 1), new byte[] {(byte) 0xc2,
            (byte) 0xff, (byte) 0xff});
        uint32s.put((long) ((1 << 24) - 1), new byte[] {(byte) 0xc3,
            (byte) 0xff, (byte) 0xff, (byte) 0xff});
        uint32s.put(max, new byte[] {(byte) 0xc4, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff});

        return uint32s;
    }

    private static Map<Integer, byte[]> uint16() {
        int max = (1 << 16) - 1;

        Map<Integer, byte[]> uint16s = new HashMap<>();

        uint16s.put(0, new byte[] {(byte) 0xa0});
        uint16s.put((1 << 8) - 1, new byte[] {(byte) 0xa1, (byte) 0xff});
        uint16s.put(500, new byte[] {(byte) 0xa2, 0x1, (byte) 0xf4});
        uint16s.put(10872, new byte[] {(byte) 0xa2, 0x2a, 0x78});
        uint16s.put(max, new byte[] {(byte) 0xa2, (byte) 0xff, (byte) 0xff});
        return uint16s;
    }

    private static Map<BigInteger, byte[]> largeUint(int bits) {
        Map<BigInteger, byte[]> uints = new HashMap<>();

        byte ctrlByte = (byte) (bits == 64 ? 0x2 : 0x3);

        uints.put(BigInteger.valueOf(0), new byte[] {0x0, ctrlByte});
        uints.put(BigInteger.valueOf(500), new byte[] {0x2, ctrlByte, 0x1,
            (byte) 0xf4});
        uints.put(BigInteger.valueOf(10872), new byte[] {0x2, ctrlByte, 0x2a,
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
        Map<Long, byte[]> pointers = new HashMap<>();

        pointers.put((long) 0, new byte[] {0x20, 0x0});
        pointers.put((long) 5, new byte[] {0x20, 0x5});
        pointers.put((long) 10, new byte[] {0x20, 0xa});
        pointers.put((long) ((1 << 10) - 1), new byte[] {0x23, (byte) 0xff,});
        pointers.put((long) 3017, new byte[] {0x28, 0x3, (byte) 0xc9});
        pointers.put((long) ((1 << 19) - 5), new byte[] {0x2f, (byte) 0xf7,
            (byte) 0xfb});
        pointers.put((long) ((1 << 19) + (1 << 11) - 1), new byte[] {0x2f,
            (byte) 0xff, (byte) 0xff});
        pointers.put((long) ((1 << 27) - 2), new byte[] {0x37, (byte) 0xf7,
            (byte) 0xf7, (byte) 0xfe});
        pointers.put((((long) 1) << 27) + (1 << 19) + (1 << 11) - 1,
            new byte[] {0x37, (byte) 0xff, (byte) 0xff, (byte) 0xff});

        pointers.put((((long) 1) << 31) - 1, new byte[] {0x38, (byte) 0x7f,
            (byte) 0xff, (byte) 0xff, (byte) 0xff});

        return pointers;
    }

    private static Map<String, byte[]> strings() {
        Map<String, byte[]> strings = new HashMap<>();

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
        DecoderTest.addTestString(strings, new byte[] {0x5d, 0x0},
            "12345678901234567890123456789");
        DecoderTest.addTestString(strings, new byte[] {0x5d, (byte) 128},
            "x".repeat(157));

        DecoderTest
            .addTestString(strings, new byte[] {0x5d, 0x0, (byte) 0xd7},
                "x".repeat(500));

        DecoderTest
            .addTestString(strings, new byte[] {0x5e, 0x0, (byte) 0xd7},
                "x".repeat(500));
        DecoderTest.addTestString(strings,
            new byte[] {0x5e, 0x6, (byte) 0xb3},
            "x".repeat(2000));
        DecoderTest.addTestString(strings,
            new byte[] {0x5f, 0x0, 0x10, 0x53,},
            "x".repeat(70000));

        return strings;

    }

    private static Map<byte[], byte[]> bytes() {
        Map<byte[], byte[]> bytes = new HashMap<>();

        Map<String, byte[]> strings = DecoderTest.strings();

        for (String s : strings.keySet()) {
            byte[] ba = strings.get(s);
            ba[0] ^= 0xc0;

            bytes.put(s.getBytes(StandardCharsets.UTF_8), ba);
        }

        return bytes;
    }

    private static void addTestString(Map<String, byte[]> tests, byte ctrl,
                                      String str) {
        DecoderTest.addTestString(tests, new byte[] {ctrl}, str);
    }

    private static void addTestString(Map<String, byte[]> tests, byte[] ctrl,
                                      String str) {

        byte[] sb = str.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[ctrl.length + sb.length];

        System.arraycopy(ctrl, 0, bytes, 0, ctrl.length);
        System.arraycopy(sb, 0, bytes, ctrl.length, sb.length);
        tests.put(str, bytes);
    }

    private static Map<Double, byte[]> doubles() {
        Map<Double, byte[]> doubles = new HashMap<>();
        doubles.put(0.0, new byte[] {0x68, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
            0x0});
        doubles.put(0.5, new byte[] {0x68, 0x3F, (byte) 0xE0, 0x0, 0x0, 0x0,
            0x0, 0x0, 0x0});
        doubles.put(3.14159265359, new byte[] {0x68, 0x40, 0x9, 0x21,
            (byte) 0xFB, 0x54, 0x44, 0x2E, (byte) 0xEA});
        doubles.put(123.0, new byte[] {0x68, 0x40, 0x5E, (byte) 0xC0, 0x0,
            0x0, 0x0, 0x0, 0x0});
        doubles.put(1073741824.12457, new byte[] {0x68, 0x41, (byte) 0xD0,
            0x0, 0x0, 0x0, 0x7, (byte) 0xF8, (byte) 0xF4});
        doubles.put(-0.5, new byte[] {0x68, (byte) 0xBF, (byte) 0xE0, 0x0,
            0x0, 0x0, 0x0, 0x0, 0x0});
        doubles.put(-3.14159265359, new byte[] {0x68, (byte) 0xC0, 0x9, 0x21,
            (byte) 0xFB, 0x54, 0x44, 0x2E, (byte) 0xEA});
        doubles.put(-1073741824.12457, new byte[] {0x68, (byte) 0xC1,
            (byte) 0xD0, 0x0, 0x0, 0x0, 0x7, (byte) 0xF8, (byte) 0xF4});

        return doubles;
    }

    private static Map<Float, byte[]> floats() {
        Map<Float, byte[]> floats = new HashMap<>();
        floats.put((float) 0.0, new byte[] {0x4, 0x8, 0x0, 0x0, 0x0, 0x0});
        floats.put((float) 1.0, new byte[] {0x4, 0x8, 0x3F, (byte) 0x80, 0x0,
            0x0});
        floats.put((float) 1.1, new byte[] {0x4, 0x8, 0x3F, (byte) 0x8C,
            (byte) 0xCC, (byte) 0xCD});
        floats.put((float) 3.14, new byte[] {0x4, 0x8, 0x40, 0x48,
            (byte) 0xF5, (byte) 0xC3});
        floats.put((float) 9999.99, new byte[] {0x4, 0x8, 0x46, 0x1C, 0x3F,
            (byte) 0xF6});
        floats.put((float) -1.0, new byte[] {0x4, 0x8, (byte) 0xBF,
            (byte) 0x80, 0x0, 0x0});
        floats.put((float) -1.1, new byte[] {0x4, 0x8, (byte) 0xBF,
            (byte) 0x8C, (byte) 0xCC, (byte) 0xCD});
        floats.put((float) -3.14, new byte[] {0x4, 0x8, (byte) 0xC0, 0x48,
            (byte) 0xF5, (byte) 0xC3});
        floats.put((float) -9999.99, new byte[] {0x4, 0x8, (byte) 0xC6, 0x1C,
            0x3F, (byte) 0xF6});

        return floats;
    }

    private static Map<Boolean, byte[]> booleans() {
        Map<Boolean, byte[]> booleans = new HashMap<>();

        booleans.put(Boolean.FALSE, new byte[] {0x0, 0x7});
        booleans.put(Boolean.TRUE, new byte[] {0x1, 0x7});
        return booleans;
    }

    private static Map<Map<String, ?>, byte[]> maps() {
        Map<Map<String, ?>, byte[]> maps = new HashMap<>();

        Map<String, Object> empty = Map.of();
        maps.put(empty, new byte[] {(byte) 0xe0});

        Map<String, String> one = new HashMap<>();
        one.put("en", "Foo");
        maps.put(one, new byte[] {(byte) 0xe1, /* en */0x42, 0x65, 0x6e,
            /* Foo */0x43, 0x46, 0x6f, 0x6f});

        Map<String, String> two = new HashMap<>();
        two.put("en", "Foo");
        two.put("zh", "人");
        maps.put(two, new byte[] {(byte) 0xe2,
            /* en */
            0x42, 0x65, 0x6e,
            /* Foo */
            0x43, 0x46, 0x6f, 0x6f,
            /* zh */
            0x42, 0x7a, 0x68,
            /* 人 */
            0x43, (byte) 0xe4, (byte) 0xba, (byte) 0xba});

        Map<String, Map<String, String>> nested = new HashMap<>();
        nested.put("name", two);

        maps.put(nested, new byte[] {(byte) 0xe1, /* name */
            0x44, 0x6e, 0x61, 0x6d, 0x65, (byte) 0xe2, /* en */
            0x42, 0x65, 0x6e,
            /* Foo */
            0x43, 0x46, 0x6f, 0x6f,
            /* zh */
            0x42, 0x7a, 0x68,
            /* 人 */
            0x43, (byte) 0xe4, (byte) 0xba, (byte) 0xba});

        Map<String, List<Object>> guess = new HashMap<>();
        List<Object> languages = new ArrayList<>();
        languages.add("en");
        languages.add("zh");
        guess.put("languages", languages);
        maps.put(guess, new byte[] {(byte) 0xe1, /* languages */
            0x49, 0x6c, 0x61, 0x6e, 0x67, 0x75, 0x61, 0x67, 0x65, 0x73,
            /* array */
            0x2, 0x4,
            /* en */
            0x42, 0x65, 0x6e,
            /* zh */
            0x42, 0x7a, 0x68});

        return maps;
    }

    private static Map<List<String>, byte[]> arrays() {
        Map<List<String>, byte[]> arrays = new HashMap<>();

        ArrayList<String> f1 = new ArrayList<>();
        f1.add("Foo");
        arrays.put(f1, new byte[] {0x1, 0x4,
            /* Foo */
            0x43, 0x46, 0x6f, 0x6f});

        ArrayList<String> f2 = new ArrayList<>();
        f2.add("Foo");
        f2.add("人");
        arrays.put(f2, new byte[] {0x2, 0x4,
            /* Foo */
            0x43, 0x46, 0x6f, 0x6f,
            /* 人 */
            0x43, (byte) 0xe4, (byte) 0xba, (byte) 0xba});

        ArrayList<String> empty = new ArrayList<>();
        arrays.put(empty, new byte[] {0x0, 0x4});

        return arrays;
    }

    @Test
    public void testUint16() throws IOException {
        DecoderTest.testTypeDecoding(Type.UINT16, uint16());
    }

    @Test
    public void testUint32() throws IOException {
        DecoderTest.testTypeDecoding(Type.UINT32, uint32());
    }

    @Test
    public void testInt32() throws IOException {
        DecoderTest.testTypeDecoding(Type.INT32, int32());
    }

    @Test
    public void testUint64() throws IOException {
        DecoderTest.testTypeDecoding(Type.UINT64, largeUint(64));
    }

    @Test
    public void testUint128() throws IOException {
        DecoderTest.testTypeDecoding(Type.UINT128, largeUint(128));
    }

    @Test
    public void testDoubles() throws IOException {
        DecoderTest
            .testTypeDecoding(Type.DOUBLE, DecoderTest.doubles());
    }

    @Test
    public void testFloats() throws IOException {
        DecoderTest.testTypeDecoding(Type.FLOAT, DecoderTest.floats());
    }

    @Test
    public void testPointers() throws IOException {
        DecoderTest.testTypeDecoding(Type.POINTER, pointers());
    }

    @Test
    public void testStrings() throws IOException {
        DecoderTest.testTypeDecoding(Type.UTF8_STRING,
            DecoderTest.strings());
    }

    @Test
    public void testBooleans() throws IOException {
        DecoderTest.testTypeDecoding(Type.BOOLEAN,
            DecoderTest.booleans());
    }

    @Test
    public void testBytes() throws IOException {
        DecoderTest.testTypeDecoding(Type.BYTES, DecoderTest.bytes());
    }

    @Test
    public void testMaps() throws IOException {
        DecoderTest.testTypeDecoding(Type.MAP, DecoderTest.maps());
    }

    @Test
    public void testArrays() throws IOException {
        DecoderTest.testTypeDecoding(Type.ARRAY, DecoderTest.arrays());
    }

    @Test
    public void testInvalidControlByte() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {0x0, 0xF});

        Decoder decoder = new Decoder(new CHMCache(), buffer, 0);
        InvalidDatabaseException ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, String.class));
        assertThat(ex.getMessage(),
                containsString("The MaxMind DB file's data section contains bad data"));
    }

    private static <T> void testTypeDecoding(Type type, Map<T, byte[]> tests)
            throws IOException {
        NodeCache cache = new CHMCache();

        for (Map.Entry<T, byte[]> entry : tests.entrySet()) {
            T expect = entry.getKey();
            byte[] input = entry.getValue();

            String desc = "decoded " + type.name() + " - " + expect;
            ByteBuffer buffer = ByteBuffer.wrap(input);

            Decoder decoder = new TestDecoder(cache, buffer, 0);

            switch (type) {
                case BYTES:
                    assertArrayEquals((byte[]) expect, decoder.decode(0, byte[].class), desc);
                    break;
                case ARRAY:
                    assertEquals(expect, decoder.decode(0, List.class), desc);
                    break;
                case UINT16:
                case INT32:
                    assertEquals(expect, decoder.decode(0, Integer.class), desc);
                    break;
                case UINT32:
                case POINTER:
                    assertEquals(expect, decoder.decode(0, Long.class), desc);
                    break;
                case UINT64:
                case UINT128:
                    assertEquals(expect, decoder.decode(0, BigInteger.class), desc);
                    break;
                case DOUBLE:
                    assertEquals(expect, decoder.decode(0, Double.class), desc);
                    break;
                case FLOAT:
                    assertEquals(expect, decoder.decode(0, Float.class), desc);
                    break;
                case UTF8_STRING:
                    assertEquals(expect, decoder.decode(0, String.class), desc);
                    break;
                case BOOLEAN:
                    assertEquals(expect, decoder.decode(0, Boolean.class), desc);
                    break;
                default: {
                    // We hit this for Type.MAP.

                    Map<?, ?> got = decoder.decode(0, Map.class);
                    Map<?, ?> expectMap = (Map<?, ?>) expect;

                    assertEquals(expectMap.size(), got.size(), desc);

                    for (Object keyObject : expectMap.keySet()) {
                        String key = (String) keyObject;
                        Object value = expectMap.get(key);

                        if (value instanceof Object[]) {
                            assertArrayEquals((Object[]) value, (Object[]) got.get(key), desc);
                        } else {
                            assertEquals(value, got.get(key), desc);
                        }
                    }
                }
            }
        }
    }

}
