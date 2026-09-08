package com.maxmind.db;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"boxing", "static-method"})
public class DecoderTest {

    private static final int TEST_MAX_DEPTH = 128;

    @Test
    public void testDecodedValueStoresMaximumCosts() {
        var value = new DecodedValue(null, Decoder.MAX_VALUES, Decoder.MAX_PAYLOAD_BYTES, Decoder.MAX_DEPTH);
        assertEquals(Decoder.MAX_VALUES, DecodedValue.values(value.costs()));
        assertEquals(Decoder.MAX_PAYLOAD_BYTES, DecodedValue.payloadBytes(value.costs()));
        assertEquals(Decoder.MAX_DEPTH, DecodedValue.depth(value.costs()));
    }

    @Test
    public void testDecodedValueCostsAreIndependent() {
        var costs = new long[][] {
            {0, 0, 0},
            {Decoder.MAX_VALUES, 0, 0},
            {0, Decoder.MAX_PAYLOAD_BYTES, 0},
            {0, 0, Decoder.MAX_DEPTH},
            {123, 456, 7},
        };
        for (var expected : costs) {
            var value = new DecodedValue(null, (int) expected[0], expected[1], (int) expected[2]);
            assertEquals(expected[0], DecodedValue.values(value.costs()), "value cost");
            assertEquals(expected[1], DecodedValue.payloadBytes(value.costs()), "payload cost");
            assertEquals(expected[2], DecodedValue.depth(value.costs()), "depth cost");
        }
    }

    private static Map<Integer, byte[]> int32() {
        int max = (2 << 30) - 1;
        var int32 = new HashMap<Integer, byte[]>();

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
        var uint32s = new HashMap<Long, byte[]>();

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

        var uint16s = new HashMap<Integer, byte[]>();

        uint16s.put(0, new byte[] {(byte) 0xa0});
        uint16s.put((1 << 8) - 1, new byte[] {(byte) 0xa1, (byte) 0xff});
        uint16s.put(500, new byte[] {(byte) 0xa2, 0x1, (byte) 0xf4});
        uint16s.put(10872, new byte[] {(byte) 0xa2, 0x2a, 0x78});
        uint16s.put(max, new byte[] {(byte) 0xa2, (byte) 0xff, (byte) 0xff});
        return uint16s;
    }

    private static Map<BigInteger, byte[]> largeUint(int bits) {
        var uints = new HashMap<BigInteger, byte[]>();

        byte ctrlByte = (byte) (bits == 64 ? 0x2 : 0x3);

        uints.put(BigInteger.valueOf(0), new byte[] {0x0, ctrlByte});
        uints.put(BigInteger.valueOf(500), new byte[] {0x2, ctrlByte, 0x1,
            (byte) 0xf4});
        uints.put(BigInteger.valueOf(10872), new byte[] {0x2, ctrlByte, 0x2a,
            0x78});

        for (int power = 1; power <= bits / 8; power++) {

            var key = BigInteger.valueOf(2).pow(8 * power)
                .subtract(BigInteger.valueOf(1));

            var value = new byte[2 + power];
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
        var pointers = new HashMap<Long, byte[]>();

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
        var strings = new HashMap<String, byte[]>();

        DecoderTest.addTestString(strings, (byte) 0x40, "");
        DecoderTest.addTestString(strings, (byte) 0x41, "1");
        DecoderTest.addTestString(strings, (byte) 0x43, "人");
        DecoderTest.addTestString(strings, (byte) 0x43, "\uFFFD");
        DecoderTest.addTestString(strings, (byte) 0x45, "a\uFFFDz");
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
        var bytes = new HashMap<byte[], byte[]>();

        var strings = DecoderTest.strings();

        for (String s : strings.keySet()) {
            var ba = strings.get(s);
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

        var sb = str.getBytes(StandardCharsets.UTF_8);
        var bytes = new byte[ctrl.length + sb.length];

        System.arraycopy(ctrl, 0, bytes, 0, ctrl.length);
        System.arraycopy(sb, 0, bytes, ctrl.length, sb.length);
        tests.put(str, bytes);
    }

    private static Map<Double, byte[]> doubles() {
        var doubles = new HashMap<Double, byte[]>();
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
        var floats = new HashMap<Float, byte[]>();
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
        var booleans = new HashMap<Boolean, byte[]>();

        booleans.put(Boolean.FALSE, new byte[] {0x0, 0x7});
        booleans.put(Boolean.TRUE, new byte[] {0x1, 0x7});
        return booleans;
    }

    private static Map<Map<String, ?>, byte[]> maps() {
        var maps = new HashMap<Map<String, ?>, byte[]>();

        var empty = Map.<String, Object>of();
        maps.put(empty, new byte[] {(byte) 0xe0});

        var one = new HashMap<String, String>();
        one.put("en", "Foo");
        maps.put(one, new byte[] {(byte) 0xe1, /* en */0x42, 0x65, 0x6e,
            /* Foo */0x43, 0x46, 0x6f, 0x6f});

        var two = new HashMap<String, String>();
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

        var nested = new HashMap<String, Map<String, String>>();
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

        var guess = new HashMap<String, List<Object>>();
        var languages = new ArrayList<Object>();
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
        var arrays = new HashMap<List<String>, byte[]>();

        var f1 = new ArrayList<String>();
        f1.add("Foo");
        arrays.put(f1, new byte[] {0x1, 0x4,
            /* Foo */
            0x43, 0x46, 0x6f, 0x6f});

        var f2 = new ArrayList<String>();
        f2.add("Foo");
        f2.add("人");
        arrays.put(f2, new byte[] {0x2, 0x4,
            /* Foo */
            0x43, 0x46, 0x6f, 0x6f,
            /* 人 */
            0x43, (byte) 0xe4, (byte) 0xba, (byte) 0xba});

        var empty = new ArrayList<String>();
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
    public void testOversizedIntegersAreRejectedBeforePayloadRead() {
        var invalidIntegers = Map.of(
            "uint16", new byte[] {(byte) 0xA3},
            "uint32", new byte[] {(byte) 0xC5},
            "int32", new byte[] {0x05, 0x01},
            "uint64", new byte[] {0x09, 0x02},
            "uint128", new byte[] {0x11, 0x03}
        );

        for (var invalidInteger : invalidIntegers.entrySet()) {
            var decoder = new Decoder(
                NoCache.getInstance(),
                SingleBuffer.wrap(invalidInteger.getValue()),
                0
            );
            var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, Object.class)
            );
            assertThat(ex.getMessage(), containsString(
                "invalid size of " + invalidInteger.getKey()));
        }
    }

    @Test
    public void testTruncatedIntegersAreRejectedAsInvalidDatabase() {
        var headers = List.of(
            new byte[] {(byte) 0xA1},
            new byte[] {(byte) 0xC1},
            new byte[] {0x01, 0x01},
            new byte[] {0x01, 0x02},
            new byte[] {0x01, 0x03}
        );

        for (var header : headers) {
            var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(header), 0);
            var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, Object.class)
            );
            assertThat(ex.getMessage(), containsString("extends beyond the end"));
        }
    }

    @Test
    public void testPointerBackedOversizedIntegerIsRejectedBeforePayloadRead() {
        // A uint32 control byte can declare a 16,843,036-byte payload. The
        // pointer target must be rejected before the decoder enters that loop.
        var data = new byte[] {
            (byte) 0xDF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
            0x20, 0x00
        };
        for (var cache : List.<NodeCache>of(NoCache.getInstance(), new CHMCache())) {
            var decoder = new Decoder(cache, SingleBuffer.wrap(data), 0);
            var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(4, Object.class)
            );
            assertThat(ex.getMessage(), containsString("invalid size of uint32"));
        }
    }

    @Test
    public void testSkippedOversizedIntegersPreserveKnownFields() throws IOException {
        var invalidIntegers = Map.of(
            "uint16", new byte[] {(byte) 0xA3, 0, 0, 0},
            "uint32", new byte[] {(byte) 0xC5, 0, 0, 0, 0, 0},
            "int32", new byte[] {0x05, 0x01, 0, 0, 0, 0, 0},
            "uint64", new byte[] {0x09, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0},
            "uint128", new byte[] {
                0x11, 0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            }
        );

        for (var invalidInteger : invalidIntegers.entrySet()) {
            var out = new ByteArrayOutputStream();
            out.write(0xE2); // map with two key/value pairs
            out.write(0x47); // seven-byte UTF-8 string
            out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
            out.writeBytes(invalidInteger.getValue());
            out.write(0x45); // five-byte UTF-8 string
            out.writeBytes("known".getBytes(StandardCharsets.UTF_8));
            out.write(0x42); // two-byte UTF-8 string
            out.writeBytes("ok".getBytes(StandardCharsets.UTF_8));

            var decoder = new Decoder(
                NoCache.getInstance(),
                SingleBuffer.wrap(out.toByteArray()),
                0
            );
            var result = decoder.decode(0, KnownFieldModel.class);
            assertEquals("ok", result.known(), invalidInteger.getKey());
        }
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
    public void testUtf8PointerAcrossChunks() throws IOException {
        var expected = "a€𐍈\uFFFDz";
        var payload = expected.getBytes(StandardCharsets.UTF_8);
        for (int chunkSize : new int[] {1, 2, 3, 4, 5, 64}) {
            for (var cache : List.<NodeCache>of(NoCache.getInstance(), new CHMCache(), new CHMCache(0))) {
                var decoder = stringPointerDecoder(payload, chunkSize, cache);
                assertEquals(expected, decoder.decode(0, String.class));
                assertEquals(expected, decoder.decode(0, String.class));
                assertEquals("a", decoder.decode(payload.length + 3, String.class));
            }
        }
    }

    @Test
    public void testMalformedUtf8IsRejectedAcrossChunks() throws IOException {
        var payloads = List.of(
            new byte[] {(byte) 0x80},
            new byte[] {(byte) 0xC0, (byte) 0xAF},
            new byte[] {(byte) 0xC2},
            new byte[] {(byte) 0xE2, (byte) 0x82},
            new byte[] {(byte) 0xED, (byte) 0xA0, (byte) 0x80},
            new byte[] {(byte) 0xF0, (byte) 0x9F, (byte) 0x92},
            new byte[] {(byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80},
            new byte[] {(byte) 0xFF},
            new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xBD, (byte) 0xFF}
        );
        for (var payload : payloads) {
            for (int chunkSize : new int[] {1, 2, 3, 4, 5, 64}) {
                for (var cache : List.<NodeCache>of(NoCache.getInstance(), new CHMCache(), new CHMCache(0))) {
                    var decoder = stringPointerDecoder(payload, chunkSize, cache);
                    var error = assertThrows(
                        InvalidDatabaseException.class,
                        () -> decoder.decode(0, String.class)
                    );
                    assertInstanceOf(CharacterCodingException.class, error.getCause());
                    assertEquals("a", decoder.decode(payload.length + 3, String.class));
                    assertThrows(InvalidDatabaseException.class, () -> decoder.decode(0, String.class));
                }
            }
        }
    }

    private static Decoder stringPointerDecoder(byte[] payload, int chunkSize, NodeCache cache) {
        var data = new byte[payload.length + 5];
        data[0] = 0x20;
        data[1] = 2;
        data[2] = (byte) (0x40 | payload.length);
        System.arraycopy(payload, 0, data, 3, payload.length);
        data[data.length - 2] = 0x41;
        data[data.length - 1] = 'a';
        if (chunkSize >= data.length) {
            return new Decoder(cache, SingleBuffer.wrap(data), 0);
        }
        var chunks = new ByteBuffer[(data.length + chunkSize - 1) / chunkSize];
        for (int i = 0; i < chunks.length; i++) {
            int offset = i * chunkSize;
            int size = Math.min(chunkSize, data.length - offset);
            chunks[i] = ByteBuffer.wrap(data, offset, size).slice();
        }
        return new Decoder(cache, new MultiBuffer(chunks, chunkSize), 0);
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
        var buffer = SingleBuffer.wrap(new byte[] {0x0, 0xF});

        var decoder = new Decoder(new CHMCache(), buffer, 0);
        var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, String.class));
        assertThat(ex.getMessage(),
                containsString("The MaxMind DB file's data section contains bad data"));
    }

    private static void writePointer(ByteArrayOutputStream out, int target) {
        if (target < 1 << 11) {
            // One-byte-payload pointer (type 1, pointer_size 1) with base 0.
            out.write((1 << 5) | ((target >> 8) & 0x7));
            out.write(target & 0xFF);
            return;
        }

        var packed = target - (1 << 11);
        out.write((1 << 5) | (1 << 3) | ((packed >> 16) & 0x7));
        out.write((packed >> 8) & 0xFF);
        out.write(packed & 0xFF);
    }

    // Array header for 29 or more elements.
    private static void writeArrayHeader(ByteArrayOutputStream out, int size) {
        if (size < 285) {
            out.write(29);
            out.write(0x04);
            out.write(size - 29);
            return;
        }

        var encoded = size - 285;
        out.write(30);
        out.write(0x04);
        out.write((encoded >> 8) & 0xFF);
        out.write(encoded & 0xFF);
    }

    // Map header for 285 or more key/value pairs.
    private static void writeMapHeader(ByteArrayOutputStream out, int size) {
        out.write(0xFE);
        var encoded = size - 285;
        out.write((encoded >>> 8) & 0xFF);
        out.write(encoded & 0xFF);
    }

    private static byte[] unknownFieldWithFlatArray(int size) {
        var out = new ByteArrayOutputStream();
        out.write(0xE1); // map with one key/value pair
        out.write(0x47); // seven-byte UTF-8 string
        out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
        writeArrayHeader(out, size);
        for (var i = 0; i < size; i++) {
            out.write(0xA0); // uint16 with value 0
        }
        return out.toByteArray();
    }

    private static byte[] unknownFieldWithFlatMap(int size) {
        var out = new ByteArrayOutputStream();
        out.write(0xE1); // map with one key/value pair
        out.write(0x47); // seven-byte UTF-8 string
        out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
        writeMapHeader(out, size);
        for (var i = 0; i < size; i++) {
            out.write(0x40); // empty UTF-8 string key
            out.write(0xA0); // uint16 with value 0
        }
        return out.toByteArray();
    }

    private static byte[] nestedArrays(int depth) {
        var out = new ByteArrayOutputStream();
        for (var i = 0; i < depth; i++) {
            out.write(0x01); // extended type, one element
            out.write(0x04); // array
        }
        out.write(0xA0); // uint16 with value 0
        return out.toByteArray();
    }

    private static byte[] nestedMaps(int depth) {
        var out = new ByteArrayOutputStream();
        for (var i = 0; i < depth; i++) {
            out.write(0xE1); // map with one key/value pair
            out.write(0x40); // empty UTF-8 string key
        }
        out.write(0xA0); // uint16 with value 0
        return out.toByteArray();
    }

    private record EncodedValue(byte[] data, int offset) {
    }

    private static EncodedValue pointerNestedArrays(int depth) {
        var out = new ByteArrayOutputStream();
        out.write(0xA0); // uint16 with value 0
        var previous = 0;
        for (var i = 0; i < depth; i++) {
            var offset = out.size();
            out.write(0x01); // extended type, one element
            out.write(0x04); // array
            writePointer(out, previous);
            previous = offset;
        }
        return new EncodedValue(out.toByteArray(), previous);
    }

    private static EncodedValue pointerNestedMaps(int depth) {
        var out = new ByteArrayOutputStream();
        out.write(0xA0); // uint16 with value 0
        var previous = 0;
        for (var i = 0; i < depth; i++) {
            var offset = out.size();
            out.write(0xE1); // map with one key/value pair
            out.write(0x40); // empty UTF-8 string key
            writePointer(out, previous);
            previous = offset;
        }
        return new EncodedValue(out.toByteArray(), previous);
    }

    private static byte[] inlineArray(int size) {
        var out = new ByteArrayOutputStream();
        writeArrayHeader(out, size);
        for (var i = 0; i < size; i++) {
            out.write(0xA0); // uint16 with value 0
        }
        return out.toByteArray();
    }

    @Test
    public void testPointerFreeContainerDepthIsBounded() throws IOException {
        var atLimit = new Decoder(NoCache.getInstance(),
                SingleBuffer.wrap(nestedArrays(TEST_MAX_DEPTH)), 0);
        atLimit.decode(0, Object.class);

        var overLimit = new Decoder(NoCache.getInstance(),
                SingleBuffer.wrap(nestedArrays(TEST_MAX_DEPTH + 1)), 0);
        var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> overLimit.decode(0, Object.class));
        assertThat(ex.getMessage(), containsString("exceeds the maximum depth"));
    }

    @Test
    public void testPointerBackedContainerDepthIsBounded() throws IOException {
        var atLimit = pointerNestedArrays(TEST_MAX_DEPTH);
        var decoderAtLimit = new Decoder(NoCache.getInstance(),
            SingleBuffer.wrap(atLimit.data()), 0);
        decoderAtLimit.decode(atLimit.offset(), Object.class);

        var overLimit = pointerNestedArrays(TEST_MAX_DEPTH + 1);
        var decoderOverLimit = new Decoder(NoCache.getInstance(),
            SingleBuffer.wrap(overLimit.data()), 0);
        var ex = assertThrows(
            InvalidDatabaseException.class,
            () -> decoderOverLimit.decode(overLimit.offset(), Object.class));
        assertThat(ex.getMessage(), containsString("exceeds the maximum depth"));
    }

    @Test
    public void testCachedPointerTargetDepthIsBounded() throws IOException {
        var nested = pointerNestedArrays(TEST_MAX_DEPTH);
        var out = new ByteArrayOutputStream();
        out.writeBytes(nested.data());

        var seedPointerOffset = out.size();
        writePointer(out, nested.offset());

        var outerArrayOffset = out.size();
        out.write(0x01); // extended type, one element
        out.write(0x04); // array
        writePointer(out, nested.offset());

        var decoder = new Decoder(new CHMCache(), SingleBuffer.wrap(out.toByteArray()), 0);
        decoder.decode(seedPointerOffset, Object.class);

        var ex = assertThrows(
            InvalidDatabaseException.class,
            () -> decoder.decode(outerArrayOffset, Object.class)
        );
        assertThat(ex.getMessage(), containsString("exceeds the maximum depth"));
    }

    @Test
    public void testContainerDepthFitsReducedThreadStack() throws Exception {
        runProbe("-Xss512k", StackProbe.class);
    }

    static void runProbe(String vmArgument, Class<?> probe) throws Exception {
        var executable = System.getProperty("os.name").startsWith("Windows")
            ? "java.exe"
            : "java";
        var java = Path.of(System.getProperty("java.home"), "bin", executable).toString();
        var classPath = System.getProperty(
            "surefire.test.class.path",
            System.getProperty("java.class.path")
        );
        var modulePath = System.getProperty("jdk.module.path");
        if (modulePath != null && !modulePath.isBlank()) {
            classPath = String.join(System.getProperty("path.separator"), classPath, modulePath);
        }
        var process = new ProcessBuilder(
            java,
            vmArgument,
            "-cp",
            classPath,
            probe.getName()
        ).redirectErrorStream(true).start();

        if (!process.waitFor(15, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError(probe.getSimpleName() + " did not finish within 15 seconds");
        }
        var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.exitValue(), output);
    }

    @Test
    public void testJavaValueCountBoundary() throws IOException {
        var atLimit = new Decoder(NoCache.getInstance(),
            SingleBuffer.wrap(inlineArray(65_535)), 0);
        var result = (List<?>) atLimit.decode(0, Object.class);
        assertEquals(65_535, result.size());

        var overLimit = new Decoder(NoCache.getInstance(),
            SingleBuffer.wrap(inlineArray(65_536)), 0);
        var ex = assertThrows(
            InvalidDatabaseException.class,
            () -> overLimit.decode(0, Object.class));
        assertThat(ex.getMessage(), containsString("exceeds the maximum number of values"));
    }

    @Test
    public void testPointerValueCountBoundaryIsIndependentOfCacheState() throws IOException {
        // A warm cache replays a recorded cost instead of decoding the target
        // again. The verdict at the boundary must not depend on which of those
        // paths ran, so decode each fixture twice against the same cache.
        for (var cache : List.<NodeCache>of(
                NoCache.getInstance(), new CHMCache(), new CHMCache(0))) {
            var atLimit = new Decoder(cache, SingleBuffer.wrap(pointerFanOut(1)), 0);
            for (var attempt = 0; attempt < 2; attempt++) {
                var result = (List<?>) atLimit.decode(1, Object.class);
                assertEquals(32_768, result.size());
            }
        }

        for (var cache : List.<NodeCache>of(
                NoCache.getInstance(), new CHMCache(), new CHMCache(0))) {
            var overLimit = new Decoder(cache, SingleBuffer.wrap(pointerFanOut(2)), 0);
            for (var attempt = 0; attempt < 2; attempt++) {
                var ex = assertThrows(
                    InvalidDatabaseException.class,
                    () -> overLimit.decode(1, Object.class));
                assertThat(ex.getMessage(),
                    containsString("exceeds the maximum number of values"));
            }
        }
    }

    // Builds an array of 32,767 pointers to one uint16, followed by the given
    // number of inline uint16 values. Each pointer occurrence costs one value
    // for the pointer and one for its target, so the decode costs
    // 65,535 + scalars values. One scalar lands on the 65,536 limit and two
    // exceed it by one.
    private static byte[] pointerFanOut(int scalars) {
        var pointers = 32_767;
        var elements = pointers + scalars;
        var out = new ByteArrayOutputStream();
        out.write(0xA0); // uint16 with value 0, the shared pointer target
        out.write(0x1E); // extended type, size code 30
        out.write(0x04); // array
        out.write((elements - 285) >> 8);
        out.write(elements - 285);
        for (var i = 0; i < pointers; i++) {
            writePointer(out, 0);
        }
        for (var i = 0; i < scalars; i++) {
            out.write(0xA0);
        }
        return out.toByteArray();
    }

    @Test
    public void testUnknownFieldValueCountIsBounded() {
        var out = new ByteArrayOutputStream();
        out.write(0xE1); // map with one key/value pair
        out.write(0x47); // seven-byte UTF-8 string
        out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
        out.write(0x1E); // extended type, two-byte size
        out.write(0x04); // array
        out.write(0xFE); // size = 65,535
        out.write(0xE2);
        for (var i = 0; i < 65_535; i++) {
            out.write(0xA0); // uint16 with value 0
        }

        var decoder = new Decoder(NoCache.getInstance(),
                SingleBuffer.wrap(out.toByteArray()), 0);
        var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, EmptyModel.class));
        assertThat(ex.getMessage(), containsString("exceeds the maximum number of values"));
    }

    @Test
    public void testUnknownFieldDepthIsBounded() throws IOException {
        for (var depth : new int[] {TEST_MAX_DEPTH - 1, TEST_MAX_DEPTH}) {
            var out = new ByteArrayOutputStream();
            out.write(0xE1); // map with one key/value pair
            out.write(0x47);
            out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
            out.writeBytes(nestedArrays(depth));
            var decoder = new Decoder(NoCache.getInstance(),
                SingleBuffer.wrap(out.toByteArray()), 0);
            if (depth == TEST_MAX_DEPTH - 1) {
                assertInstanceOf(EmptyModel.class, decoder.decode(0, EmptyModel.class));
            } else {
                var ex = assertThrows(InvalidDatabaseException.class,
                    () -> decoder.decode(0, EmptyModel.class));
                assertThat(ex.getMessage(), containsString("exceeds the maximum depth"));
            }
        }
    }

    @Test
    public void testUnknownFieldPointersAreSkippedByTheirEncodedWidth() throws IOException {
        var pointerEncodings = new ArrayList<byte[]>();
        pointerEncodings.add(new byte[] {0x20, 0x00});
        pointerEncodings.add(new byte[] {0x28, 0x00, 0x00});
        pointerEncodings.add(new byte[] {0x30, 0x00, 0x00, 0x00});
        for (var controlByte = 0x38; controlByte <= 0x3F; controlByte++) {
            pointerEncodings.add(new byte[] {
                (byte) controlByte, 0x00, 0x00, 0x00, 0x00
            });
        }

        for (var pointer : pointerEncodings) {
            var out = new ByteArrayOutputStream();
            out.write(0xE2); // map with two key/value pairs
            out.write(0x47); // seven-byte UTF-8 string
            out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
            out.writeBytes(pointer);
            out.write(0x45); // five-byte UTF-8 string
            out.writeBytes("known".getBytes(StandardCharsets.UTF_8));
            out.write(0x42); // two-byte UTF-8 string
            out.writeBytes("ok".getBytes(StandardCharsets.UTF_8));

            var decoder = new Decoder(
                NoCache.getInstance(),
                SingleBuffer.wrap(out.toByteArray()),
                0
            );
            var result = decoder.decode(0, KnownFieldModel.class);
            assertEquals("ok", result.known());
        }
    }

    @Test
    public void testTruncatedUnknownPointersAreRejectedAsInvalidDatabase() {
        for (var pointerSize = 1; pointerSize <= 4; pointerSize++) {
            var out = new ByteArrayOutputStream();
            out.write(0xE1); // map with one key/value pair
            out.write(0x47); // seven-byte UTF-8 string
            out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
            out.write(0x20 | ((pointerSize - 1) << 3));
            out.writeBytes(new byte[pointerSize - 1]);

            var decoder = new Decoder(
                NoCache.getInstance(),
                SingleBuffer.wrap(out.toByteArray()),
                0
            );
            var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, EmptyModel.class)
            );
            assertThat(ex.getMessage(), containsString("extends beyond the end"));
        }
    }

    @Test
    public void testTruncatedUnknownScalarIsRejectedAsInvalidDatabase() {
        var out = new ByteArrayOutputStream();
        out.write(0xE1); // map with one key/value pair
        out.write(0x47); // seven-byte UTF-8 string
        out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
        out.write(0x41); // one-byte UTF-8 string with no payload

        var decoder = new Decoder(
            NoCache.getInstance(),
            SingleBuffer.wrap(out.toByteArray()),
            0
        );
        var ex = assertThrows(
            InvalidDatabaseException.class,
            () -> decoder.decode(0, EmptyModel.class)
        );
        assertThat(ex.getMessage(), containsString("extends beyond the end"));
    }

    @Test
    public void testTruncatedHeaderIsRejectedAsInvalidDatabase() {
        var cases = new LinkedHashMap<String, byte[]>();
        // The outer array declares two elements, but the first element and its
        // own child consume the rest of the buffer, so no control byte remains.
        cases.put("control byte", new byte[] {0x02, 0x04, 0x01, 0x04, (byte) 0xA0});
        cases.put("extended type byte", new byte[] {0x00});
        cases.put("size code 29", new byte[] {0x5D});
        cases.put("size code 30", new byte[] {0x5E, 0x00});
        cases.put("size code 31", new byte[] {0x5F, 0x00, 0x00});
        cases.put("pointer", new byte[] {0x20});
        cases.put("double", new byte[] {0x68});
        cases.put("float", new byte[] {0x04, 0x08});

        for (var entry : cases.entrySet()) {
            for (var buffer : truncationBuffers(entry.getValue())) {
                var decoder = new Decoder(NoCache.getInstance(), buffer, 0);
                var ex = assertThrows(
                    InvalidDatabaseException.class,
                    () -> decoder.decode(0, Object.class),
                    entry.getKey()
                );
                assertThat(entry.getKey(), ex.getMessage(),
                    containsString("extends beyond the end"));
            }
        }
    }

    @Test
    public void testTruncatedUnknownFieldHeaderIsRejectedAsInvalidDatabase() {
        var cases = new LinkedHashMap<String, byte[]>();
        cases.put("extended type byte", new byte[] {0x00});
        cases.put("size code 29", new byte[] {0x5D});
        cases.put("size code 30", new byte[] {0x5E, 0x00});
        cases.put("size code 31", new byte[] {0x5F, 0x00, 0x00});

        for (var entry : cases.entrySet()) {
            var out = new ByteArrayOutputStream();
            out.write(0xE1); // map with one key/value pair
            out.write(0x47); // seven-byte UTF-8 string
            out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
            out.writeBytes(entry.getValue());

            for (var buffer : truncationBuffers(out.toByteArray())) {
                var decoder = new Decoder(NoCache.getInstance(), buffer, 0);
                var ex = assertThrows(
                    InvalidDatabaseException.class,
                    () -> decoder.decode(0, EmptyModel.class),
                    entry.getKey()
                );
                assertThat(entry.getKey(), ex.getMessage(),
                    containsString("extends beyond the end"));
            }
        }
    }

    // Both buffer implementations must report a truncated read as an
    // InvalidDatabaseException rather than a BufferUnderflowException or an
    // IndexOutOfBoundsException.
    private static List<Buffer> truncationBuffers(byte[] data) {
        var chunks = new ByteBuffer[data.length];
        for (var i = 0; i < data.length; i++) {
            chunks[i] = ByteBuffer.wrap(data, i, 1).slice();
        }
        return List.of(SingleBuffer.wrap(data), new MultiBuffer(chunks, 1));
    }

    @Test
    public void testHugeContainerIsRejectedBeforeAllocation() throws IOException {
        // An array control byte can declare up to ~16.8 million entries from a
        // few bytes. The value limit must reject this before the decoder uses
        // the declared size as an allocation hint.
        var out = new ByteArrayOutputStream();
        out.write(0x1F); // extended type, size code 31 (three size bytes)
        out.write(0x04); // array
        out.write(0xFF); // size = 65821 + 0xFFFFFF = 16,843,036
        out.write(0xFF);
        out.write(0xFF);

        var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(out.toByteArray()), 0);
        var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, Object.class));
        assertThat(ex.getMessage(), containsString("exceeds the maximum number of values"));
    }

    @Test
    public void testArrayInitialCapacityIsBounded() throws IOException {
        var decoder = new Decoder(NoCache.getInstance(),
            SingleBuffer.wrap(inlineArray(129)), 0);
        var result = decoder.decode(0, CapacityList.class);
        assertEquals(128, result.initialCapacity);
        assertEquals(129, result.size());
    }

    @Test
    public void testMapInitialCapacityIsBounded() throws IOException {
        var out = new ByteArrayOutputStream();
        out.write(0xFD); // map, size code 29
        out.write(100); // 29 + 100 = 129 entries
        for (var i = 0; i < 129; i++) {
            var key = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
            out.write(0x40 | key.length);
            out.writeBytes(key);
            out.write(0xA0); // uint16 with value 0
        }

        var decoder = new Decoder(NoCache.getInstance(),
            SingleBuffer.wrap(out.toByteArray()), 0);
        var result = decoder.decode(0, CapacityMap.class);
        assertEquals(128, result.initialCapacity);
        assertEquals(129, result.size());
    }

    @Test
    public void testNestedLargeCollectionsDoNotExhaustHeap() throws Exception {
        runProbe("-Xmx16m", AllocationProbe.class);
    }

    @Test
    public void testImpossibleArrayIsRejectedBeforeAllocation() {
        // The declared size is below the value budget, but two elements cannot
        // be encoded in the one remaining byte.
        var decoder = new Decoder(NoCache.getInstance(),
                SingleBuffer.wrap(new byte[] {0x02, 0x04, (byte) 0xA0}), 0);
        var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, Object.class));
        assertThat(ex.getMessage(), containsString(
                "a container declares more entries than the database can hold"));
    }

    @Test
    public void testImpossibleMapIsRejectedBeforeAllocation() {
        // A one-entry map needs both a key and a value, but only one byte
        // remains after its control byte.
        var decoder = new Decoder(NoCache.getInstance(),
                SingleBuffer.wrap(new byte[] {(byte) 0xE1, (byte) 0xA0}), 0);
        var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, Object.class));
        assertThat(ex.getMessage(), containsString(
                "a container declares more entries than the database can hold"));
    }

    @Test
    public void testCyclicPointerThrows() {
        // A pointer to itself must throw a catchable InvalidDatabaseException
        // rather than recursing until the stack overflows.
        var decoder = new Decoder(NoCache.getInstance(),
                SingleBuffer.wrap(new byte[] {0x20, 0x00}), 0);
        assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, Object.class));
    }

    @Test
    public void testAcyclicPointerToPointerThrows() {
        // The pointer chain terminates at a scalar, but pointer-to-pointer is
        // illegal regardless of whether the chain forms a cycle.
        var data = new byte[] {0x20, 0x02, 0x20, 0x04, (byte) 0xA0};
        for (var cache : List.<NodeCache>of(NoCache.getInstance(), new CHMCache())) {
            var decoder = new Decoder(cache, SingleBuffer.wrap(data), 0);
            var ex = assertThrows(
                    InvalidDatabaseException.class,
                    () -> decoder.decode(0, Object.class));
            assertThat(ex.getMessage(), containsString("pointer to a pointer"));
        }
    }

    @Test
    public void testOverBudgetPayloadHeadersAreRejectedBeforePayloadRead() {
        var overBudgetHeaders = List.of(
            new byte[] {0x5F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
            new byte[] {(byte) 0x9F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}
        );
        for (var header : overBudgetHeaders) {
            var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(header), 0);
            var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, Object.class)
            );
            assertThat(ex.getMessage(), containsString("exceeds the maximum payload size"));
        }
    }

    @Test
    public void testTruncatedPayloadsAreRejectedAsInvalidDatabase() {
        var headers = List.of(
            new byte[] {0x41},
            new byte[] {(byte) 0x81}
        );
        for (var header : headers) {
            var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(header), 0);
            var ex = assertThrows(
                InvalidDatabaseException.class,
                () -> decoder.decode(0, Object.class)
            );
            assertThat(ex.getMessage(), containsString("extends beyond the end"));
        }
    }

    @Test
    public void testDecoderCanBeReusedAfterInvalidString() throws IOException {
        var data = new byte[] {0x41, (byte) 0xFF, 0x41, 'a'};
        var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(data), 0);

        var ex = assertThrows(
            InvalidDatabaseException.class,
            () -> decoder.decode(0, Object.class)
        );
        assertThat(ex.getMessage(), containsString("invalid UTF-8 string"));
        assertEquals("a", decoder.decode(2, Object.class));
    }

    @Test
    public void testSkippedPayloadDoesNotConsumeMaterializationBudget() throws IOException {
        var payloadSize = (1 << 21) + 1;
        var out = new ByteArrayOutputStream();
        out.write(0xE2); // map with two key/value pairs
        out.write(0x47); // seven-byte UTF-8 string
        out.writeBytes("unknown".getBytes(StandardCharsets.UTF_8));
        out.write(0x5F); // UTF-8 string, size code 31
        var encodedSize = payloadSize - 65_821;
        out.write((encodedSize >>> 16) & 0xFF);
        out.write((encodedSize >>> 8) & 0xFF);
        out.write(encodedSize & 0xFF);
        out.writeBytes(new byte[payloadSize]);
        out.write(0x45); // five-byte UTF-8 string
        out.writeBytes("known".getBytes(StandardCharsets.UTF_8));
        out.write(0x42); // two-byte UTF-8 string
        out.writeBytes("ok".getBytes(StandardCharsets.UTF_8));

        var decoder = new Decoder(
            NoCache.getInstance(),
            SingleBuffer.wrap(out.toByteArray()),
            0
        );
        var result = decoder.decode(0, KnownFieldModel.class);
        assertEquals("ok", result.known());
    }

    @Test
    public void testBigIntegerDoesNotConsumeStringAndBytesBudget() throws IOException {
        var payloadSize = 1 << 21;
        var out = new ByteArrayOutputStream();
        out.write(0x02); // extended type, two elements
        out.write(0x04); // array
        out.write(0x10); // 16-byte extended value
        out.write(0x03); // uint128
        out.writeBytes(new byte[16]);

        out.write(0x9F); // bytes, size code 31
        var encodedSize = payloadSize - 65_821;
        out.write((encodedSize >>> 16) & 0xFF);
        out.write((encodedSize >>> 8) & 0xFF);
        out.write(encodedSize & 0xFF);
        out.writeBytes(new byte[payloadSize]);

        var decoder = new Decoder(
            NoCache.getInstance(),
            SingleBuffer.wrap(out.toByteArray()),
            0
        );
        var result = (List<?>) decoder.decode(0, Object.class);
        assertEquals(2, result.size());
    }

    public static final class StackProbe {
        private StackProbe() {
        }

        public static void main(String[] args) throws IOException {
            decode(nestedArrays(TEST_MAX_DEPTH), 0);
            decode(nestedMaps(TEST_MAX_DEPTH), 0);

            var pointerArray = pointerNestedArrays(TEST_MAX_DEPTH);
            decode(pointerArray.data(), pointerArray.offset());
            for (var cache : caches()) {
                decode(pointerArray.data(), pointerArray.offset(), cache);
            }
            var pointerMap = pointerNestedMaps(TEST_MAX_DEPTH);
            decode(pointerMap.data(), pointerMap.offset());
            for (var cache : caches()) {
                decode(pointerMap.data(), pointerMap.offset(), cache);
            }

            expectDepthRejection(nestedArrays(TEST_MAX_DEPTH + 1), 0);
            expectDepthRejection(nestedMaps(TEST_MAX_DEPTH + 1), 0);

            pointerArray = pointerNestedArrays(TEST_MAX_DEPTH + 1);
            expectDepthRejection(pointerArray.data(), pointerArray.offset());
            for (var cache : caches()) {
                expectDepthRejection(pointerArray.data(), pointerArray.offset(), cache);
            }
            pointerMap = pointerNestedMaps(TEST_MAX_DEPTH + 1);
            expectDepthRejection(pointerMap.data(), pointerMap.offset());
            for (var cache : caches()) {
                expectDepthRejection(pointerMap.data(), pointerMap.offset(), cache);
            }

            decodeUnknown(unknownFieldWithFlatArray(65_532));
            decodeUnknown(unknownFieldWithFlatMap(32_766));
        }

        private static void decode(byte[] data, int offset) throws IOException {
            decode(data, offset, NoCache.getInstance());
        }

        private static void decode(byte[] data, int offset, NodeCache cache) throws IOException {
            var decoder = new Decoder(cache, SingleBuffer.wrap(data), 0);
            decoder.decode(offset, Object.class);
        }

        private static void expectDepthRejection(byte[] data, int offset) throws IOException {
            expectDepthRejection(data, offset, NoCache.getInstance());
        }

        private static void expectDepthRejection(
            byte[] data,
            int offset,
            NodeCache cache
        ) throws IOException {
            try {
                decode(data, offset, cache);
                throw new AssertionError("over-depth container decoded without rejection");
            } catch (InvalidDatabaseException e) {
                if (!e.getMessage().contains("exceeds the maximum depth")) {
                    throw e;
                }
            }
        }

        private static List<NodeCache> caches() {
            return List.of(new CHMCache(), new CHMCache(0));
        }

        private static void decodeUnknown(byte[] data) throws IOException {
            var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(data), 0);
            decoder.decode(0, EmptyModel.class);
        }
    }

    public static final class EmptyModel {
        @MaxMindDbConstructor
        public EmptyModel() {
        }
    }

    public static final class KnownFieldModel {
        private final String known;

        @MaxMindDbConstructor
        public KnownFieldModel(@MaxMindDbParameter(name = "known") String known) {
            this.known = known;
        }

        public String known() {
            return this.known;
        }
    }

    public static final class CapacityList extends ArrayList<Object> {
        private static final long serialVersionUID = 1L;
        private final int initialCapacity;

        public CapacityList(int initialCapacity) {
            super(initialCapacity);
            this.initialCapacity = initialCapacity;
        }
    }

    public static final class CapacityMap extends HashMap<String, Object> {
        private static final long serialVersionUID = 1L;
        private final int initialCapacity;

        public CapacityMap(int initialCapacity) {
            super(initialCapacity);
            this.initialCapacity = initialCapacity;
        }
    }

    public static final class AllocationProbe {
        private AllocationProbe() {
        }

        public static void main(String[] args) throws IOException {
            decodeRecursivelyNestedArray();
            decodeRecursivelyNestedMap();
        }

        private static void decodeRecursivelyNestedArray() throws IOException {
            var data = new byte[40_000];
            var encodedSize = 32_768 - 285;
            data[0] = 0x1E; // extended type, size code 30
            data[1] = 0x04; // array
            data[2] = (byte) (encodedSize >> 8);
            data[3] = (byte) encodedSize;
            data[4] = 0x20; // one-byte pointer to offset 0
            data[5] = 0x00;

            expectDepthRejection(data);
        }

        private static void decodeRecursivelyNestedMap() throws IOException {
            var data = new byte[40_000];
            var encodedSize = 16_384 - 285;
            data[0] = (byte) 0xFE; // map, size code 30
            data[1] = (byte) (encodedSize >> 8);
            data[2] = (byte) encodedSize;
            data[3] = 0x41; // one-byte UTF-8 string key
            data[4] = 'a';
            data[5] = (byte) 0xA0; // uint16 with value 0
            data[6] = 0x40; // empty UTF-8 string key
            data[7] = 0x20; // one-byte pointer to offset 0
            data[8] = 0x00;

            expectDepthRejection(data);
        }

        private static void expectDepthRejection(byte[] data) throws IOException {
            var decoder = new Decoder(NoCache.getInstance(), SingleBuffer.wrap(data), 0);
            try {
                decoder.decode(0, Object.class);
                throw new AssertionError("nested large collection decoded without rejection");
            } catch (InvalidDatabaseException e) {
                if (!e.getMessage().contains("exceeds the maximum depth")) {
                    throw e;
                }
            }
        }
    }

    private static <T> void testTypeDecoding(Type type, Map<T, byte[]> tests)
            throws IOException {
        var cache = new CHMCache();

        for (Map.Entry<T, byte[]> entry : tests.entrySet()) {
            var expect = entry.getKey();
            var input = entry.getValue();

            var desc = "decoded " + type.name() + " - " + expect;
            var buffer = SingleBuffer.wrap(input);

            var decoder = new TestDecoder(cache, buffer, 0);

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

                    var got = decoder.decode(0, Map.class);
                    var expectMap = (Map<?, ?>) expect;

                    assertEquals(expectMap.size(), got.size(), desc);

                    for (Object keyObject : expectMap.keySet()) {
                        var key = (String) keyObject;
                        var value = expectMap.get(key);

                        if (value instanceof Object[] arrayValue) {
                            assertArrayEquals(arrayValue, (Object[]) got.get(key), desc);
                        } else {
                            assertEquals(value, got.get(key), desc);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testUint64Coercion() throws IOException {
        // Test data: small UINT64 values that fit in smaller types
        var testData = largeUint(64);

        var cache = new CHMCache();

        // Test UINT64(0) → Byte
        var zeroBytes = testData.get(BigInteger.ZERO);
        var buffer = SingleBuffer.wrap(zeroBytes);
        var decoder = new TestDecoder(cache, buffer, 0);
        assertEquals((byte) 0, decoder.decode(0, Byte.class), "UINT64(0) should coerce to byte");

        // Test UINT64(500) → Long
        buffer = SingleBuffer.wrap(testData.get(BigInteger.valueOf(500)));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(500L, decoder.decode(0, Long.class), "UINT64(500) should coerce to long");

        // Test UINT64(500) → Integer
        buffer = SingleBuffer.wrap(testData.get(BigInteger.valueOf(500)));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(500, decoder.decode(0, Integer.class), "UINT64(500) should coerce to int");

        // Test UINT64(500) → Short
        buffer = SingleBuffer.wrap(testData.get(BigInteger.valueOf(500)));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals((short) 500, decoder.decode(0, Short.class), "UINT64(500) should coerce to short");

        // Test UINT64(500) → Byte (should fail - out of range)
        buffer = SingleBuffer.wrap(testData.get(BigInteger.valueOf(500)));
        decoder = new TestDecoder(cache, buffer, 0);
        var finalDecoder1 = decoder;
        var ex1 = assertThrows(DeserializationException.class,
            () -> finalDecoder1.decode(0, Byte.class),
            "UINT64(500) should not fit in byte");
        assertThat(ex1.getMessage(), containsString("out of range for byte"));

        // Test UINT64(2^64-1) → Long (should fail - too large)
        var maxUint64 = BigInteger.valueOf(2).pow(64).subtract(BigInteger.ONE);
        buffer = SingleBuffer.wrap(testData.get(maxUint64));
        decoder = new TestDecoder(cache, buffer, 0);
        var finalDecoder2 = decoder;
        var ex2 = assertThrows(DeserializationException.class,
            () -> finalDecoder2.decode(0, Long.class),
            "UINT64(2^64-1) should not fit in long");
        assertThat(ex2.getMessage(), containsString("out of range for long"));

        // Test UINT64(2^64-1) → BigInteger (should work)
        buffer = SingleBuffer.wrap(testData.get(maxUint64));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(maxUint64, decoder.decode(0, BigInteger.class),
            "UINT64(2^64-1) should decode to BigInteger");

        // Test UINT64(10872) → Float
        buffer = SingleBuffer.wrap(testData.get(BigInteger.valueOf(10872)));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(10872.0f, decoder.decode(0, Float.class), 0.001f,
            "UINT64(10872) should coerce to float");

        // Test UINT64(10872) → Double
        buffer = SingleBuffer.wrap(testData.get(BigInteger.valueOf(10872)));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(10872.0, decoder.decode(0, Double.class), 0.001,
            "UINT64(10872) should coerce to double");
    }

    @Test
    public void testUint128Coercion() throws IOException {
        // Test data: UINT128 values
        var testData = largeUint(128);

        var cache = new CHMCache();

        // Test UINT128(0) → Long
        var zeroBytes = testData.get(BigInteger.ZERO);
        var buffer = SingleBuffer.wrap(zeroBytes);
        var decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(0L, decoder.decode(0, Long.class), "UINT128(0) should coerce to long");

        // Test UINT128(500) → Integer
        buffer = SingleBuffer.wrap(testData.get(BigInteger.valueOf(500)));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(500, decoder.decode(0, Integer.class), "UINT128(500) should coerce to int");

        // Test UINT128(2^128-1) → Long (should fail - way too large)
        var maxUint128 = BigInteger.valueOf(2).pow(128).subtract(BigInteger.ONE);
        buffer = SingleBuffer.wrap(testData.get(maxUint128));
        decoder = new TestDecoder(cache, buffer, 0);
        var finalDecoder = decoder;
        var ex = assertThrows(DeserializationException.class,
            () -> finalDecoder.decode(0, Long.class),
            "UINT128(2^128-1) should not fit in long");
        assertThat(ex.getMessage(), containsString("out of range for long"));

        // Test UINT128(2^128-1) → BigInteger (should work)
        buffer = SingleBuffer.wrap(testData.get(maxUint128));
        decoder = new TestDecoder(cache, buffer, 0);
        assertEquals(maxUint128, decoder.decode(0, BigInteger.class),
            "UINT128(2^128-1) should decode to BigInteger");
    }

}
