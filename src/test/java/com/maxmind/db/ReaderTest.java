package com.maxmind.db;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;

public class ReaderTest {
    private Reader testReader;

    @Before
    public void setupReader() {
        this.testReader = null;
    }

    @After
    public void teardownReader() throws IOException {
        if (this.testReader != null) {
            this.testReader.close();
        }
    }

    @Test
    public void test() throws IOException {
        for (long recordSize : new long[]{24, 28, 32}) {
            for (int ipVersion : new int[]{4, 6}) {
                File file = getFile("MaxMind-DB-test-ipv" + ipVersion + "-" + recordSize + ".mmdb");
                try (Reader reader = new Reader(file)) {
                    this.testMetadata(reader, ipVersion, recordSize);
                    if (ipVersion == 4) {
                        this.testIpV4(reader, file);
                    } else {
                        this.testIpV6(reader, file);
                    }
                }
            }
        }
    }

    static class GetRecordTest {
        InetAddress ip;
        File db;
        String network;
        boolean hasRecord;

        GetRecordTest(String ip, String file, String network, boolean hasRecord) throws UnknownHostException {
            this.ip = InetAddress.getByName(ip);
            db = getFile(file);
            this.network = network;
            this.hasRecord = hasRecord;
        }
    }

    @Test
    public void testGetRecord() throws IOException {
        GetRecordTest[] mapTests = {
                new GetRecordTest("1.1.1.1", "MaxMind-DB-test-ipv6-32.mmdb", "1.0.0.0/8", false),
                new GetRecordTest("::1:ffff:ffff", "MaxMind-DB-test-ipv6-24.mmdb", "0:0:0:0:0:1:ffff:ffff/128", true),
                new GetRecordTest("::2:0:1", "MaxMind-DB-test-ipv6-24.mmdb", "0:0:0:0:0:2:0:0/122", true),
                new GetRecordTest("1.1.1.1", "MaxMind-DB-test-ipv4-24.mmdb", "1.1.1.1/32", true),
                new GetRecordTest("1.1.1.3", "MaxMind-DB-test-ipv4-24.mmdb", "1.1.1.2/31", true),
                new GetRecordTest("1.1.1.3", "MaxMind-DB-test-decoder.mmdb", "1.1.1.0/24", true),
                new GetRecordTest("::ffff:1.1.1.128", "MaxMind-DB-test-decoder.mmdb", "1.1.1.0/24", true),
                new GetRecordTest("::1.1.1.128", "MaxMind-DB-test-decoder.mmdb", "0:0:0:0:0:0:101:100/120", true),
        };
        for (GetRecordTest test : mapTests) {
            try (Reader reader = new Reader(test.db)) {
                DatabaseRecord record = reader.getRecord(test.ip, Map.class);

                assertEquals(test.network, record.getNetwork().toString());

                if (test.hasRecord) {
                    assertNotNull(record.getData());
                } else {
                    assertNull(record.getData());
                }
            }
        }

        GetRecordTest[] stringTests = {
                new GetRecordTest("200.0.2.1", "MaxMind-DB-no-ipv4-search-tree.mmdb", "0.0.0.0/0", true),
                new GetRecordTest("::200.0.2.1", "MaxMind-DB-no-ipv4-search-tree.mmdb", "0:0:0:0:0:0:0:0/64", true),
                new GetRecordTest("0:0:0:0:ffff:ffff:ffff:ffff", "MaxMind-DB-no-ipv4-search-tree.mmdb", "0:0:0:0:0:0:0:0/64", true),
                new GetRecordTest("ef00::", "MaxMind-DB-no-ipv4-search-tree.mmdb", "8000:0:0:0:0:0:0:0/1", false)
        };
        for (GetRecordTest test : stringTests) {
            try (Reader reader = new Reader(test.db)) {
                DatabaseRecord record = reader.getRecord(test.ip, String.class);

                assertEquals(test.network, record.getNetwork().toString());

                if (test.hasRecord) {
                    assertNotNull(record.getData());
                } else {
                    assertNull(record.getData());
                }
            }
        }
    }


    @Test
    public void testNoIpV4SearchTreeFile() throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-no-ipv4-search-tree.mmdb"));
        this.testNoIpV4SearchTree(this.testReader);
    }

    @Test
    public void testNoIpV4SearchTreeStream() throws IOException {
        this.testReader = new Reader(getStream("MaxMind-DB-no-ipv4-search-tree.mmdb"));
        this.testNoIpV4SearchTree(this.testReader);
    }

    private void testNoIpV4SearchTree(Reader reader) throws IOException {

        assertEquals("::0/64", reader.get(InetAddress.getByName("1.1.1.1"), String.class));
        assertEquals("::0/64", reader.get(InetAddress.getByName("192.1.1.1"), String.class));
    }

    @Test
    public void testDecodingTypesFile() throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));
        this.testDecodingTypes(this.testReader, true);
        this.testDecodingTypesIntoModelObject(this.testReader, true);
        this.testDecodingTypesIntoModelObjectBoxed(this.testReader, true);
        this.testDecodingTypesIntoModelWithList(this.testReader);
    }

    @Test
    public void testDecodingTypesStream() throws IOException {
        this.testReader = new Reader(getStream("MaxMind-DB-test-decoder.mmdb"));
        this.testDecodingTypes(this.testReader, true);
        this.testDecodingTypesIntoModelObject(this.testReader, true);
        this.testDecodingTypesIntoModelObjectBoxed(this.testReader, true);
        this.testDecodingTypesIntoModelWithList(this.testReader);
    }

    @Test
    public void testDecodingTypesPointerDecoderFile() throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-pointer-decoder.mmdb"));
        this.testDecodingTypes(this.testReader, false);
        this.testDecodingTypesIntoModelObject(this.testReader, false);
        this.testDecodingTypesIntoModelObjectBoxed(this.testReader, false);
        this.testDecodingTypesIntoModelWithList(this.testReader);
    }

    private void testDecodingTypes(Reader reader, boolean booleanValue) throws IOException {
        Map record = reader.get(InetAddress.getByName("::1.1.1.0"), Map.class);

        if (booleanValue) {
            assertTrue((boolean) record.get("boolean"));
        } else {
            assertFalse((boolean) record.get("boolean"));
        }

        assertArrayEquals(new byte[]{0, 0, 0, (byte) 42}, (byte[]) record
                .get("bytes"));

        assertEquals("unicode! ☯ - ♫", (String) record.get("utf8_string"));

        @SuppressWarnings("unchecked")
        List<Object> array = (List<Object>) record.get("array");
        assertEquals(3, array.size());
        assertEquals(1, (long) array.get(0));
        assertEquals(2, (long) array.get(1));
        assertEquals(3, (long) array.get(2));

        Map map = (Map) record.get("map");
        assertEquals(1, map.size());

        Map mapX = (Map) map.get("mapX");
        assertEquals(2, mapX.size());

        @SuppressWarnings("unchecked")
        List<Object> arrayX = (List<Object>) mapX.get("arrayX");
        assertEquals(3, arrayX.size());
        assertEquals(7, (long) arrayX.get(0));
        assertEquals(8, (long) arrayX.get(1));
        assertEquals(9, (long) arrayX.get(2));

        assertEquals("hello", (String) mapX.get("utf8_stringX"));

        assertEquals(42.123456, (double) record.get("double"), 0.000000001);
        assertEquals(1.1, (float) record.get("float"), 0.000001);
        assertEquals(-268435456, (int) record.get("int32"));
        assertEquals(100, (int) record.get("uint16"));
        assertEquals(268435456, (long) record.get("uint32"));
        assertEquals(new BigInteger("1152921504606846976"), (BigInteger) record
                .get("uint64"));
        assertEquals(new BigInteger("1329227995784915872903807060280344576"),
                (BigInteger) record.get("uint128"));
    }

    private void testDecodingTypesIntoModelObject(Reader reader, boolean booleanValue)
            throws IOException {
        TestModel model = reader.get(InetAddress.getByName("::1.1.1.0"), TestModel.class);

        if (booleanValue) {
            assertTrue(model.booleanField);
        } else {
            assertFalse(model.booleanField);
        }

        assertArrayEquals(new byte[]{0, 0, 0, (byte) 42}, model.bytesField);

        assertEquals("unicode! ☯ - ♫", model.utf8StringField);

        List<Long> expectedArray = new ArrayList<>(Arrays.asList(
            (long) 1, (long) 2, (long) 3
        ));
        assertEquals(expectedArray, model.arrayField);

        List<Long> expectedArray2 = new ArrayList<>(Arrays.asList(
            (long) 7, (long) 8, (long) 9
        ));
        assertEquals(expectedArray2, model.mapField.mapXField.arrayXField);

        assertEquals("hello", model.mapField.mapXField.utf8StringXField);

        assertEquals(42.123456, model.doubleField, 0.000000001);
        assertEquals(1.1, model.floatField, 0.000001);
        assertEquals(-268435456, model.int32Field);
        assertEquals(100, model.uint16Field);
        assertEquals(268435456, model.uint32Field);
        assertEquals(new BigInteger("1152921504606846976"), model.uint64Field);
        assertEquals(new BigInteger("1329227995784915872903807060280344576"),
                model.uint128Field);
    }

    static class TestModel {
        boolean booleanField;
        byte[] bytesField;
        String utf8StringField;
        List<Long> arrayField;
        MapModel mapField;
        double doubleField;
        float floatField;
        int int32Field;
        int uint16Field;
        long uint32Field;
        BigInteger uint64Field;
        BigInteger uint128Field;

        @MaxMindDbConstructor
        public TestModel (
            @MaxMindDbParameter(name="boolean")
            boolean booleanField,
            @MaxMindDbParameter(name="bytes")
            byte[] bytesField,
            @MaxMindDbParameter(name="utf8_string")
            String utf8StringField,
            @MaxMindDbParameter(name="array")
            List<Long> arrayField,
            @MaxMindDbParameter(name="map")
            MapModel mapField,
            @MaxMindDbParameter(name="double")
            double doubleField,
            @MaxMindDbParameter(name="float")
            float floatField,
            @MaxMindDbParameter(name="int32")
            int int32Field,
            @MaxMindDbParameter(name="uint16")
            int uint16Field,
            @MaxMindDbParameter(name="uint32")
            long uint32Field,
            @MaxMindDbParameter(name="uint64")
            BigInteger uint64Field,
            @MaxMindDbParameter(name="uint128")
            BigInteger uint128Field
        ) {
            this.booleanField = booleanField;
            this.bytesField = bytesField;
            this.utf8StringField = utf8StringField;
            this.arrayField = arrayField;
            this.mapField = mapField;
            this.doubleField = doubleField;
            this.floatField = floatField;
            this.int32Field = int32Field;
            this.uint16Field = uint16Field;
            this.uint32Field = uint32Field;
            this.uint64Field = uint64Field;
            this.uint128Field = uint128Field;
        }
    }

    static class MapModel {
        MapXModel mapXField;

        @MaxMindDbConstructor
        public MapModel (
            @MaxMindDbParameter(name="mapX")
            MapXModel mapXField
        ) {
            this.mapXField = mapXField;
        }
    }

    static class MapXModel {
        List<Long> arrayXField;
        String utf8StringXField;

        @MaxMindDbConstructor
        public MapXModel (
            @MaxMindDbParameter(name="arrayX")
            List<Long> arrayXField,
            @MaxMindDbParameter(name="utf8_stringX")
            String utf8StringXField
        ) {
            this.arrayXField = arrayXField;
            this.utf8StringXField = utf8StringXField;
        }
    }

    private void testDecodingTypesIntoModelObjectBoxed(Reader reader, boolean booleanValue)
            throws IOException {
        TestModelBoxed model = reader.get(InetAddress.getByName("::1.1.1.0"), TestModelBoxed.class);

        if (booleanValue) {
            assertTrue(model.booleanField);
        } else {
            assertFalse(model.booleanField);
        }

        assertArrayEquals(new byte[]{0, 0, 0, (byte) 42}, model.bytesField);

        assertEquals("unicode! ☯ - ♫", model.utf8StringField);

        List<Long> expectedArray = new ArrayList<>(Arrays.asList(
            (long) 1, (long) 2, (long) 3
        ));
        assertEquals(expectedArray, model.arrayField);

        List<Long> expectedArray2 = new ArrayList<>(Arrays.asList(
            (long) 7, (long) 8, (long) 9
        ));
        assertEquals(expectedArray2, model.mapField.mapXField.arrayXField);

        assertEquals("hello", model.mapField.mapXField.utf8StringXField);

        assertEquals(Double.valueOf(42.123456), model.doubleField, 0.000000001);
        assertEquals(Float.valueOf((float) 1.1), model.floatField, 0.000001);
        assertEquals(Integer.valueOf(-268435456), model.int32Field);
        assertEquals(Integer.valueOf(100), model.uint16Field);
        assertEquals(Long.valueOf(268435456), model.uint32Field);
        assertEquals(new BigInteger("1152921504606846976"), model.uint64Field);
        assertEquals(new BigInteger("1329227995784915872903807060280344576"),
                model.uint128Field);
    }

    static class TestModelBoxed {
        Boolean booleanField;
        byte[] bytesField;
        String utf8StringField;
        List<Long> arrayField;
        MapModelBoxed mapField;
        Double doubleField;
        Float floatField;
        Integer int32Field;
        Integer uint16Field;
        Long uint32Field;
        BigInteger uint64Field;
        BigInteger uint128Field;

        @MaxMindDbConstructor
        public TestModelBoxed (
            @MaxMindDbParameter(name="boolean")
            Boolean booleanField,
            @MaxMindDbParameter(name="bytes")
            byte[] bytesField,
            @MaxMindDbParameter(name="utf8_string")
            String utf8StringField,
            @MaxMindDbParameter(name="array")
            List<Long> arrayField,
            @MaxMindDbParameter(name="map")
            MapModelBoxed mapField,
            @MaxMindDbParameter(name="double")
            Double doubleField,
            @MaxMindDbParameter(name="float")
            Float floatField,
            @MaxMindDbParameter(name="int32")
            Integer int32Field,
            @MaxMindDbParameter(name="uint16")
            Integer uint16Field,
            @MaxMindDbParameter(name="uint32")
            Long uint32Field,
            @MaxMindDbParameter(name="uint64")
            BigInteger uint64Field,
            @MaxMindDbParameter(name="uint128")
            BigInteger uint128Field
        ) {
            this.booleanField = booleanField;
            this.bytesField = bytesField;
            this.utf8StringField = utf8StringField;
            this.arrayField = arrayField;
            this.mapField = mapField;
            this.doubleField = doubleField;
            this.floatField = floatField;
            this.int32Field = int32Field;
            this.uint16Field = uint16Field;
            this.uint32Field = uint32Field;
            this.uint64Field = uint64Field;
            this.uint128Field = uint128Field;
        }
    }

    static class MapModelBoxed {
        MapXModelBoxed mapXField;

        @MaxMindDbConstructor
        public MapModelBoxed (
            @MaxMindDbParameter(name="mapX")
            MapXModelBoxed mapXField
        ) {
            this.mapXField = mapXField;
        }
    }

    static class MapXModelBoxed {
        List<Long> arrayXField;
        String utf8StringXField;

        @MaxMindDbConstructor
        public MapXModelBoxed (
            @MaxMindDbParameter(name="arrayX")
            List<Long> arrayXField,
            @MaxMindDbParameter(name="utf8_stringX")
            String utf8StringXField
        ) {
            this.arrayXField = arrayXField;
            this.utf8StringXField = utf8StringXField;
        }
    }

    private void testDecodingTypesIntoModelWithList(Reader reader)
            throws IOException {
        TestModelList model = reader.get(InetAddress.getByName("::1.1.1.0"), TestModelList.class);

        assertEquals(Arrays.asList((long) 1, (long) 2, (long) 3), model.arrayField);
    }

    static class TestModelList {
        List<Long> arrayField;

        @MaxMindDbConstructor
        public TestModelList (
            @MaxMindDbParameter(name="array") List<Long> arrayField
        ) {
            this.arrayField = arrayField;
        }
    }

    @Test
    public void testZerosFile() throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));
        this.testZeros(this.testReader);
        this.testZerosModelObject(this.testReader);
    }

    @Test
    public void testZerosStream() throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));
        this.testZeros(this.testReader);
        this.testZerosModelObject(this.testReader);
    }

    private void testZeros(Reader reader) throws IOException {
        Map record = reader.get(InetAddress.getByName("::"), Map.class);

        assertFalse((boolean) record.get("boolean"));

        assertArrayEquals(new byte[0], (byte[]) record.get("bytes"));

        assertEquals("", (String) record.get("utf8_string"));

        @SuppressWarnings("unchecked")
        List<Object> array = (List<Object>) record.get("array");
        assertEquals(0, array.size());

        Map map = (Map) record.get("map");
        assertEquals(0, map.size());

        assertEquals(0, (double) record.get("double"), 0.000000001);
        assertEquals(0, (float) record.get("float"), 0.000001);
        assertEquals(0, (int) record.get("int32"));
        assertEquals(0, (int) record.get("uint16"));
        assertEquals(0, (long) record.get("uint32"));
        assertEquals(BigInteger.ZERO, (BigInteger) record.get("uint64"));
        assertEquals(BigInteger.ZERO, (BigInteger) record.get("uint128"));
    }

    private void testZerosModelObject(Reader reader) throws IOException {
        TestModel model = reader.get(InetAddress.getByName("::"), TestModel.class);

        assertFalse(model.booleanField);

        assertArrayEquals(new byte[0], model.bytesField);

        assertEquals("", model.utf8StringField);

        List<Long> expectedArray = new ArrayList<>();
        assertEquals(expectedArray, model.arrayField);

        assertNull(model.mapField.mapXField);

        assertEquals(0, model.doubleField, 0.000000001);
        assertEquals(0, model.floatField, 0.000001);
        assertEquals(0, model.int32Field);
        assertEquals(0, model.uint16Field);
        assertEquals(0, model.uint32Field);
        assertEquals(BigInteger.ZERO, model.uint64Field);
        assertEquals(BigInteger.ZERO, model.uint128Field);
    }

    @Test
    public void testDecodeSubdivisions() throws IOException {
        this.testReader = new Reader(getFile("GeoIP2-City-Test.mmdb"));

        TestModelSubdivisions model = this.testReader.get(
            InetAddress.getByName("2.125.160.216"),
            TestModelSubdivisions.class
        );

        assertEquals(2, model.subdivisions.size());
        assertEquals("ENG", model.subdivisions.get(0).isoCode);
        assertEquals("WBK", model.subdivisions.get(1).isoCode);
    }

    static class TestModelSubdivisions {
        List<TestModelSubdivision> subdivisions;

        @MaxMindDbConstructor
        public TestModelSubdivisions (
            @MaxMindDbParameter(name="subdivisions")
            List<TestModelSubdivision> subdivisions
        ) {
            this.subdivisions = subdivisions;
        }
    }

    static class TestModelSubdivision {
        String isoCode;

        @MaxMindDbConstructor
        public TestModelSubdivision(
            @MaxMindDbParameter(name="iso_code")
            String isoCode
        ) {
            this.isoCode = isoCode;
        }
    }

    @Test
    public void testDecodeConcurrentHashMap() throws IOException {
        this.testReader = new Reader(getFile("GeoIP2-City-Test.mmdb"));

        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, Object> m = this.testReader.get(
            InetAddress.getByName("2.125.160.216"),
            ConcurrentHashMap.class
        );

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> subdivisions = (List) m.get("subdivisions");

        Map<String, Object> eng = subdivisions.get(0);

        String isoCode = (String) eng.get("iso_code");
        assertEquals("ENG", isoCode);
    }

    @Test
    public void testDecodeVector() throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));

        TestModelVector model = this.testReader.get(
            InetAddress.getByName("::1.1.1.0"),
            TestModelVector.class
        );

        assertEquals(3, model.arrayField.size());
        assertEquals(1, (long) model.arrayField.get(0));
        assertEquals(2, (long) model.arrayField.get(1));
        assertEquals(3, (long) model.arrayField.get(2));
    }

    static class TestModelVector {
        Vector<Long> arrayField;

        @MaxMindDbConstructor
        public TestModelVector(
            @MaxMindDbParameter(name="array")
            Vector<Long> arrayField
        ) {
            this.arrayField = arrayField;
        }
    }

    // Test that we cache differently depending on more than the offset.
    @Test
    public void testCacheWithDifferentModels() throws IOException {
        NodeCache cache = new CHMCache();

        this.testReader = new Reader(
                getFile("MaxMind-DB-test-decoder.mmdb"),
                cache
        );

        TestModelA modelA = this.testReader.get(
            InetAddress.getByName("::1.1.1.0"),
            TestModelA.class
        );
        assertEquals("unicode! ☯ - ♫", modelA.utf8StringFieldA);

        TestModelB modelB = this.testReader.get(
            InetAddress.getByName("::1.1.1.0"),
            TestModelB.class
        );
        assertEquals("unicode! ☯ - ♫", modelB.utf8StringFieldB);
    }

    static class TestModelA {
        String utf8StringFieldA;

        @MaxMindDbConstructor
        public TestModelA (
            @MaxMindDbParameter(name="utf8_string") String utf8StringFieldA
        ) {
            this.utf8StringFieldA = utf8StringFieldA;
        }
    }

    static class TestModelB {
        String utf8StringFieldB;

        @MaxMindDbConstructor
        public TestModelB (
            @MaxMindDbParameter(name="utf8_string") String utf8StringFieldB
        ) {
            this.utf8StringFieldB = utf8StringFieldB;
        }
    }

    @Test
    public void testCacheKey() {
        Class<TestModelCacheKey> cls = TestModelCacheKey.class;

        CacheKey a = new CacheKey(1, cls, getType(cls, 0));
        CacheKey b = new CacheKey(1, cls, getType(cls, 0));
        assertEquals(a, b);

        CacheKey c = new CacheKey(2, cls, getType(cls, 0));
        assertNotEquals(a, c);

        CacheKey d = new CacheKey(1, String.class, getType(cls, 0));
        assertNotEquals(a, d);

        CacheKey e = new CacheKey(1, cls, getType(cls, 1));
        assertNotEquals(a, e);
    }

    private <T> java.lang.reflect.Type getType(Class<T> cls, int i) {
        Constructor<?>[] constructors = cls.getConstructors();
        Constructor<TestModelCacheKey> constructor = null;
        for (Constructor<?> constructor2 : constructors) {
            constructor = (Constructor<TestModelCacheKey>) constructor2;
            break;
        }
        assertNotNull(constructor);

        java.lang.reflect.Type[] types = constructor.getGenericParameterTypes();
        return types[i];
    }

    static class TestModelCacheKey {
        private List<Long> a;
        private List<Integer> b;

        public TestModelCacheKey (List<Long> a, List<Integer> b) {
            this.a = a;
            this.b = b;
        }
    }

    @Test
    public void testBrokenDatabaseFile() throws IOException {
        this.testReader = new Reader(getFile("GeoIP2-City-Test-Broken-Double-Format.mmdb"));
        this.testBrokenDatabase(this.testReader);
    }

    @Test
    public void testBrokenDatabaseStream() throws IOException {
        this.testReader = new Reader(getStream("GeoIP2-City-Test-Broken-Double-Format.mmdb"));
        this.testBrokenDatabase(this.testReader);
    }

    private void testBrokenDatabase(Reader reader) {
        InvalidDatabaseException ex = assertThrows(
                InvalidDatabaseException.class,
                () -> reader.get(InetAddress.getByName("2001:220::"), Map.class));
        assertThat(ex.getMessage(), containsString("The MaxMind DB file's data section contains bad data"));
    }

    @Test
    public void testBrokenSearchTreePointerFile() throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-broken-pointers-24.mmdb"));
        this.testBrokenSearchTreePointer(this.testReader);
    }

    @Test
    public void testBrokenSearchTreePointerStream() throws IOException {
        this.testReader = new Reader(getStream("MaxMind-DB-test-broken-pointers-24.mmdb"));
        this.testBrokenSearchTreePointer(this.testReader);
    }

    private void testBrokenSearchTreePointer(Reader reader) {
        InvalidDatabaseException ex = assertThrows(InvalidDatabaseException.class,
                () -> reader.get(InetAddress.getByName("1.1.1.32"), Map.class));
        assertThat(ex.getMessage(), containsString("The MaxMind DB file's search tree is corrupt"));
    }

    @Test
    public void testBrokenDataPointerFile() throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-broken-pointers-24.mmdb"));
        this.testBrokenDataPointer(this.testReader);
    }

    @Test
    public void testBrokenDataPointerStream() throws IOException {
        this.testReader = new Reader(getStream("MaxMind-DB-test-broken-pointers-24.mmdb"));
        this.testBrokenDataPointer(this.testReader);
    }

    private void testBrokenDataPointer(Reader reader) {
        InvalidDatabaseException ex = assertThrows(InvalidDatabaseException.class,
                () -> reader.get(InetAddress.getByName("1.1.1.16"), Map.class));
        assertThat(ex.getMessage(), containsString("The MaxMind DB file's data section contains bad data"));
    }

    @Test
    public void testClosedReaderThrowsException() throws IOException {
        Reader reader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"));

        reader.close();
        ClosedDatabaseException ex = assertThrows(ClosedDatabaseException.class,
                () -> reader.get(InetAddress.getByName("1.1.1.16"), Map.class));
        assertEquals("The MaxMind DB has been closed.", ex.getMessage());
    }

    @Test
    public void voidTestMapKeyIsString() throws IOException {
        this.testReader = new Reader(getFile("GeoIP2-City-Test.mmdb"));

        DeserializationException ex = assertThrows(
            DeserializationException.class,
            () -> this.testReader.get(
                InetAddress.getByName("2.125.160.216"),
                TestModelInvalidMap.class
            )
        );
        assertEquals("Map keys must be strings.", ex.getMessage());
    }

    static class TestModelInvalidMap {
        Map<Integer, String> postal;

        @MaxMindDbConstructor
        public TestModelInvalidMap (
            @MaxMindDbParameter(name="postal")
            Map<Integer, String> postal
        ) {
            this.postal = postal;
        }
    }

    private void testMetadata(Reader reader, int ipVersion, long recordSize) {

        Metadata metadata = reader.getMetadata();

        assertEquals("major version", 2, metadata.getBinaryFormatMajorVersion());
        assertEquals(0, metadata.getBinaryFormatMinorVersion());
        assertEquals(ipVersion, metadata.getIpVersion());
        assertEquals("Test", metadata.getDatabaseType());

        List<String> languages = new ArrayList<>(Arrays.asList("en", "zh"));

        assertEquals(languages, metadata.getLanguages());

        Map<String, String> description = new HashMap<>();
        description.put("en", "Test Database");
        description.put("zh", "Test Database Chinese");

        assertEquals(description, metadata.getDescription());
        assertEquals(recordSize, metadata.getRecordSize());

        Calendar cal = Calendar.getInstance();
        cal.set(2014, Calendar.JANUARY, 1);

        assertTrue(metadata.getBuildDate().compareTo(cal.getTime()) > 0);
    }

    private void testIpV4(Reader reader, File file) throws IOException {

        for (int i = 0; i <= 5; i++) {
            String address = "1.1.1." + (int) Math.pow(2, i);
            Map<String, String> data = new HashMap<>();
            data.put("ip", address);

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address), Map.class));
        }

        Map<String, String> pairs = new HashMap<>();
        pairs.put("1.1.1.3", "1.1.1.2");
        pairs.put("1.1.1.5", "1.1.1.4");
        pairs.put("1.1.1.7", "1.1.1.4");
        pairs.put("1.1.1.9", "1.1.1.8");
        pairs.put("1.1.1.15", "1.1.1.8");
        pairs.put("1.1.1.17", "1.1.1.16");
        pairs.put("1.1.1.31", "1.1.1.16");
        for (String address : pairs.keySet()) {
            Map<String, String> data = new HashMap<>();
            data.put("ip", pairs.get(address));

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address), Map.class));
        }

        for (String ip : new String[]{"1.1.1.33", "255.254.253.123"}) {
            assertNull(reader.get(InetAddress.getByName(ip), Map.class));
        }
    }

    // XXX - logic could be combined with above
    private void testIpV6(Reader reader, File file) throws IOException {
        String[] subnets = new String[]{"::1:ffff:ffff", "::2:0:0",
                "::2:0:40", "::2:0:50", "::2:0:58"};

        for (String address : subnets) {
            Map<String, String> data = new HashMap<>();
            data.put("ip", address);

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address), Map.class));
        }

        Map<String, String> pairs = new HashMap<>();
        pairs.put("::2:0:1", "::2:0:0");
        pairs.put("::2:0:33", "::2:0:0");
        pairs.put("::2:0:39", "::2:0:0");
        pairs.put("::2:0:41", "::2:0:40");
        pairs.put("::2:0:49", "::2:0:40");
        pairs.put("::2:0:52", "::2:0:50");
        pairs.put("::2:0:57", "::2:0:50");
        pairs.put("::2:0:59", "::2:0:58");

        for (String address : pairs.keySet()) {
            Map<String, String> data = new HashMap<>();
            data.put("ip", pairs.get(address));

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address), Map.class));
        }

        for (String ip : new String[]{"1.1.1.33", "255.254.253.123", "89fa::"}) {
            assertNull(reader.get(InetAddress.getByName(ip), Map.class));
        }
    }

    static File getFile(String name) {
        return new File(ReaderTest.class.getResource("/maxmind-db/test-data/" + name).getFile());
    }

    static InputStream getStream(String name) {
        return ReaderTest.class.getResourceAsStream("/maxmind-db/test-data/" + name);
    }

}
