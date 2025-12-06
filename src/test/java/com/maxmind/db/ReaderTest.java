package com.maxmind.db;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ReaderTest {
    private Reader testReader;

    @BeforeEach
    public void setupReader() {
        this.testReader = null;
    }

    @AfterEach
    public void teardownReader() throws IOException {
        if (this.testReader != null) {
            this.testReader.close();
        }
    }

    static IntStream chunkSizes() {
        var sizes = new int[] {
                512,
                2048,
                // The default chunk size of the MultiBuffer is close to max int, that causes
                // some issues when running tests in CI as we try to allocate some byte arrays
                // that are too big to fit in the heap.
                // We use half of that just to be sure nothing breaks, but big enough that we
                // ensure SingleBuffer is tested too using the test MMDBs.
                MultiBuffer.DEFAULT_CHUNK_SIZE / 4,
        };
        return IntStream.of(sizes);
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void test(int chunkSize) throws IOException {
        for (long recordSize : new long[] {24, 28, 32}) {
            for (int ipVersion : new int[] {4, 6}) {
                var file = getFile("MaxMind-DB-test-ipv" + ipVersion + "-" + recordSize + ".mmdb");
                try (var reader = new Reader(file, chunkSize)) {
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

        GetRecordTest(String ip, String file, String network, boolean hasRecord)
            throws UnknownHostException {
            this.ip = InetAddress.getByName(ip);
            db = getFile(file);
            this.network = network;
            this.hasRecord = hasRecord;
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testNetworks(int chunkSize) throws IOException, InvalidDatabaseException, InvalidNetworkException {
        for (long recordSize : new long[] {24, 28, 32}) {
            for (int ipVersion : new int[] {4, 6}) {
                var file = getFile("MaxMind-DB-test-ipv" + ipVersion + "-" + recordSize + ".mmdb");

                var reader = new Reader(file, chunkSize);
                var networks = reader.networks(false, Map.class);

                while(networks.hasNext()) {
                    var iteration = networks.next();
                    var data = (Map<?, ?>) iteration.data();

                    var actualIPInData = InetAddress.getByName((String) data.get("ip"));

                    assertEquals(
                        iteration.network().networkAddress(), 
                        actualIPInData,
                        "expected ip address"
                    );
                }

                reader.close();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testNetworksWithInvalidSearchTree(int chunkSize) throws IOException, InvalidNetworkException{
        var file = getFile("MaxMind-DB-test-broken-search-tree-24.mmdb");
        var reader = new Reader(file, chunkSize);

        var networks = reader.networks(false, Map.class);

        var exception = assertThrows(RuntimeException.class, () -> {
            while(networks.hasNext()){
                assertNotNull(networks.next());
            }
        });

        assertEquals("Invalid search tree", exception.getMessage());
        reader.close();
    }

    private class networkTest {
        String network;
        String database;
        int prefix;
        String[] expected;
        boolean skipAliasedNetworks;
        public networkTest(String network,  int prefix,String database, String[] expected, boolean skipAliasedNetworks){
            this(network, prefix, database, expected);
            this.skipAliasedNetworks = skipAliasedNetworks;
        }
        public networkTest(String network,  int prefix,String database, String[] expected){
            this.network = network;
            this.prefix = prefix;
            this.database = database;
            this.expected = expected;
        }
    }

    private networkTest[] tests = new networkTest[]{
        new networkTest(
            "0.0.0.0",
            0,
            "ipv4",
            new String[]{
                "1.1.1.1/32",
                "1.1.1.2/31",
                "1.1.1.4/30",
                "1.1.1.8/29",
                "1.1.1.16/28",
                "1.1.1.32/32",
            }
        ),
        new networkTest(
            "1.1.1.1",
            30,
            "ipv4",
            new String[]{
                "1.1.1.1/32",
                "1.1.1.2/31",
            }
        ),
        new networkTest(
            "1.1.1.1",
            32,
            "ipv4",
            new String[]{
                "1.1.1.1/32",
            }
        ),
        new networkTest(
            "255.255.255.0",
            24,
            "ipv4",
            new String[]{}
        ),
        new networkTest(
            "1.1.1.1",
            32,
            "mixed",
            new String[]{
                "1.1.1.1/32",
            }
        ),
        new networkTest(
            "255.255.255.0",
            24,
            "mixed",
            new String[]{}
        ),
        new networkTest(
            "::1:ffff:ffff",
            128,
            "ipv6",
            new String[]{
                "0:0:0:0:0:1:ffff:ffff/128",
            },
            true
        ),
        new networkTest(
            "::",
            0,
            "ipv6",
            new String[]{
                "0:0:0:0:0:1:ffff:ffff/128",
                "0:0:0:0:0:2:0:0/122",
                "0:0:0:0:0:2:0:40/124",
                "0:0:0:0:0:2:0:50/125",
                "0:0:0:0:0:2:0:58/127",
            }
        ),
        new networkTest(
            "::2:0:40",
            123,
            "ipv6",
            new String[]{
                "0:0:0:0:0:2:0:40/124",
                "0:0:0:0:0:2:0:50/125",
                "0:0:0:0:0:2:0:58/127",
            }
        ),
        new networkTest(
            "0:0:0:0:0:ffff:ffff:ff00",
            120,
            "ipv6",
            new String[]{}
        ),
        new networkTest(
            "0.0.0.0",
            0,
            "mixed",
            new String[]{
                "1.1.1.1/32",
                "1.1.1.2/31",
                "1.1.1.4/30",
                "1.1.1.8/29",
                "1.1.1.16/28",
                "1.1.1.32/32",
            }
        ),
        new networkTest(
            "0.0.0.0",
            0,
            "mixed",
            new String[]{
                "1.1.1.1/32",
                "1.1.1.2/31",
                "1.1.1.4/30",
                "1.1.1.8/29",
                "1.1.1.16/28",
                "1.1.1.32/32",
            },
            true
        ),
        new networkTest(
            "::",
            0,
            "mixed",
            new String[]{
                "0:0:0:0:0:0:101:101/128",
                "0:0:0:0:0:0:101:102/127",
                "0:0:0:0:0:0:101:104/126",
                "0:0:0:0:0:0:101:108/125",
                "0:0:0:0:0:0:101:110/124",
                "0:0:0:0:0:0:101:120/128",
                "0:0:0:0:0:1:ffff:ffff/128",
                "0:0:0:0:0:2:0:0/122",
                "0:0:0:0:0:2:0:40/124",
                "0:0:0:0:0:2:0:50/125",
                "0:0:0:0:0:2:0:58/127",
                "1.1.1.1/32",
                "1.1.1.2/31",
                "1.1.1.4/30",
                "1.1.1.8/29",
                "1.1.1.16/28",
                "1.1.1.32/32",
                "2001:0:101:101:0:0:0:0/64",
                "2001:0:101:102:0:0:0:0/63",
                "2001:0:101:104:0:0:0:0/62",
                "2001:0:101:108:0:0:0:0/61",
                "2001:0:101:110:0:0:0:0/60",
                "2001:0:101:120:0:0:0:0/64",
                "2002:101:101:0:0:0:0:0/48",
                "2002:101:102:0:0:0:0:0/47",
                "2002:101:104:0:0:0:0:0/46",
                "2002:101:108:0:0:0:0:0/45",
                "2002:101:110:0:0:0:0:0/44",
                "2002:101:120:0:0:0:0:0/48",
            }
        ),
        new networkTest(
            "::",
            0,
            "mixed",
            new String[]{
                "1.1.1.1/32",
                "1.1.1.2/31",
                "1.1.1.4/30",
                "1.1.1.8/29",
                "1.1.1.16/28",
                "1.1.1.32/32",
                "0:0:0:0:0:1:ffff:ffff/128",
                "0:0:0:0:0:2:0:0/122",
                "0:0:0:0:0:2:0:40/124",
                "0:0:0:0:0:2:0:50/125",
                "0:0:0:0:0:2:0:58/127",
            },
            true
        ),
        new networkTest(
            "1.1.1.16",
            28,
            "mixed",
            new String[]{
                "1.1.1.16/28"
            }
        ),
        new networkTest(
            "1.1.1.4",
            30,
            "ipv4",
            new String[]{
                "1.1.1.4/30"
            }
        )
    };

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testNetworksWithin(int chunkSize) throws IOException, InvalidNetworkException{
        for(networkTest test : tests){
            for(int recordSize : new int[]{24, 28, 32}){
                var file = getFile("MaxMind-DB-test-"+test.database+"-"+recordSize+".mmdb");
                var reader = new Reader(file, chunkSize);

                var address = InetAddress.getByName(test.network);
                var network = new Network(address, test.prefix);

                boolean includeAliasedNetworks = !test.skipAliasedNetworks;
                var networks = reader.networksWithin(network, includeAliasedNetworks, Map.class);

                var innerIPs = new ArrayList<String>();
                while(networks.hasNext()){
                    var iteration = networks.next();
                    innerIPs.add(iteration.network().toString());
                }

                assertArrayEquals(test.expected, innerIPs.toArray());

                reader.close();
            }
        }
    }

    private networkTest[] geoipTests = new networkTest[]{
        new networkTest(
            "81.2.69.128",
            26,
            "GeoIP2-Country-Test.mmdb",
            new String[]{
                "81.2.69.142/31",
                "81.2.69.144/28",
                "81.2.69.160/27",
            }
        )
    };

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testGeoIPNetworksWithin(int chunkSize) throws IOException, InvalidNetworkException{
        for (networkTest test : geoipTests){
            var file = getFile(test.database);
            var reader = new Reader(file, chunkSize);

            var address = InetAddress.getByName(test.network);
            var network = new Network(address, test.prefix);

            var networks = reader.networksWithin(network, test.skipAliasedNetworks, Map.class);

            var innerIPs = new ArrayList<String>();
            while(networks.hasNext()){
                var iteration = networks.next();
                innerIPs.add(iteration.network().toString());
            }

            assertArrayEquals(test.expected, innerIPs.toArray());

            reader.close();
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testGetRecord(int chunkSize) throws IOException {
        var mapTests = new GetRecordTest[] {
            new GetRecordTest("1.1.1.1", "MaxMind-DB-test-ipv6-32.mmdb", "1.0.0.0/8", false),
            new GetRecordTest("::1:ffff:ffff", "MaxMind-DB-test-ipv6-24.mmdb",
                "0:0:0:0:0:1:ffff:ffff/128", true),
            new GetRecordTest("::2:0:1", "MaxMind-DB-test-ipv6-24.mmdb", "0:0:0:0:0:2:0:0/122",
                true),
            new GetRecordTest("1.1.1.1", "MaxMind-DB-test-ipv4-24.mmdb", "1.1.1.1/32", true),
            new GetRecordTest("1.1.1.3", "MaxMind-DB-test-ipv4-24.mmdb", "1.1.1.2/31", true),
            new GetRecordTest("1.1.1.3", "MaxMind-DB-test-decoder.mmdb", "1.1.1.0/24", true),
            new GetRecordTest("::ffff:1.1.1.128", "MaxMind-DB-test-decoder.mmdb", "1.1.1.0/24",
                true),
            new GetRecordTest("::1.1.1.128", "MaxMind-DB-test-decoder.mmdb",
                "0:0:0:0:0:0:101:100/120", true),
        };
        for (GetRecordTest test : mapTests) {
            try (var reader = new Reader(test.db, chunkSize)) {
                var record = reader.getRecord(test.ip, Map.class);

                assertEquals(test.network, record.network().toString());

                if (test.hasRecord) {
                    assertNotNull(record.data());
                } else {
                    assertNull(record.data());
                }
            }
        }

        var stringTests = new GetRecordTest[] {
            new GetRecordTest("200.0.2.1", "MaxMind-DB-no-ipv4-search-tree.mmdb", "0.0.0.0/0",
                true),
            new GetRecordTest("::200.0.2.1", "MaxMind-DB-no-ipv4-search-tree.mmdb",
                "0:0:0:0:0:0:0:0/64", true),
            new GetRecordTest("0:0:0:0:ffff:ffff:ffff:ffff", "MaxMind-DB-no-ipv4-search-tree.mmdb",
                "0:0:0:0:0:0:0:0/64", true),
            new GetRecordTest("ef00::", "MaxMind-DB-no-ipv4-search-tree.mmdb",
                "8000:0:0:0:0:0:0:0/1", false)
        };
        for (GetRecordTest test : stringTests) {
            try (var reader = new Reader(test.db, chunkSize)) {
                var record = reader.getRecord(test.ip, String.class);

                assertEquals(test.network, record.network().toString());

                if (test.hasRecord) {
                    assertNotNull(record.data());
                } else {
                    assertNull(record.data());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testMetadataPointers(int chunkSize) throws IOException {
        var reader = new Reader(getFile("MaxMind-DB-test-metadata-pointers.mmdb"), chunkSize);
        assertEquals("Lots of pointers in metadata", reader.getMetadata().databaseType());
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testNoIpV4SearchTreeFile(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-no-ipv4-search-tree.mmdb"), chunkSize);
        this.testNoIpV4SearchTree(this.testReader);
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testNoIpV4SearchTreeStream(int chunkSizes) throws IOException {
        this.testReader = new Reader(getStream("MaxMind-DB-no-ipv4-search-tree.mmdb"), chunkSizes);
        this.testNoIpV4SearchTree(this.testReader);
    }

    private void testNoIpV4SearchTree(Reader reader) throws IOException {

        assertEquals("::0/64", reader.get(InetAddress.getByName("1.1.1.1"), String.class));
        assertEquals("::0/64", reader.get(InetAddress.getByName("192.1.1.1"), String.class));
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testDecodingTypesFile(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);
        this.testDecodingTypes(this.testReader, true);
        this.testDecodingTypesIntoModelObject(this.testReader, true);
        this.testDecodingTypesIntoModelObjectBoxed(this.testReader, true);
        this.testDecodingTypesIntoModelWithList(this.testReader);
        this.testRecordImplicitConstructor(this.testReader);
        this.testSingleConstructorWithoutAnnotation(this.testReader);
        this.testPojoImplicitParameters(this.testReader);
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testDecodingTypesStream(int chunkSize) throws IOException {
        this.testReader = new Reader(getStream("MaxMind-DB-test-decoder.mmdb"), chunkSize);
        this.testDecodingTypes(this.testReader, true);
        this.testDecodingTypesIntoModelObject(this.testReader, true);
        this.testDecodingTypesIntoModelObjectBoxed(this.testReader, true);
        this.testDecodingTypesIntoModelWithList(this.testReader);
        this.testRecordImplicitConstructor(this.testReader);
        this.testSingleConstructorWithoutAnnotation(this.testReader);
        this.testPojoImplicitParameters(this.testReader);
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testContextAnnotations(int chunkSize) throws IOException {
        try (var reader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize)) {
            var firstIp = InetAddress.getByName("1.1.1.1");
            var secondIp = InetAddress.getByName("1.1.1.3");

            var expectedNetwork = reader.getRecord(firstIp, Map.class).network().toString();

            var first = reader.get(firstIp, ContextModel.class);
            var second = reader.get(secondIp, ContextModel.class);

            assertEquals(firstIp, first.lookupIp);
            assertEquals(firstIp.getHostAddress(), first.lookupIpString);
            assertEquals(expectedNetwork, first.lookupNetwork.toString());
            assertEquals(expectedNetwork, first.lookupNetworkString);
            assertEquals(firstIp, first.lookupNetwork.ipAddress());
            assertEquals(100, first.uint16Field);

            assertEquals(secondIp, second.lookupIp);
            assertEquals(secondIp.getHostAddress(), second.lookupIpString);
            assertEquals(expectedNetwork, second.lookupNetwork.toString());
            assertEquals(expectedNetwork, second.lookupNetworkString);
            assertEquals(secondIp, second.lookupNetwork.ipAddress());
            assertEquals(100, second.uint16Field);
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testNestedContextAnnotations(int chunkSize) throws IOException {
        try (var reader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize)) {
            var firstIp = InetAddress.getByName("1.1.1.1");
            var secondIp = InetAddress.getByName("1.1.1.3");
            var expectedNetwork = reader.getRecord(firstIp, Map.class).network().toString();

            var first = reader.get(firstIp, WrapperContextOnlyModel.class);
            var second = reader.get(secondIp, WrapperContextOnlyModel.class);

            assertNotNull(first.context);
            assertEquals(firstIp, first.context.lookupIp);
            assertEquals(expectedNetwork, first.context.lookupNetwork.toString());

            assertNotNull(second.context);
            assertEquals(secondIp, second.context.lookupIp);
            assertEquals(expectedNetwork, second.context.lookupNetwork.toString());
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testNestedContextAnnotationsWithCache(int chunkSize) throws IOException {
        var cache = new CHMCache();
        try (var reader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), cache, chunkSize)) {
            var firstIp = InetAddress.getByName("1.1.1.1");
            var secondIp = InetAddress.getByName("1.1.1.3");
            var expectedNetwork = reader.getRecord(firstIp, Map.class).network().toString();

            var first = reader.get(firstIp, WrapperContextOnlyModel.class);
            var second = reader.get(secondIp, WrapperContextOnlyModel.class);

            assertNotNull(first.context);
            assertEquals(firstIp, first.context.lookupIp);
            assertEquals(expectedNetwork, first.context.lookupNetwork.toString());

            assertNotNull(second.context);
            assertEquals(secondIp, second.context.lookupIp);
            assertEquals(expectedNetwork, second.context.lookupNetwork.toString());
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testCreatorMethod(int chunkSize) throws IOException {
        try (var reader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize)) {
            // Test with IP that has boolean=true
            var ipTrue = InetAddress.getByName("1.1.1.1");
            var resultTrue = reader.get(ipTrue, CreatorMethodModel.class);
            assertNotNull(resultTrue);
            assertNotNull(resultTrue.enumField);
            assertEquals(BooleanEnum.TRUE_VALUE, resultTrue.enumField);

            // Test with IP that has boolean=false
            var ipFalse = InetAddress.getByName("::");
            var resultFalse = reader.get(ipFalse, CreatorMethodModel.class);
            assertNotNull(resultFalse);
            assertNotNull(resultFalse.enumField);
            assertEquals(BooleanEnum.FALSE_VALUE, resultFalse.enumField);
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testCreatorMethodWithString(int chunkSize) throws IOException {
        try (var reader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize)) {
            // The database has utf8_stringX="hello" in map.mapX at this IP
            var ip = InetAddress.getByName("1.1.1.1");

            // Get the nested map containing utf8_stringX to verify the raw data
            var record = reader.get(ip, Map.class);
            var map = (Map<?, ?>) record.get("map");
            assertNotNull(map);
            var mapX = (Map<?, ?>) map.get("mapX");
            assertNotNull(mapX);
            assertEquals("hello", mapX.get("utf8_stringX"));

            // Now test that the creator method converts "hello" to StringEnum.HELLO
            var result = reader.get(ip, StringEnumModel.class);
            assertNotNull(result);
            assertNotNull(result.map);
            assertNotNull(result.map.mapX);
            assertNotNull(result.map.mapX.stringEnumField);
            assertEquals(StringEnum.HELLO, result.map.mapX.stringEnumField);
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testDecodingTypesPointerDecoderFile(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-pointer-decoder.mmdb"), chunkSize);
        this.testDecodingTypes(this.testReader, false);
        this.testDecodingTypesIntoModelObject(this.testReader, false);
        this.testDecodingTypesIntoModelObjectBoxed(this.testReader, false);
        this.testDecodingTypesIntoModelWithList(this.testReader);
    }

    private void testDecodingTypes(Reader reader, boolean booleanValue) throws IOException {
        var record = reader.get(InetAddress.getByName("::1.1.1.0"), Map.class);

        if (booleanValue) {
            assertTrue((boolean) record.get("boolean"));
        } else {
            assertFalse((boolean) record.get("boolean"));
        }

        assertArrayEquals(new byte[] {0, 0, 0, (byte) 42}, (byte[]) record
            .get("bytes"));

        assertEquals("unicode! ☯ - ♫", record.get("utf8_string"));

        var array = (List<?>) record.get("array");
        assertEquals(3, array.size());
        assertEquals(1, (long) array.get(0));
        assertEquals(2, (long) array.get(1));
        assertEquals(3, (long) array.get(2));

        var map = (Map<?, ?>) record.get("map");
        assertEquals(1, map.size());

        var mapX = (Map<?, ?>) map.get("mapX");
        assertEquals(2, mapX.size());

        var arrayX = (List<?>) mapX.get("arrayX");
        assertEquals(3, arrayX.size());
        assertEquals(7, (long) arrayX.get(0));
        assertEquals(8, (long) arrayX.get(1));
        assertEquals(9, (long) arrayX.get(2));

        assertEquals("hello", mapX.get("utf8_stringX"));

        assertEquals(42.123456, (double) record.get("double"), 0.000000001);
        assertEquals(1.1, (float) record.get("float"), 0.000001);
        assertEquals(-268435456, (int) record.get("int32"));
        assertEquals(100, (int) record.get("uint16"));
        assertEquals(268435456, (long) record.get("uint32"));
        assertEquals(new BigInteger("1152921504606846976"), record
            .get("uint64"));
        assertEquals(new BigInteger("1329227995784915872903807060280344576"),
            record.get("uint128"));
    }

    private void testDecodingTypesIntoModelObject(Reader reader, boolean booleanValue)
        throws IOException {
        var model = reader.get(InetAddress.getByName("::1.1.1.0"), TestModel.class);

        if (booleanValue) {
            assertTrue(model.booleanField);
        } else {
            assertFalse(model.booleanField);
        }

        assertArrayEquals(new byte[] {0, 0, 0, (byte) 42}, model.bytesField);

        assertEquals("unicode! ☯ - ♫", model.utf8StringField);

        var expectedArray = new ArrayList<>(List.of(
            (long) 1, (long) 2, (long) 3
        ));
        assertEquals(expectedArray, model.arrayField);

        var expectedArray2 = new ArrayList<>(List.of(
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

    static class ContextModel {
        InetAddress lookupIp;
        String lookupIpString;
        Network lookupNetwork;
        String lookupNetworkString;
        int uint16Field;

        @MaxMindDbConstructor
        public ContextModel(
            @MaxMindDbIpAddress InetAddress lookupIp,
            @MaxMindDbIpAddress String lookupIpString,
            @MaxMindDbNetwork Network lookupNetwork,
            @MaxMindDbNetwork String lookupNetworkString,
            @MaxMindDbParameter(name = "uint16") int uint16Field
        ) {
            this.lookupIp = lookupIp;
            this.lookupIpString = lookupIpString;
            this.lookupNetwork = lookupNetwork;
            this.lookupNetworkString = lookupNetworkString;
            this.uint16Field = uint16Field;
        }
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
        public TestModel(
            @MaxMindDbParameter(name = "boolean")
            boolean booleanField,
            @MaxMindDbParameter(name = "bytes")
            byte[] bytesField,
            @MaxMindDbParameter(name = "utf8_string")
            String utf8StringField,
            @MaxMindDbParameter(name = "array")
            List<Long> arrayField,
            @MaxMindDbParameter(name = "map")
            MapModel mapField,
            @MaxMindDbParameter(name = "double")
            double doubleField,
            @MaxMindDbParameter(name = "float")
            float floatField,
            @MaxMindDbParameter(name = "int32")
            int int32Field,
            @MaxMindDbParameter(name = "uint16")
            int uint16Field,
            @MaxMindDbParameter(name = "uint32")
            long uint32Field,
            @MaxMindDbParameter(name = "uint64")
            BigInteger uint64Field,
            @MaxMindDbParameter(name = "uint128")
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
        public MapModel(
            @MaxMindDbParameter(name = "mapX")
            MapXModel mapXField
        ) {
            this.mapXField = mapXField;
        }
    }

    static class MapXModel {
        List<Long> arrayXField;
        String utf8StringXField;

        @MaxMindDbConstructor
        public MapXModel(
            @MaxMindDbParameter(name = "arrayX")
            List<Long> arrayXField,
            @MaxMindDbParameter(name = "utf8_stringX")
            String utf8StringXField
        ) {
            this.arrayXField = arrayXField;
            this.utf8StringXField = utf8StringXField;
        }
    }

    private void testDecodingTypesIntoModelObjectBoxed(Reader reader, boolean booleanValue)
        throws IOException {
        var model = reader.get(InetAddress.getByName("::1.1.1.0"), TestModelBoxed.class);

        if (booleanValue) {
            assertTrue(model.booleanField);
        } else {
            assertFalse(model.booleanField);
        }

        assertArrayEquals(new byte[] {0, 0, 0, (byte) 42}, model.bytesField);

        assertEquals("unicode! ☯ - ♫", model.utf8StringField);

        var expectedArray = new ArrayList<>(List.of(
            (long) 1, (long) 2, (long) 3
        ));
        assertEquals(expectedArray, model.arrayField);

        var expectedArray2 = new ArrayList<>(List.of(
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
        public TestModelBoxed(
            @MaxMindDbParameter(name = "boolean")
            Boolean booleanField,
            @MaxMindDbParameter(name = "bytes")
            byte[] bytesField,
            @MaxMindDbParameter(name = "utf8_string")
            String utf8StringField,
            @MaxMindDbParameter(name = "array")
            List<Long> arrayField,
            @MaxMindDbParameter(name = "map")
            MapModelBoxed mapField,
            @MaxMindDbParameter(name = "double")
            Double doubleField,
            @MaxMindDbParameter(name = "float")
            Float floatField,
            @MaxMindDbParameter(name = "int32")
            Integer int32Field,
            @MaxMindDbParameter(name = "uint16")
            Integer uint16Field,
            @MaxMindDbParameter(name = "uint32")
            Long uint32Field,
            @MaxMindDbParameter(name = "uint64")
            BigInteger uint64Field,
            @MaxMindDbParameter(name = "uint128")
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

    static class ContextOnlyModel {
        InetAddress lookupIp;
        Network lookupNetwork;

        @MaxMindDbConstructor
        public ContextOnlyModel(
            @MaxMindDbIpAddress InetAddress lookupIp,
            @MaxMindDbNetwork Network lookupNetwork
        ) {
            this.lookupIp = lookupIp;
            this.lookupNetwork = lookupNetwork;
        }
    }

    static class WrapperContextOnlyModel {
        ContextOnlyModel context;

        @MaxMindDbConstructor
        public WrapperContextOnlyModel(
            @MaxMindDbParameter(name = "missing_context")
            ContextOnlyModel context
        ) {
            this.context = context;
        }
    }

    enum BooleanEnum {
        TRUE_VALUE,
        FALSE_VALUE,
        UNKNOWN;

        @MaxMindDbCreator
        public static BooleanEnum fromBoolean(Boolean b) {
            if (b == null) {
                return UNKNOWN;
            }
            return b ? TRUE_VALUE : FALSE_VALUE;
        }
    }

    enum StringEnum {
        HELLO("hello"),
        GOODBYE("goodbye"),
        UNKNOWN("unknown");

        private final String value;

        StringEnum(String value) {
            this.value = value;
        }

        @MaxMindDbCreator
        public static StringEnum fromString(String s) {
            if (s == null) {
                return UNKNOWN;
            }
            return switch (s) {
                case "hello" -> HELLO;
                case "goodbye" -> GOODBYE;
                default -> UNKNOWN;
            };
        }

        @Override
        public String toString() {
            return value;
        }
    }

    static class CreatorMethodModel {
        BooleanEnum enumField;

        @MaxMindDbConstructor
        public CreatorMethodModel(
            @MaxMindDbParameter(name = "boolean")
            BooleanEnum enumField
        ) {
            this.enumField = enumField;
        }
    }

    static class MapXWithEnum {
        StringEnum stringEnumField;

        @MaxMindDbConstructor
        public MapXWithEnum(
            @MaxMindDbParameter(name = "utf8_stringX")
            StringEnum stringEnumField
        ) {
            this.stringEnumField = stringEnumField;
        }
    }

    static class MapWithEnum {
        MapXWithEnum mapX;

        @MaxMindDbConstructor
        public MapWithEnum(
            @MaxMindDbParameter(name = "mapX")
            MapXWithEnum mapX
        ) {
            this.mapX = mapX;
        }
    }

    static class StringEnumModel {
        MapWithEnum map;

        @MaxMindDbConstructor
        public StringEnumModel(
            @MaxMindDbParameter(name = "map")
            MapWithEnum map
        ) {
            this.map = map;
        }
    }

    static class MapModelBoxed {
        MapXModelBoxed mapXField;

        @MaxMindDbConstructor
        public MapModelBoxed(
            @MaxMindDbParameter(name = "mapX")
            MapXModelBoxed mapXField
        ) {
            this.mapXField = mapXField;
        }
    }

    static class MapXModelBoxed {
        List<Long> arrayXField;
        String utf8StringXField;

        @MaxMindDbConstructor
        public MapXModelBoxed(
            @MaxMindDbParameter(name = "arrayX")
            List<Long> arrayXField,
            @MaxMindDbParameter(name = "utf8_stringX")
            String utf8StringXField
        ) {
            this.arrayXField = arrayXField;
            this.utf8StringXField = utf8StringXField;
        }
    }

    private void testDecodingTypesIntoModelWithList(Reader reader)
        throws IOException {
        var model = reader.get(InetAddress.getByName("::1.1.1.0"), TestModelList.class);

        assertEquals(List.of((long) 1, (long) 2, (long) 3), model.arrayField);
    }

    static class TestModelList {
        List<Long> arrayField;

        @MaxMindDbConstructor
        public TestModelList(
            @MaxMindDbParameter(name = "array") List<Long> arrayField
        ) {
            this.arrayField = arrayField;
        }
    }

    // Record-based decoding without annotations
    record MapXRecord(List<Long> arrayX) {}
    record MapRecord(MapXRecord mapX) {}
    record TestRecordImplicit(MapRecord map) {}

    private void testRecordImplicitConstructor(Reader reader) throws IOException {
        var model = reader.get(InetAddress.getByName("::1.1.1.0"), TestRecordImplicit.class);
        assertEquals(List.of(7L, 8L, 9L), model.map().mapX().arrayX());
    }

    // Single-constructor classes without @MaxMindDbConstructor
    static class MapXPojo {
        List<Long> arrayX;
        String utf8StringX;

        public MapXPojo(
            @MaxMindDbParameter(name = "arrayX") List<Long> arrayX,
            @MaxMindDbParameter(name = "utf8_stringX") String utf8StringX
        ) {
            this.arrayX = arrayX;
            this.utf8StringX = utf8StringX;
        }
    }

    static class MapContainerPojo {
        MapXPojo mapX;

        public MapContainerPojo(@MaxMindDbParameter(name = "mapX") MapXPojo mapX) {
            this.mapX = mapX;
        }
    }

    static class TopLevelPojo {
        MapContainerPojo map;

        public TopLevelPojo(@MaxMindDbParameter(name = "map") MapContainerPojo map) {
            this.map = map;
        }
    }

    private void testSingleConstructorWithoutAnnotation(Reader reader) throws IOException {
        var pojo = reader.get(InetAddress.getByName("::1.1.1.0"), TopLevelPojo.class);
        assertEquals(List.of(7L, 8L, 9L), pojo.map.mapX.arrayX);
    }

    // Unannotated parameters on non-record types using Java parameter names
    static class TestPojoImplicit {
        MapContainerPojo map;

        public TestPojoImplicit(MapContainerPojo map) {
            this.map = map;
        }
    }

    private void testPojoImplicitParameters(Reader reader) throws IOException {
        var model = reader.get(InetAddress.getByName("::1.1.1.0"), TestPojoImplicit.class);
        assertEquals(List.of(7L, 8L, 9L), model.map.mapX.arrayX);
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testZerosFile(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);
        this.testZeros(this.testReader);
        this.testZerosModelObject(this.testReader);
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testZerosStream(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);
        this.testZeros(this.testReader);
        this.testZerosModelObject(this.testReader);
    }

    private void testZeros(Reader reader) throws IOException {
        var record = reader.get(InetAddress.getByName("::"), Map.class);

        assertFalse((boolean) record.get("boolean"));

        assertArrayEquals(new byte[0], (byte[]) record.get("bytes"));

        assertEquals("", record.get("utf8_string"));

        var array = (List<?>) record.get("array");
        assertEquals(0, array.size());

        var map = (Map<?, ?>) record.get("map");
        assertEquals(0, map.size());

        assertEquals(0, (double) record.get("double"), 0.000000001);
        assertEquals(0, (float) record.get("float"), 0.000001);
        assertEquals(0, (int) record.get("int32"));
        assertEquals(0, (int) record.get("uint16"));
        assertEquals(0, (long) record.get("uint32"));
        assertEquals(BigInteger.ZERO, record.get("uint64"));
        assertEquals(BigInteger.ZERO, record.get("uint128"));
    }

    private void testZerosModelObject(Reader reader) throws IOException {
        var model = reader.get(InetAddress.getByName("::"), TestModel.class);

        assertFalse(model.booleanField);

        assertArrayEquals(new byte[0], model.bytesField);

        assertEquals("", model.utf8StringField);

        var expectedArray = new ArrayList<Long>();
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

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testDecodeSubdivisions(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("GeoIP2-City-Test.mmdb"), chunkSize);

        var model = this.testReader.get(
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
        public TestModelSubdivisions(
            @MaxMindDbParameter(name = "subdivisions")
            List<TestModelSubdivision> subdivisions
        ) {
            this.subdivisions = subdivisions;
        }
    }

    static class TestModelSubdivision {
        String isoCode;

        @MaxMindDbConstructor
        public TestModelSubdivision(
            @MaxMindDbParameter(name = "iso_code")
            String isoCode
        ) {
            this.isoCode = isoCode;
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testDecodeWrongTypeWithConstructorException(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("GeoIP2-City-Test.mmdb"), chunkSize);
        var ex = assertThrows(DeserializationException.class,
            () -> this.testReader.get(InetAddress.getByName("2.125.160.216"),
                TestModelSubdivisionsWithUnknownException.class));

        assertThat(ex.getMessage(),
            containsString("Error getting record for IP /2.125.160.216 -  Error creating object"));
    }

    static class TestModelSubdivisionsWithUnknownException {
        List<TestModelSubdivision> subdivisions;

        @MaxMindDbConstructor
        public TestModelSubdivisionsWithUnknownException(
            @MaxMindDbParameter(name = "subdivisions")
            List<TestModelSubdivision> subdivisions
        ) throws Exception {
            throw new Exception();
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testDecodeWrongTypeWithWrongArguments(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("GeoIP2-City-Test.mmdb"), chunkSize);
        var ex = assertThrows(DeserializationException.class,
            () -> this.testReader.get(InetAddress.getByName("2.125.160.216"),
                TestWrongModelSubdivisions.class));
        assertThat(ex.getMessage(), containsString("Error getting record for IP"));
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testDecodeWithDataTypeMismatchInModel(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("GeoIP2-City-Test.mmdb"), chunkSize);
        var ex = assertThrows(DeserializationException.class,
                () -> this.testReader.get(InetAddress.getByName("2.125.160.216"),
                        TestDataTypeMismatchInModel.class));
        assertThat(ex.getMessage(), containsString("Error getting record for IP"));
        assertThat(ex.getMessage(), containsString("Error creating map entry for"));
        assertThat(ex.getCause().getCause().getClass(), equalTo(ClassCastException.class));
    }


    static class TestConstructorMismatchModel {
        @MaxMindDbConstructor
        public TestConstructorMismatchModel(
            @MaxMindDbParameter(name = "other")
            String other,
            @MaxMindDbParameter(name = "utf8_string")
            double utf8StringField
        ) {
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testDecodeWithDataTypeMismatchInModelAndNullValue(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);

        var ex = assertThrows(DeserializationException.class,
            () -> this.testReader.get(
                InetAddress.getByName("::1.1.1.0"),
                TestConstructorMismatchModel.class));

        assertThat(ex.getMessage(), containsString("Error creating object of type"));
        assertThat(ex.getCause().getCause().getClass(), equalTo(IllegalArgumentException.class));
    }

    static class TestWrongModelSubdivisions {
        List<TestWrongModelSubdivision> subdivisions;

        @MaxMindDbConstructor
        public TestWrongModelSubdivisions(
            @MaxMindDbParameter(name = "subdivisions")
            List<TestWrongModelSubdivision> subdivisions
        ) {
            this.subdivisions = subdivisions;
        }
    }

    static class TestWrongModelSubdivision {
        Integer uint16Field;

        @MaxMindDbConstructor
        public TestWrongModelSubdivision(
            @MaxMindDbParameter(name = "iso_code")
            Integer uint16Field
        ) {
            this.uint16Field = uint16Field;
        }
    }

    static class TestDataTypeMismatchInModel {
        Map<String, Float> location;

        @MaxMindDbConstructor
        public TestDataTypeMismatchInModel(
                @MaxMindDbParameter(name = "location")
                Map<String, Float> location
        ) {
            this.location = location;
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testDecodeConcurrentHashMap(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("GeoIP2-City-Test.mmdb"), chunkSize);

        var m = this.testReader.get(
            InetAddress.getByName("2.125.160.216"),
            ConcurrentHashMap.class
        );

        var subdivisions = (List<?>) m.get("subdivisions");

        var eng = (Map<?, ?>) subdivisions.get(0);

        var isoCode = (String) eng.get("iso_code");
        assertEquals("ENG", isoCode);
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testDecodeVector(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);

        var model = this.testReader.get(
            InetAddress.getByName("::1.1.1.0"),
            TestModelVector.class
        );

        assertEquals(3, model.arrayField.size());
        assertEquals(1, (long) model.arrayField.get(0));
        assertEquals(2, (long) model.arrayField.get(1));
        assertEquals(3, (long) model.arrayField.get(2));
    }

    static class TestModelVector {
        ArrayList<Long> arrayField;

        @MaxMindDbConstructor
        public TestModelVector(
            @MaxMindDbParameter(name = "array")
            ArrayList<Long> arrayField
        ) {
        this.arrayField = arrayField;
        }
    }

    // Positive tests for primitive constructor parameters
    static class TestModelPrimitivesBasic {
        boolean booleanField;
        double doubleField;
        float floatField;
        int int32Field;
        long uint32Field;

        @MaxMindDbConstructor
        public TestModelPrimitivesBasic(
            @MaxMindDbParameter(name = "boolean") boolean booleanField,
            @MaxMindDbParameter(name = "double") double doubleField,
            @MaxMindDbParameter(name = "float") float floatField,
            @MaxMindDbParameter(name = "int32") int int32Field,
            @MaxMindDbParameter(name = "uint32") long uint32Field
        ) {
            this.booleanField = booleanField;
            this.doubleField = doubleField;
            this.floatField = floatField;
            this.int32Field = int32Field;
            this.uint32Field = uint32Field;
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testPrimitiveConstructorParamsBasicWorks(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);

        var model = this.testReader.get(
            InetAddress.getByName("::1.1.1.0"),
            TestModelPrimitivesBasic.class
        );

        assertTrue(model.booleanField);
        assertEquals(42.123456, model.doubleField, 0.000000001);
        assertEquals(1.1, model.floatField, 0.000001);
        assertEquals(-268435456, model.int32Field);
        assertEquals(268435456L, model.uint32Field);
    }

    static class TestModelShortPrimitive {
        short uint16Field;

        @MaxMindDbConstructor
        public TestModelShortPrimitive(
            @MaxMindDbParameter(name = "uint16") short uint16Field
        ) {
            this.uint16Field = uint16Field;
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testPrimitiveConstructorParamShortWorks(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);
        var model = this.testReader.get(
            InetAddress.getByName("::1.1.1.0"),
            TestModelShortPrimitive.class
        );
        assertEquals((short) 100, model.uint16Field);
    }

    static class TestModelBytePrimitive {
        byte uint16Field;

        @MaxMindDbConstructor
        public TestModelBytePrimitive(
            @MaxMindDbParameter(name = "uint16") byte uint16Field
        ) {
            this.uint16Field = uint16Field;
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testPrimitiveConstructorParamByteWorks(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);
        var model = this.testReader.get(
            InetAddress.getByName("::1.1.1.0"),
            TestModelBytePrimitive.class
        );
        assertEquals((byte) 100, model.uint16Field);
    }

    // Tests for behavior when a primitive constructor parameter is missing from the DB
    static class MissingBooleanPrimitive {
        boolean v;

        @MaxMindDbConstructor
        public MissingBooleanPrimitive(
            @MaxMindDbParameter(name = "missing_key") boolean v
        ) {
            this.v = v;
        }
    }

    static class MissingBytePrimitive {
        byte v;

        @MaxMindDbConstructor
        public MissingBytePrimitive(
            @MaxMindDbParameter(name = "missing_key") byte v
        ) {
            this.v = v;
        }
    }

    static class MissingShortPrimitive {
        short v;

        @MaxMindDbConstructor
        public MissingShortPrimitive(
            @MaxMindDbParameter(name = "missing_key") short v
        ) {
            this.v = v;
        }
    }

    static class MissingIntPrimitive {
        int v;

        @MaxMindDbConstructor
        public MissingIntPrimitive(
            @MaxMindDbParameter(name = "missing_key") int v
        ) {
            this.v = v;
        }
    }

    static class MissingLongPrimitive {
        long v;

        @MaxMindDbConstructor
        public MissingLongPrimitive(
            @MaxMindDbParameter(name = "missing_key") long v
        ) {
            this.v = v;
        }
    }

    static class MissingFloatPrimitive {
        float v;

        @MaxMindDbConstructor
        public MissingFloatPrimitive(
            @MaxMindDbParameter(name = "missing_key") float v
        ) {
            this.v = v;
        }
    }

    static class MissingDoublePrimitive {
        double v;

        @MaxMindDbConstructor
        public MissingDoublePrimitive(
            @MaxMindDbParameter(name = "missing_key") double v
        ) {
            this.v = v;
        }
    }

    // Positive tests: defaults via annotation when key is missing
    static class DefaultBooleanPrimitive {
        boolean v;

        @MaxMindDbConstructor
        public DefaultBooleanPrimitive(
            @MaxMindDbParameter(name = "missing_key", useDefault = true, defaultValue = "true")
            boolean v
        ) {
            this.v = v;
        }
    }

    static class DefaultBytePrimitive {
        byte v;

        @MaxMindDbConstructor
        public DefaultBytePrimitive(
            @MaxMindDbParameter(name = "missing_key", useDefault = true, defaultValue = "7")
            byte v
        ) {
            this.v = v;
        }
    }

    static class DefaultShortPrimitive {
        short v;

        @MaxMindDbConstructor
        public DefaultShortPrimitive(
            @MaxMindDbParameter(name = "missing_key", useDefault = true, defaultValue = "300")
            short v
        ) {
            this.v = v;
        }
    }

    static class DefaultIntPrimitive {
        int v;

        @MaxMindDbConstructor
        public DefaultIntPrimitive(
            @MaxMindDbParameter(name = "missing_key", useDefault = true, defaultValue = "-5")
            int v
        ) {
            this.v = v;
        }
    }

    static class DefaultLongPrimitive {
        long v;

        @MaxMindDbConstructor
        public DefaultLongPrimitive(
            @MaxMindDbParameter(name = "missing_key", useDefault = true, defaultValue = "123456789")
            long v
        ) {
            this.v = v;
        }
    }

    static class DefaultFloatPrimitive {
        float v;

        @MaxMindDbConstructor
        public DefaultFloatPrimitive(
            @MaxMindDbParameter(name = "missing_key", useDefault = true, defaultValue = "3.14")
            float v
        ) {
            this.v = v;
        }
    }

    static class DefaultDoublePrimitive {
        double v;

        @MaxMindDbConstructor
        public DefaultDoublePrimitive(
            @MaxMindDbParameter(name = "missing_key", useDefault = true, defaultValue = "2.71828")
            double v
        ) {
            this.v = v;
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testMissingPrimitiveDefaultsApplied(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);

        assertTrue(this.testReader.get(
            InetAddress.getByName("::1.1.1.0"), DefaultBooleanPrimitive.class).v);
        assertEquals((byte) 7, this.testReader.get(
            InetAddress.getByName("::1.1.1.0"), DefaultBytePrimitive.class).v);
        assertEquals((short) 300, this.testReader.get(
            InetAddress.getByName("::1.1.1.0"), DefaultShortPrimitive.class).v);
        assertEquals(-5, this.testReader.get(
            InetAddress.getByName("::1.1.1.0"), DefaultIntPrimitive.class).v);
        assertEquals(123456789L, this.testReader.get(
            InetAddress.getByName("::1.1.1.0"), DefaultLongPrimitive.class).v);
        assertEquals(3.14f, this.testReader.get(
            InetAddress.getByName("::1.1.1.0"), DefaultFloatPrimitive.class).v, 0.0001);
        assertEquals(2.71828, this.testReader.get(
            InetAddress.getByName("::1.1.1.0"), DefaultDoublePrimitive.class).v, 0.00001);
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testMissingPrimitiveBooleanFails(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);
        var ex = assertThrows(DeserializationException.class,
            () -> this.testReader.get(InetAddress.getByName("::1.1.1.0"), MissingBooleanPrimitive.class));
        assertThat(ex.getMessage(), containsString("Error creating object"));
        assertThat(ex.getCause().getCause().getClass(), equalTo(IllegalArgumentException.class));
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testMissingPrimitiveByteFails(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);
        var ex = assertThrows(DeserializationException.class,
            () -> this.testReader.get(InetAddress.getByName("::1.1.1.0"), MissingBytePrimitive.class));
        assertThat(ex.getMessage(), containsString("Error creating object"));
        assertThat(ex.getCause().getCause().getClass(), equalTo(IllegalArgumentException.class));
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testMissingPrimitiveShortFails(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);
        var ex = assertThrows(DeserializationException.class,
            () -> this.testReader.get(InetAddress.getByName("::1.1.1.0"), MissingShortPrimitive.class));
        assertThat(ex.getMessage(), containsString("Error creating object"));
        assertThat(ex.getCause().getCause().getClass(), equalTo(IllegalArgumentException.class));
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testMissingPrimitiveIntFails(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);
        var ex = assertThrows(DeserializationException.class,
            () -> this.testReader.get(InetAddress.getByName("::1.1.1.0"), MissingIntPrimitive.class));
        assertThat(ex.getMessage(), containsString("Error creating object"));
        assertThat(ex.getCause().getCause().getClass(), equalTo(IllegalArgumentException.class));
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testMissingPrimitiveLongFails(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);
        var ex = assertThrows(DeserializationException.class,
            () -> this.testReader.get(InetAddress.getByName("::1.1.1.0"), MissingLongPrimitive.class));
        assertThat(ex.getMessage(), containsString("Error creating object"));
        assertThat(ex.getCause().getCause().getClass(), equalTo(IllegalArgumentException.class));
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testMissingPrimitiveFloatFails(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);
        var ex = assertThrows(DeserializationException.class,
            () -> this.testReader.get(InetAddress.getByName("::1.1.1.0"), MissingFloatPrimitive.class));
        assertThat(ex.getMessage(), containsString("Error creating object"));
        assertThat(ex.getCause().getCause().getClass(), equalTo(IllegalArgumentException.class));
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testMissingPrimitiveDoubleFails(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);
        var ex = assertThrows(DeserializationException.class,
            () -> this.testReader.get(InetAddress.getByName("::1.1.1.0"), MissingDoublePrimitive.class));
        assertThat(ex.getMessage(), containsString("Error creating object"));
        assertThat(ex.getCause().getCause().getClass(), equalTo(IllegalArgumentException.class));
    }

    // Test that we cache differently depending on more than the offset.
    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testCacheWithDifferentModels(int chunkSize) throws IOException {
        var cache = new CHMCache();

        this.testReader = new Reader(
            getFile("MaxMind-DB-test-decoder.mmdb"),
            cache,
            chunkSize
        );

        var modelA = this.testReader.get(
            InetAddress.getByName("::1.1.1.0"),
            TestModelA.class
        );
        assertEquals("unicode! ☯ - ♫", modelA.utf8StringFieldA);

        var modelB = this.testReader.get(
            InetAddress.getByName("::1.1.1.0"),
            TestModelB.class
        );
        assertEquals("unicode! ☯ - ♫", modelB.utf8StringFieldB);
    }

    static class TestModelA {
        String utf8StringFieldA;

        @MaxMindDbConstructor
        public TestModelA(
            @MaxMindDbParameter(name = "utf8_string") String utf8StringFieldA
        ) {
            this.utf8StringFieldA = utf8StringFieldA;
        }
    }

    static class TestModelB {
        String utf8StringFieldB;

        @MaxMindDbConstructor
        public TestModelB(
            @MaxMindDbParameter(name = "utf8_string") String utf8StringFieldB
        ) {
            this.utf8StringFieldB = utf8StringFieldB;
        }
    }

    @Test
    public void testCacheKey() {
        var cls = TestModelCacheKey.class;

        var a = new CacheKey<>(1, cls, getType(cls, 0));
        var b = new CacheKey<>(1, cls, getType(cls, 0));
        assertEquals(a, b);

        var c = new CacheKey<>(2, cls, getType(cls, 0));
        assertNotEquals(a, c);

        var d = new CacheKey<>(1, String.class, getType(cls, 0));
        assertNotEquals(a, d);

        var e = new CacheKey<>(1, cls, getType(cls, 1));
        assertNotEquals(a, e);
    }

    private <T> java.lang.reflect.Type getType(Class<T> cls, int i) {
        var constructors = cls.getConstructors();
        Constructor<TestModelCacheKey> constructor = null;
        for (var constructor2 : constructors) {
            constructor = (Constructor<TestModelCacheKey>) constructor2;
            break;
        }
        assertNotNull(constructor);

        var types = constructor.getGenericParameterTypes();
        return types[i];
    }

    static class TestModelCacheKey {
        private final List<Long> a;
        private final List<Integer> b;

        public TestModelCacheKey(List<Long> a, List<Integer> b) {
            this.a = a;
            this.b = b;
        }
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testBrokenDatabaseFile(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("GeoIP2-City-Test-Broken-Double-Format.mmdb"), chunkSize);
        this.testBrokenDatabase(this.testReader);
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testBrokenDatabaseStream(int chunkSize) throws IOException {
        this.testReader = new Reader(getStream("GeoIP2-City-Test-Broken-Double-Format.mmdb"), chunkSize);
        this.testBrokenDatabase(this.testReader);
    }

    private void testBrokenDatabase(Reader reader) {
        var ex = assertThrows(
            InvalidDatabaseException.class,
            () -> reader.get(InetAddress.getByName("2001:220::"), Map.class));
        assertThat(ex.getMessage(),
            containsString("The MaxMind DB file's data section contains bad data"));
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testBrokenSearchTreePointerFile(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-broken-pointers-24.mmdb"), chunkSize);
        this.testBrokenSearchTreePointer(this.testReader);
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testBrokenSearchTreePointerStream(int chunkSize) throws IOException {
        this.testReader = new Reader(getStream("MaxMind-DB-test-broken-pointers-24.mmdb"), chunkSize);
        this.testBrokenSearchTreePointer(this.testReader);
    }

    private void testBrokenSearchTreePointer(Reader reader) {
        var ex = assertThrows(InvalidDatabaseException.class,
            () -> reader.get(InetAddress.getByName("1.1.1.32"), Map.class));
        assertThat(ex.getMessage(), containsString("The MaxMind DB file's search tree is corrupt"));
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testBrokenDataPointerFile(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("MaxMind-DB-test-broken-pointers-24.mmdb"), chunkSize);
        this.testBrokenDataPointer(this.testReader);
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testBrokenDataPointerStream(int chunkSize) throws IOException {
        this.testReader = new Reader(getStream("MaxMind-DB-test-broken-pointers-24.mmdb"), chunkSize);
        this.testBrokenDataPointer(this.testReader);
    }

    private void testBrokenDataPointer(Reader reader) {
        var ex = assertThrows(InvalidDatabaseException.class,
            () -> reader.get(InetAddress.getByName("1.1.1.16"), Map.class));
        assertThat(ex.getMessage(),
            containsString("The MaxMind DB file's data section contains bad data"));
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testClosedReaderThrowsException(int chunkSize) throws IOException {
        var reader = new Reader(getFile("MaxMind-DB-test-decoder.mmdb"), chunkSize);

        reader.close();
        var ex = assertThrows(ClosedDatabaseException.class,
            () -> reader.get(InetAddress.getByName("1.1.1.16"), Map.class));
        assertEquals("The MaxMind DB has been closed.", ex.getMessage());
    }

    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void voidTestMapKeyIsString(int chunkSize) throws IOException {
        this.testReader = new Reader(getFile("GeoIP2-City-Test.mmdb"), chunkSize);

        var ex = assertThrows(
            DeserializationException.class,
            () -> this.testReader.get(
                InetAddress.getByName("2.125.160.216"),
                TestModelInvalidMap.class
            )
        );
        assertEquals("Error getting record for IP /2.125.160.216 -  Map keys must be strings.",
            ex.getMessage());
    }

    static class TestModelInvalidMap {
        Map<Integer, String> postal;

        @MaxMindDbConstructor
        public TestModelInvalidMap(
            @MaxMindDbParameter(name = "postal")
            Map<Integer, String> postal
        ) {
            this.postal = postal;
        }
    }

    private void testMetadata(Reader reader, int ipVersion, long recordSize) {

        var metadata = reader.getMetadata();

        assertEquals(2, metadata.binaryFormatMajorVersion(), "major version");
        assertEquals(0, metadata.binaryFormatMinorVersion());
        assertEquals(ipVersion, metadata.ipVersion());
        assertEquals("Test", metadata.databaseType());

        var languages = new ArrayList<>(List.of("en", "zh"));

        assertEquals(languages, metadata.languages());

        var description = new HashMap<String, String>();
        description.put("en", "Test Database");
        description.put("zh", "Test Database Chinese");

        assertEquals(description, metadata.description());
        assertEquals(recordSize, metadata.recordSize());

        var january2014 = LocalDate.of(2014, 1, 1)
                .atStartOfDay(ZoneOffset.UTC)
                .toInstant();

        assertTrue(metadata.buildTime().isAfter(january2014));
    }

    private void testIpV4(Reader reader, File file) throws IOException {

        for (int i = 0; i <= 5; i++) {
            var address = "1.1.1." + (int) Math.pow(2, i);
            var data = new HashMap<String, String>();
            data.put("ip", address);

            assertEquals(
                data,
                reader.get(InetAddress.getByName(address), Map.class),
                "found expected data record for " + address + " in " + file
            );
        }

        var pairs = new HashMap<String, String>();
        pairs.put("1.1.1.3", "1.1.1.2");
        pairs.put("1.1.1.5", "1.1.1.4");
        pairs.put("1.1.1.7", "1.1.1.4");
        pairs.put("1.1.1.9", "1.1.1.8");
        pairs.put("1.1.1.15", "1.1.1.8");
        pairs.put("1.1.1.17", "1.1.1.16");
        pairs.put("1.1.1.31", "1.1.1.16");
        for (String address : pairs.keySet()) {
            var data = new HashMap<String, String>();
            data.put("ip", pairs.get(address));

            assertEquals(
                data,
                reader.get(InetAddress.getByName(address), Map.class),
                "found expected data record for " + address + " in " + file
            );
        }

        for (String ip : new String[] {"1.1.1.33", "255.254.253.123"}) {
            assertNull(reader.get(InetAddress.getByName(ip), Map.class));
        }
    }

    private void testIpV6(Reader reader, File file) throws IOException {
        var subnets = new String[] {"::1:ffff:ffff", "::2:0:0",
            "::2:0:40", "::2:0:50", "::2:0:58"};

        for (String address : subnets) {
            var data = new HashMap<String, String>();
            data.put("ip", address);

            assertEquals(
                data,
                reader.get(InetAddress.getByName(address), Map.class),
                "found expected data record for " + address + " in " + file
            );
        }

        var pairs = new HashMap<String, String>();
        pairs.put("::2:0:1", "::2:0:0");
        pairs.put("::2:0:33", "::2:0:0");
        pairs.put("::2:0:39", "::2:0:0");
        pairs.put("::2:0:41", "::2:0:40");
        pairs.put("::2:0:49", "::2:0:40");
        pairs.put("::2:0:52", "::2:0:50");
        pairs.put("::2:0:57", "::2:0:50");
        pairs.put("::2:0:59", "::2:0:58");

        for (String address : pairs.keySet()) {
            var data = new HashMap<String, String>();
            data.put("ip", pairs.get(address));

            assertEquals(
                data,
                reader.get(InetAddress.getByName(address), Map.class),
                "found expected data record for " + address + " in " + file
            );
        }

        for (String ip : new String[] {"1.1.1.33", "255.254.253.123", "89fa::"}) {
            assertNull(reader.get(InetAddress.getByName(ip), Map.class));
        }
    }

    // =========================================================================
    // Tests for enum with @MaxMindDbCreator when data is stored via pointer
    // See: https://github.com/maxmind/GeoIP2-java/issues/644
    // =========================================================================

    /**
     * Enum with @MaxMindDbCreator for converting string values.
     * This simulates how ConnectionType enum works in geoip2.
     */
    enum ConnectionTypeEnum {
        DIALUP("Dialup"),
        CABLE_DSL("Cable/DSL"),
        CORPORATE("Corporate"),
        CELLULAR("Cellular"),
        SATELLITE("Satellite"),
        UNKNOWN("Unknown");

        private final String name;

        ConnectionTypeEnum(String name) {
            this.name = name;
        }

        @MaxMindDbCreator
        public static ConnectionTypeEnum fromString(String s) {
            if (s == null) {
                return UNKNOWN;
            }
            return switch (s) {
                case "Dialup" -> DIALUP;
                case "Cable/DSL" -> CABLE_DSL;
                case "Corporate" -> CORPORATE;
                case "Cellular" -> CELLULAR;
                case "Satellite" -> SATELLITE;
                default -> UNKNOWN;
            };
        }
    }

    /**
     * Model class that uses the ConnectionTypeEnum for the connection_type field.
     */
    static class TraitsModel {
        ConnectionTypeEnum connectionType;

        @MaxMindDbConstructor
        public TraitsModel(
            @MaxMindDbParameter(name = "connection_type")
            ConnectionTypeEnum connectionType
        ) {
            this.connectionType = connectionType;
        }
    }

    /**
     * Top-level model for Enterprise database records.
     */
    static class EnterpriseModel {
        TraitsModel traits;

        @MaxMindDbConstructor
        public EnterpriseModel(
            @MaxMindDbParameter(name = "traits")
            TraitsModel traits
        ) {
            this.traits = traits;
        }
    }

    /**
     * This test passes because IP 74.209.24.0 has connection_type stored inline.
     */
    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testEnumCreatorWithInlineData(int chunkSize) throws IOException {
        try (var reader = new Reader(getFile("GeoIP2-Enterprise-Test.mmdb"), chunkSize)) {
            var ip = InetAddress.getByName("74.209.24.0");
            var result = reader.get(ip, EnterpriseModel.class);
            assertNotNull(result);
            assertNotNull(result.traits);
            assertEquals(ConnectionTypeEnum.CABLE_DSL, result.traits.connectionType);
        }
    }

    /**
     * This test verifies that enums with @MaxMindDbCreator work correctly when
     * the data is stored via a pointer (common for deduplication in databases).
     *
     * <p>Previously, this would throw ConstructorNotFoundException because
     * requiresLookupContext() called loadConstructorMetadata() before checking
     * for creator methods.
     */
    @ParameterizedTest
    @MethodSource("chunkSizes")
    public void testEnumCreatorWithPointerData(int chunkSize) throws IOException {
        try (var reader = new Reader(getFile("GeoIP2-Enterprise-Test.mmdb"), chunkSize)) {
            var ip = InetAddress.getByName("89.160.20.112");
            var result = reader.get(ip, EnterpriseModel.class);
            assertNotNull(result);
            assertNotNull(result.traits);
            assertEquals(ConnectionTypeEnum.CORPORATE, result.traits.connectionType);
        }
    }

    static File getFile(String name) {
        return new File(ReaderTest.class.getResource("/maxmind-db/test-data/" + name).getFile());
    }

    static InputStream getStream(String name) {
        return ReaderTest.class.getResourceAsStream("/maxmind-db/test-data/" + name);
    }
}
