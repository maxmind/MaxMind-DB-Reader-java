package com.maxmind.db;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.BiFunction;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;

import com.maxmind.db.Callbacks.*;

public class NewReaderTest {
    private final ObjectMapper om = new ObjectMapper();

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
    public void testDecodingTypesFile() throws IOException {
        CallbackReader testReader = new CallbackReader(getFile("MaxMind-DB-test-decoder.mmdb"));
        this.testDecodingTypes(testReader);
    }

    @Test
    public void testDecodingTypesStream() throws IOException {
        CallbackReader testReader = new CallbackReader(getStream("MaxMind-DB-test-decoder.mmdb"));
        this.testDecodingTypes(testReader);
    }

    private void testDecodingTypes(CallbackReader reader) throws IOException {
	final AllocMeter allocMeter = new AllocMeter();
	InetAddress key = InetAddress.getByName("::1.1.1.0");
	byte[] rawKey = key.getAddress();

	BiFunction<String, RecordCallbackBuilder<AccumulatorForTypes>, AccumulatorForTypes> runIt = new BiFunction<String, RecordCallbackBuilder<AccumulatorForTypes>, AccumulatorForTypes>() {
		public AccumulatorForTypes apply(String runDescription, RecordCallbackBuilder<AccumulatorForTypes> builder) {
		    RecordCallback<AccumulatorForTypes> callback = builder.build();
		    AccumulatorForTypes acc = new AccumulatorForTypes();
		    try {
			long a0 = allocMeter.allocByteCount();
			reader.lookupRecord(rawKey, callback, acc);
			long a1 = allocMeter.allocByteCount();

			long allocatedBytes = a1 - a0;
			//System.out.println("Allocation by lookup call for '" + runDescription + "': "+ allocatedBytes);

			if (!"Warmup".equals(runDescription)) {
			    assertTrue("Zero-allocation broken by '"+runDescription+"': allocatedBytes = "+allocatedBytes,
				       allocatedBytes == 0);
			}
		    } catch (IOException ioe) {
			throw new RuntimeException(ioe);
		    }
		    return acc;
		}
	    };

	// Warm up the reader:
	runIt.apply("Warmup", new RecordCallbackBuilder<AccumulatorForTypes>());

	{ // Strings:
	    RecordCallbackBuilder<AccumulatorForTypes> builder = new RecordCallbackBuilder<>();
	    builder.text("utf8_string", (AccumulatorForTypes state, CharSequence value) -> TextNode.assignToStringBuilder(state.string1, value));
	    AccumulatorForTypes result = runIt.apply("String value", builder);
	    assertEquals("unicode! ☯ - ♫", result.string1.toString());
	}

	{ // Floating-point numbers:
	    RecordCallbackBuilder<AccumulatorForTypes> builder = new RecordCallbackBuilder<>();
	    builder.number("double", (AccumulatorForTypes state, double value) -> state.double1 = value);
	    builder.number("float", (AccumulatorForTypes state, double value) -> state.double2 = value);
	    AccumulatorForTypes result = runIt.apply("Number values", builder);
	    assertEquals(42.123456, result.double1, 0.000000001);
	    assertEquals(1.1, result.double2, 0.000001);
	}

	{ // Nested records:
	    RecordCallbackBuilder<AccumulatorForTypes> builder = new RecordCallbackBuilder<>();
	    builder.obj("map").obj("mapX").text("utf8_stringX", (AccumulatorForTypes state, CharSequence value) -> TextNode.assignToStringBuilder(state.string1, value));
	    AccumulatorForTypes result = runIt.apply("Value in nested records", builder);
	    assertEquals("hello", result.string1.toString());
	}

	// Node type not handled yet: "boolean"
	// Node type not handled yet: "bytes"
	// Node type not handled yet: "array"
	// Node type not handled yet: "map" - existence signal
	// Node type not handled yet: "int"
    }

    static class AccumulatorForTypes {
	StringBuilder string1 = new StringBuilder(1000);
	double double1 = Double.NaN;
	double double2 = Double.NaN;
	{
	    string1.append("test\u1010").setLength(0); // Ensure buffer is primed for non-Latin1 content.
	}
    }

    @Test
    public void testGetRecord() throws IOException {
        GetRecordTest[] tests = {
	    new GetRecordTest("8.8.8.8", "ip-db-0cf0e7a0b9649404168f52a0c8be57c9.mmdb", "8.8.0.0/17", true), //ERK
	    new GetRecordTest("85.191.80.236",  "ip-db-0cf0e7a0b9649404168f52a0c8be57c9.mmdb", "85.191.80.0/21", true), //ERK
	    /*
                new GetRecordTest("1.1.1.1", "MaxMind-DB-test-ipv6-32.mmdb", "1.0.0.0/8", false),
                new GetRecordTest("::1:ffff:ffff", "MaxMind-DB-test-ipv6-24.mmdb", "0:0:0:0:0:1:ffff:ffff/128", true),
                new GetRecordTest("::2:0:1", "MaxMind-DB-test-ipv6-24.mmdb", "0:0:0:0:0:2:0:0/122", true),
                new GetRecordTest("1.1.1.1", "MaxMind-DB-test-ipv4-24.mmdb", "1.1.1.1/32", true),
                new GetRecordTest("1.1.1.3", "MaxMind-DB-test-ipv4-24.mmdb", "1.1.1.2/31", true),
                new GetRecordTest("1.1.1.3", "MaxMind-DB-test-decoder.mmdb", "1.1.1.0/24", true),
                new GetRecordTest("::ffff:1.1.1.128", "MaxMind-DB-test-decoder.mmdb", "1.1.1.0/24", true),
                new GetRecordTest("::1.1.1.128", "MaxMind-DB-test-decoder.mmdb", "0:0:0:0:0:0:101:100/120", true),
                // new GetRecordTest("200.0.2.1", "MaxMind-DB-no-ipv4-search-tree.mmdb", "0.0.0.0/0", true),
                // new GetRecordTest("::200.0.2.1", "MaxMind-DB-no-ipv4-search-tree.mmdb", "0:0:0:0:0:0:0:0/64", true),
                // new GetRecordTest("0:0:0:0:ffff:ffff:ffff:ffff", "MaxMind-DB-no-ipv4-search-tree.mmdb", "0:0:0:0:0:0:0:0/64", true),
                new GetRecordTest("ef00::", "MaxMind-DB-no-ipv4-search-tree.mmdb", "8000:0:0:0:0:0:0:0/1", false)
	    */
        };

	// Continent readout:
	Callbacks.TextNode continentTextNode = new Callbacks.TextNode<Accumulator>() {
		@Override public void setValue(Accumulator state, CharSequence value) {
		    TextNode.assignToStringBuilder(state.continent, value);
		}
	    };
	Map<String, Callbacks.Callback<Accumulator>> continentNamesMap = new HashMap();
	continentNamesMap.put("en", continentTextNode);
	Callbacks.ObjectNode<Accumulator> continentNamesNode = new Callbacks.ObjectNode(continentNamesMap);
	Map<String, Callbacks.Callback<Accumulator>> continentMap = new HashMap();
	continentMap.put("names", continentNamesNode);
	Callbacks.ObjectNode<Accumulator> continentNode = new Callbacks.ObjectNode(continentMap);

	// Country readout:
	Callbacks.TextNode countryTextNode = new Callbacks.TextNode<Accumulator>() {
		@Override public void setValue(Accumulator state, CharSequence value) {
		    TextNode.assignToStringBuilder(state.country, value);
		}
	    };
	Map<String, Callbacks.Callback<Accumulator>> countryNamesMap = new HashMap();
	countryNamesMap.put("en", countryTextNode);
	Callbacks.ObjectNode<Accumulator> countryNamesNode = new Callbacks.ObjectNode(countryNamesMap);
	Map<String, Callbacks.Callback<Accumulator>> countryMap = new HashMap();
	countryMap.put("names", countryNamesNode);
	Callbacks.ObjectNode<Accumulator> countryNode = new Callbacks.ObjectNode(countryMap);

	// City readout:
	Callbacks.TextNode cityTextNode = new Callbacks.TextNode<Accumulator>() {
		@Override public void setValue(Accumulator state, CharSequence value) {
		    TextNode.assignToStringBuilder(state.city, value);
		}
	    };
	Map<String, Callbacks.Callback<Accumulator>> cityNamesMap = new HashMap();
	cityNamesMap.put("en", cityTextNode);
	Callbacks.ObjectNode<Accumulator> cityNamesNode = new Callbacks.ObjectNode(cityNamesMap);
	Map<String, Callbacks.Callback<Accumulator>> cityMap = new HashMap();
	cityMap.put("names", cityNamesNode);
	Callbacks.ObjectNode<Accumulator> cityNode = new Callbacks.ObjectNode(cityMap);

	// Position readout:
	Callbacks.DoubleNode latitudeNode = new Callbacks.DoubleNode<Accumulator>() {
		@Override public void setValue(Accumulator state, double value) {
		    state.latitude = value;
		}
	    };
	Callbacks.DoubleNode longitudeNode = new Callbacks.DoubleNode<Accumulator>() {
		@Override public void setValue(Accumulator state, double value) {
		    state.longitude = value;
		}
	    };
	Map<String, Callbacks.Callback<Accumulator>> locationMap = new HashMap();
	locationMap.put("latitude", latitudeNode);
	locationMap.put("longitude", longitudeNode);
	Callbacks.ObjectNode<Accumulator> locationNode = new Callbacks.ObjectNode(locationMap);

	Map<String, Callbacks.Callback<Accumulator>> rootFieldMap = new HashMap();
	rootFieldMap.put("continent", continentNode);
	rootFieldMap.put("country", countryNode);
	rootFieldMap.put("city", cityNode);
	rootFieldMap.put("location", locationNode);
	Callbacks.RecordCallback<Accumulator> callback = new Callbacks.RecordCallback<Accumulator>(rootFieldMap) {
		@Override
		public void network(Accumulator state, byte[] ipAddress, int prefixLength) {
		    state.ipAddress = ipAddress;
		    state.prefixLength = prefixLength;
		}

		@Override
		public void objectBegin(Accumulator state) {
		    state.recordFound = true;
		}
	    };

	System.out.println("ERK| In test - without cache");
        for (GetRecordTest test : tests) {
            try (CallbackReader reader = new CallbackReader(test.db, NoCache.getInstance())) {

		System.out.println("ERK| Test case: " + test.ip +" in " + test.db);
		Accumulator acc = new Accumulator();
		for (int i=1; i<=3; i++) {
		    byte[] rawAddress = test.ip.getAddress();
		    long a0 = ErikUtil.allocCount();
		    reader.lookupRecord(rawAddress, callback, acc);
		    long a1 = ErikUtil.allocCount();
		    System.out.println("ERK| Read #"+i+": alloc=" + (a1-a0));
		    System.out.println("ERK| - Continent: " + acc.continent);
		    System.out.println("ERK| - Country: " + acc.country);
		    System.out.println("ERK| - City: " + acc.city);
		    System.out.println("ERK| - Location: " + acc.longitude + ", "+acc.latitude);
		}

                assertEquals(test.network, acc.getNetwork().toString());
		assertEquals(test.hasRecord, acc.recordFound);
            }
        }
    }

    @Test
    public void testGetRecordWithBuilder() throws IOException {
        GetRecordTest[] tests = {
	    new GetRecordTest("8.8.8.8", "ip-db-0cf0e7a0b9649404168f52a0c8be57c9.mmdb", "8.8.0.0/17", true), //ERK
	    new GetRecordTest("85.191.80.236",  "ip-db-0cf0e7a0b9649404168f52a0c8be57c9.mmdb", "85.191.80.0/21", true), //ERK
	    /*
                new GetRecordTest("1.1.1.1", "MaxMind-DB-test-ipv6-32.mmdb", "1.0.0.0/8", false),
                new GetRecordTest("::1:ffff:ffff", "MaxMind-DB-test-ipv6-24.mmdb", "0:0:0:0:0:1:ffff:ffff/128", true),
                new GetRecordTest("::2:0:1", "MaxMind-DB-test-ipv6-24.mmdb", "0:0:0:0:0:2:0:0/122", true),
                new GetRecordTest("1.1.1.1", "MaxMind-DB-test-ipv4-24.mmdb", "1.1.1.1/32", true),
                new GetRecordTest("1.1.1.3", "MaxMind-DB-test-ipv4-24.mmdb", "1.1.1.2/31", true),
                new GetRecordTest("1.1.1.3", "MaxMind-DB-test-decoder.mmdb", "1.1.1.0/24", true),
                new GetRecordTest("::ffff:1.1.1.128", "MaxMind-DB-test-decoder.mmdb", "1.1.1.0/24", true),
                new GetRecordTest("::1.1.1.128", "MaxMind-DB-test-decoder.mmdb", "0:0:0:0:0:0:101:100/120", true),
                // new GetRecordTest("200.0.2.1", "MaxMind-DB-no-ipv4-search-tree.mmdb", "0.0.0.0/0", true),
                // new GetRecordTest("::200.0.2.1", "MaxMind-DB-no-ipv4-search-tree.mmdb", "0:0:0:0:0:0:0:0/64", true),
                // new GetRecordTest("0:0:0:0:ffff:ffff:ffff:ffff", "MaxMind-DB-no-ipv4-search-tree.mmdb", "0:0:0:0:0:0:0:0/64", true),
                new GetRecordTest("ef00::", "MaxMind-DB-no-ipv4-search-tree.mmdb", "8000:0:0:0:0:0:0:0/1", false)
	    */
        };

	// Build callback:
	RecordCallbackBuilder<Accumulator> builder = new RecordCallbackBuilder<>();
	builder.obj("continent").obj("names").text("en", (Accumulator state, CharSequence value) -> TextNode.assignToStringBuilder(state.continent, value));
	builder.obj("country").obj("names").text("en", (Accumulator state, CharSequence value) -> TextNode.assignToStringBuilder(state.country, value));
	builder.obj("city").obj("names").text("en", (Accumulator state, CharSequence value) -> TextNode.assignToStringBuilder(state.city, value));
	builder.obj("location").number("latitude", (Accumulator state, double value) -> state.latitude = value);
	builder.obj("location").number("longitude", (Accumulator state, double value) -> state.longitude = value);
	builder.onBegin(state -> state.recordFound = true);
	builder.onNetwork((Accumulator state, byte[] ipAddress, int prefixLength) -> {
		state.ipAddress = ipAddress;
		state.prefixLength = prefixLength;
	    });


	Callbacks.RecordCallback<Accumulator> callback = builder.build();

	System.out.println("ERK| In test - without cache");
        for (GetRecordTest test : tests) {
            try (CallbackReader reader = new CallbackReader(test.db, NoCache.getInstance())) {

		System.out.println("ERK| Test case: " + test.ip +" in " + test.db);
		Accumulator acc = new Accumulator();
		for (int i=1; i<=3; i++) {
		    byte[] rawAddress = test.ip.getAddress();
		    long a0 = ErikUtil.allocCount();
		    reader.lookupRecord(rawAddress, callback, acc);
		    long a1 = ErikUtil.allocCount();
		    System.out.println("ERK| Read #"+i+": alloc=" + (a1-a0));
		    System.out.println("ERK| - Continent: " + acc.continent);
		    System.out.println("ERK| - Country: " + acc.country);
		    System.out.println("ERK| - City: " + acc.city);
		    System.out.println("ERK| - Location: " + acc.longitude + ", "+acc.latitude);
		}

                assertEquals(test.network, acc.getNetwork().toString());
		assertEquals(test.hasRecord, acc.recordFound);
            }
        }
    }

    private void testIpV4(Reader reader, File file) throws IOException {

        for (int i = 0; i <= 5; i++) {
            String address = "1.1.1." + (int) Math.pow(2, i);
            ObjectNode data = this.om.createObjectNode();
            data.put("ip", address);

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address)));
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
            ObjectNode data = this.om.createObjectNode();
            data.put("ip", pairs.get(address));

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address)));
        }

        for (String ip : new String[]{"1.1.1.33", "255.254.253.123"}) {
            assertNull(reader.get(InetAddress.getByName(ip)));
        }
    }

    // XXX - logic could be combined with above
    private void testIpV6(Reader reader, File file) throws IOException {
        String[] subnets = new String[]{"::1:ffff:ffff", "::2:0:0",
                "::2:0:40", "::2:0:50", "::2:0:58"};

        for (String address : subnets) {
            ObjectNode data = this.om.createObjectNode();
            data.put("ip", address);

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address)));
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
            ObjectNode data = this.om.createObjectNode();
            data.put("ip", pairs.get(address));

            assertEquals("found expected data record for " + address + " in "
                    + file, data, reader.get(InetAddress.getByName(address)));
        }

        for (String ip : new String[]{"1.1.1.33", "255.254.253.123", "89fa::"}) {
            assertNull(reader.get(InetAddress.getByName(ip)));
        }
    }

    static File getFile(String name) {
        return new File(ReaderTest.class.getResource("/maxmind-db/test-data/" + name).getFile());
    }

    static InputStream getStream(String name) {
        return ReaderTest.class.getResourceAsStream("/maxmind-db/test-data/" + name);
    }

    static class Accumulator {
	byte[] ipAddress;
	int prefixLength;
	boolean recordFound;

	final StringBuilder continent = new StringBuilder();
	final StringBuilder country = new StringBuilder();
	final StringBuilder city = new StringBuilder();
	double latitude, longitude;

	public void reset() {
	    ipAddress = null;
	    prefixLength = -1;
	    recordFound = false;

	    city.setLength(0);
	    continent.setLength(0);
	    country.setLength(0);
	    latitude = Double.NaN;
	    longitude = Double.NaN;
	}

	public Network getNetwork() {
	    try {
		return new Network(InetAddress.getByAddress(ipAddress), prefixLength);
	    } catch (UnknownHostException uhe) {throw new RuntimeException(uhe);}
	}
    }

}
