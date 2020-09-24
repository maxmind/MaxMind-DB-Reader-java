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

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;

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
    public void testGetRecord() throws IOException {
        GetRecordTest[] tests = {
	    new GetRecordTest("8.8.8.8", "ip-db-0cf0e7a0b9649404168f52a0c8be57c9.mmdb", "8.8.0.0/17", true), //ERK
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
        };

	// Continent readout:
	AreasOfInterest.TextNode continentTextNode = new AreasOfInterest.TextNode<Accumulator>() {
		@Override public void setValue(Accumulator state, CharSequence value) {
		    assignToStringBuilder(state.continent, value);
		}
	    };
	Map<String, AreasOfInterest.Callback<Accumulator>> continentNamesMap = new HashMap();
	continentNamesMap.put("en", continentTextNode);
	AreasOfInterest.ObjectNode<Accumulator> continentNamesNode = new AreasOfInterest.ObjectNode(continentNamesMap);
	Map<String, AreasOfInterest.Callback<Accumulator>> continentMap = new HashMap();
	continentMap.put("names", continentNamesNode);
	AreasOfInterest.ObjectNode<Accumulator> continentNode = new AreasOfInterest.ObjectNode(continentMap);

	// Country readout:
	AreasOfInterest.TextNode countryTextNode = new AreasOfInterest.TextNode<Accumulator>() {
		@Override public void setValue(Accumulator state, CharSequence value) {
		    assignToStringBuilder(state.country, value);
		}
	    };
	Map<String, AreasOfInterest.Callback<Accumulator>> countryNamesMap = new HashMap();
	countryNamesMap.put("en", countryTextNode);
	AreasOfInterest.ObjectNode<Accumulator> countryNamesNode = new AreasOfInterest.ObjectNode(countryNamesMap);
	Map<String, AreasOfInterest.Callback<Accumulator>> countryMap = new HashMap();
	countryMap.put("names", countryNamesNode);
	AreasOfInterest.ObjectNode<Accumulator> countryNode = new AreasOfInterest.ObjectNode(countryMap);

	// City readout:
	AreasOfInterest.TextNode cityTextNode = new AreasOfInterest.TextNode<Accumulator>() {
		@Override public void setValue(Accumulator state, CharSequence value) {
		    assignToStringBuilder(state.city, value);
		}
	    };
	Map<String, AreasOfInterest.Callback<Accumulator>> cityNamesMap = new HashMap();
	cityNamesMap.put("en", cityTextNode);
	AreasOfInterest.ObjectNode<Accumulator> cityNamesNode = new AreasOfInterest.ObjectNode(cityNamesMap);
	Map<String, AreasOfInterest.Callback<Accumulator>> cityMap = new HashMap();
	cityMap.put("names", cityNamesNode);
	AreasOfInterest.ObjectNode<Accumulator> cityNode = new AreasOfInterest.ObjectNode(cityMap);

	// Position readout:
	AreasOfInterest.DoubleNode latitudeNode = new AreasOfInterest.DoubleNode<Accumulator>() {
		@Override public void setValue(Accumulator state, double value) {
		    state.latitude = value;
		}
	    };
	AreasOfInterest.DoubleNode longitudeNode = new AreasOfInterest.DoubleNode<Accumulator>() {
		@Override public void setValue(Accumulator state, double value) {
		    state.longitude = value;
		}
	    };
	Map<String, AreasOfInterest.Callback<Accumulator>> locationMap = new HashMap();
	locationMap.put("latitude", latitudeNode);
	locationMap.put("longitude", longitudeNode);
	AreasOfInterest.ObjectNode<Accumulator> locationNode = new AreasOfInterest.ObjectNode(locationMap);

	Map<String, AreasOfInterest.Callback<Accumulator>> rootFieldMap = new HashMap();
	rootFieldMap.put("continent", continentNode);
	rootFieldMap.put("country", countryNode);
	rootFieldMap.put("city", cityNode);
	rootFieldMap.put("location", locationNode);
	AreasOfInterest.RecordCallback<Accumulator> callback = new AreasOfInterest.RecordCallback<Accumulator>(rootFieldMap) {
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
