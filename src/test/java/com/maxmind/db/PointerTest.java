package com.maxmind.db;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.maxmind.db.Reader.FileMode;

public class PointerTest {
    @SuppressWarnings("static-method")
    @Test
    public void testWithPointers() throws
            IOException, URISyntaxException {
        File file = new File(PointerTest.class.getResource(
                "/maxmind-db/test-data/maps-with-pointers.raw").toURI());
        BufferHolder ptf = new BufferHolder(file, FileMode.MEMORY);
        Decoder decoder = new Decoder(new NoCache(), ptf.get(), 0);

        ObjectMapper om = new ObjectMapper();

        ObjectNode map = om.createObjectNode();
        map.put("long_key", "long_value1");
        assertEquals(map, decoder.decode(0).getNode());

        map = om.createObjectNode();
        map.put("long_key", "long_value2");
        assertEquals(map, decoder.decode(22).getNode());

        map = om.createObjectNode();
        map.put("long_key2", "long_value1");
        assertEquals(map, decoder.decode(37).getNode());

        map = om.createObjectNode();
        map.put("long_key2", "long_value2");
        assertEquals(map, decoder.decode(50).getNode());

        map = om.createObjectNode();
        map.put("long_key", "long_value1");
        assertEquals(map, decoder.decode(55).getNode());

        map = om.createObjectNode();
        map.put("long_key2", "long_value2");
        assertEquals(map, decoder.decode(57).getNode());
    }
}
