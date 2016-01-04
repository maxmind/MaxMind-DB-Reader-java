package com.maxmind.db;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.maxmind.db.Reader.FileMode;

public class PointerTest {
    @SuppressWarnings("static-method")
    @Test
    public void testWithPointers() throws IOException {
        File file = ReaderTest.getFile("maps-with-pointers.raw");
        BufferHolder ptf = new BufferHolder(file, FileMode.MEMORY);
        Decoder decoder = new Decoder(NoCache.getInstance(), ptf.get(), 0);

        ObjectMapper om = new ObjectMapper();

        ObjectNode map = om.createObjectNode();
        map.put("long_key", "long_value1");
        assertEquals(map, decoder.decode(0));

        map = om.createObjectNode();
        map.put("long_key", "long_value2");
        assertEquals(map, decoder.decode(22));

        map = om.createObjectNode();
        map.put("long_key2", "long_value1");
        assertEquals(map, decoder.decode(37));

        map = om.createObjectNode();
        map.put("long_key2", "long_value2");
        assertEquals(map, decoder.decode(50));

        map = om.createObjectNode();
        map.put("long_key", "long_value1");
        assertEquals(map, decoder.decode(55));

        map = om.createObjectNode();
        map.put("long_key2", "long_value2");
        assertEquals(map, decoder.decode(57));
    }
}
