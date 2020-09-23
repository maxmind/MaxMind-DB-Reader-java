package com.maxmind.db;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.maxmind.db.Reader.FileMode;

public class PointerTest {
    @SuppressWarnings("static-method")
    @Test
    public void testWithPointers() throws IOException {
        File file = ReaderTest.getFile("maps-with-pointers.raw");
        BufferHolder ptf = new BufferHolder(file, FileMode.MEMORY);
        Decoder decoder = new Decoder(NoCache.getInstance(), ptf.get(), 0);

        Map<String, String> map = new HashMap<>();
        map.put("long_key", "long_value1");
        assertEquals(map, decoder.decode(0, Map.class));

        map = new HashMap<>();
        map.put("long_key", "long_value2");
        assertEquals(map, decoder.decode(22, Map.class));

        map = new HashMap<>();
        map.put("long_key2", "long_value1");
        assertEquals(map, decoder.decode(37, Map.class));

        map = new HashMap<>();
        map.put("long_key2", "long_value2");
        assertEquals(map, decoder.decode(50, Map.class));

        map = new HashMap<>();
        map.put("long_key", "long_value1");
        assertEquals(map, decoder.decode(55, Map.class));

        map = new HashMap<>();
        map.put("long_key2", "long_value2");
        assertEquals(map, decoder.decode(57, Map.class));
    }
}
