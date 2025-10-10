package com.maxmind.db;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.maxmind.db.Reader.FileMode;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class PointerTest {
    @SuppressWarnings("static-method")
    @Test
    public void testWithPointers() throws IOException {
        var file = ReaderTest.getFile("maps-with-pointers.raw");
        var ptf = new BufferHolder(file, FileMode.MEMORY);
        var decoder = new Decoder(NoCache.getInstance(), ptf.get(), 0);

        var map = new HashMap<String, String>();
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
