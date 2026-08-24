package com.maxmind.db;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.maxmind.db.Reader.FileMode;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
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

    @SuppressWarnings("static-method")
    @Test
    public void testPointerBeyond2GiBIsNotSignExtended() throws IOException {
        // Control byte for a four-byte pointer, followed by the offset 3473557240.
        var buffer = new SingleBuffer(ByteBuffer.wrap(new byte[]{
            (byte) 0x38, (byte) 0xCF, (byte) 0x0A, (byte) 0x46, (byte) 0xF8}));

        var observed = new AtomicLong(Long.MIN_VALUE);
        NodeCache cache = (key, loader) -> {
            observed.set(key.offset());
            return new DecodedValue(null);
        };

        new Decoder(cache, buffer, 0).decode(0, Object.class);

        assertEquals(3473557240L, observed.get());
    }
}
