package com.maxmind.maxminddb;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class PointerTest {
    @Test
    public void testWithPointers() throws MaxMindDbException, IOException {
        File file = new File("test-data/pointer.bin");
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        FileChannel fc = raf.getChannel();
        Decoder decoder = new Decoder(fc, 0);

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("long_key", "long_value1");
        assertEquals(map, decoder.decode(0).getObject());

        map = new HashMap<String, Object>();
        map.put("long_key", "long_value2");
        assertEquals(map, decoder.decode(22).getObject());

        map = new HashMap<String, Object>();
        map.put("long_key2", "long_value1");
        assertEquals(map, decoder.decode(37).getObject());

        map = new HashMap<String, Object>();
        map.put("long_key2", "long_value2");
        assertEquals(map, decoder.decode(50).getObject());

        map = new HashMap<String, Object>();
        map.put("long_key", "long_value1");
        assertEquals(map, decoder.decode(55).getObject());

        map = new HashMap<String, Object>();
        map.put("long_key2", "long_value2");
        assertEquals(map, decoder.decode(57).getObject());

    }
}
