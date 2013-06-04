package com.maxmind.maxminddb;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class PointerTest {
    @SuppressWarnings("static-method")
    @Test
    public void testWithPointers() throws MaxMindDbException, IOException {
        File file = new File("test-data/pointer.bin");
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        FileChannel fc = raf.getChannel();
        try {
            Decoder decoder = new Decoder(fc, 0);

            ObjectMapper om = new ObjectMapper();

            ObjectNode map = om.createObjectNode();
            map.put("long_key", "long_value1");
            assertEquals(map, decoder.decode(0).getObject());

            map = om.createObjectNode();
            map.put("long_key", "long_value2");
            assertEquals(map, decoder.decode(22).getObject());

            map = om.createObjectNode();
            map.put("long_key2", "long_value1");
            assertEquals(map, decoder.decode(37).getObject());

            map = om.createObjectNode();
            map.put("long_key2", "long_value2");
            assertEquals(map, decoder.decode(50).getObject());

            map = om.createObjectNode();
            map.put("long_key", "long_value1");
            assertEquals(map, decoder.decode(55).getObject());

            map = om.createObjectNode();
            map.put("long_key2", "long_value2");
            assertEquals(map, decoder.decode(57).getObject());
        } finally {
            if (fc != null) {
                fc.close();
            }
            if (raf != null) {
                raf.close();
            }
        }

    }
}
