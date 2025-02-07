package com.maxmind.db;

import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PositionedByteReaderTest {

    @Test
    public void position() {
        final PositionedByteReader r = new PositionByteReader(10);
        Assertions.assertEquals(0L, r.position());
        Assertions.assertSame(r, r.position(1));
        Assertions.assertEquals(1L, r.position());
        Assertions.assertThrows(IllegalArgumentException.class, () -> r.position(r.capacity()));
        Assertions.assertThrows(IllegalArgumentException.class, () -> r.position(-1));
        final byte[] expectedPositionBytes = new byte[3];
        Arrays.fill(expectedPositionBytes, (byte) 1);
        Assertions.assertArrayEquals(
            expectedPositionBytes,
            r.getBytes(expectedPositionBytes.length)
        );
    }

    @Test
    public void capacity() {
        final long capacity = 0;
        final PositionedByteReader r = new PositionByteReader(capacity);
        Assertions.assertEquals(capacity, r.capacity());
        Assertions.assertThrows(IllegalArgumentException.class, () -> new PositionByteReader(-1));
    }

}
