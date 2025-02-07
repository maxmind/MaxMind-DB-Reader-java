package com.maxmind.db;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ByteReaderHolderTest {

    @Test
    public void testThreadSafe() throws IOException {
        final ByteReaderHolder holder =
            new ByteReaderHolder(
                new ByteArrayInputStream(new byte[] {0, 1})
            );
        final ByteReader byteReader0 = holder.get();
        byteReader0.position(1);
        final ByteReader byteReader1 = holder.get();
        Assertions.assertNotEquals(byteReader0.position(), byteReader1.position());
    }

}
