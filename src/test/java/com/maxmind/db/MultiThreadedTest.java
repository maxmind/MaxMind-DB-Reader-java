package com.maxmind.db;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

public class MultiThreadedTest {

    @Test
    public void multipleMmapOpens() throws InterruptedException,
            ExecutionException {
        Callable<Map> task = () -> {
            try (Reader reader = new Reader(ReaderTest.getFile("MaxMind-DB-test-decoder.mmdb"))) {
                return reader.get(InetAddress.getByName("::1.1.1.0"), Map.class);
            }
        };
        MultiThreadedTest.runThreads(task);
    }

    @Test
    public void streamThreadTest() throws IOException, InterruptedException,
            ExecutionException {
        try (Reader reader = new Reader(ReaderTest.getStream("MaxMind-DB-test-decoder.mmdb"))) {
            MultiThreadedTest.threadTest(reader);
        }
    }

    @Test
    public void mmapThreadTest() throws IOException, InterruptedException,
            ExecutionException {
        try (Reader reader = new Reader(ReaderTest.getFile("MaxMind-DB-test-decoder.mmdb"))) {
            MultiThreadedTest.threadTest(reader);
        }
    }

    private static void threadTest(final Reader reader)
            throws InterruptedException, ExecutionException {
        Callable<Map> task = () -> reader.get(InetAddress.getByName("::1.1.1.0"), Map.class);
        MultiThreadedTest.runThreads(task);
    }

    private static void runThreads(Callable<Map> task)
            throws InterruptedException, ExecutionException {
        int threadCount = 256;
        List<Callable<Map>> tasks = Collections.nCopies(threadCount, task);
        ExecutorService executorService = Executors
                .newFixedThreadPool(threadCount);
        List<Future<Map>> futures = executorService.invokeAll(tasks);

        for (Future<Map> future : futures) {
            Map record = future.get();
            assertEquals(268435456, (long) record.get("uint32"));
            assertEquals("unicode! ☯ - ♫", (String) record.get("utf8_string"));
        }
    }
}
