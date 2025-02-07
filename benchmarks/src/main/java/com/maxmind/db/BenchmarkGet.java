package com.maxmind.db;

import com.maxmind.db.Reader.FileMode;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 10, time = 5)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BenchmarkGet {
    private static final int COUNT = 1_000_000;

    private static InetAddress[] getInetAddresses(final int seed) throws UnknownHostException {
        final InetAddress[] addresses = new InetAddress[COUNT];
        final Random random = new Random(seed);
        final byte[] address = new byte[4];
        for (int addressIx = 0; addressIx < COUNT; addressIx++) {
            random.nextBytes(address);
            addresses[addressIx] = InetAddress.getByAddress(address);
        }
        return addresses;
    }

    InetAddress[] addresses;
    Reader reader;
    Reader cachedReader;

    @Setup
    public void setup() throws IOException {
        addresses = getInetAddresses(0);
        final File database = new File(System.getenv("GEO_LITE"));
        reader = new Reader(database, FileMode.MEMORY_MAPPED, NoCache.getInstance());
        cachedReader = new Reader(database, FileMode.MEMORY_MAPPED, new CHMCache());
    }

    @Benchmark
    @OperationsPerInvocation(COUNT)
    public void withoutCaching(Blackhole bh) throws IOException {
        for (InetAddress address: addresses) {
            bh.consume(reader.get(address, Map.class));
        }
    }

    @Benchmark
    @OperationsPerInvocation(COUNT)
    public void withCaching(Blackhole bh) throws IOException {
        for (InetAddress address: addresses) {
            bh.consume(cachedReader.get(address, Map.class));
        }
    }
}
