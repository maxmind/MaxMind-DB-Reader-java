import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.net.InetAddresses;
import com.maxmind.db.InvalidDatabaseException;
import com.maxmind.db.Reader;
import com.maxmind.db.Reader.FileMode;

public class Benchmark {

    private final static int COUNT = 1000000;
    private final static int WARMUPS = 3;
    private final static int BENCHMARKS = 5;
    private final static boolean TRACE = false;

    public static void main(String[] args) throws IOException, InvalidDatabaseException {
        File file = new File(args.length > 0 ? args[0] : "GeoLite2-City.mmdb");
        loop("Warming up", file, WARMUPS);
        loop("Benchmarking", file, BENCHMARKS);
    }

    private static void loop(String msg, File file, int loops) throws IOException {
        System.out.println(msg);
        for (int i = 0; i < loops; i++) {
            Reader r = new Reader(file, FileMode.MEMORY_MAPPED);
            bench(r, COUNT, i);
        }
        System.out.println();
    }

    private static void bench(Reader r, int count, int seed) throws IOException {
        Random random = new Random(seed);
        long startTime = System.nanoTime();
        for (int i = 0; i < count; i++) {
            InetAddress ip = InetAddresses.fromInteger(random.nextInt());
            JsonNode t = r.get(ip);
            if (TRACE) {
                if (i % 50000 == 0) {
                    System.out.println(i + " " + ip);
                    System.out.println(t);
                }
            }
        }
        long endTime = System.nanoTime();

        long duration = endTime - startTime;
        long qps = count * 1000000000L / duration;
        System.out.println("Requests per second: " + qps);
    }
}
