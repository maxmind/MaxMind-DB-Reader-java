import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.net.InetAddresses;
import com.maxmind.maxminddb.MaxMindDbException;
import com.maxmind.maxminddb.Reader;
import com.maxmind.maxminddb.Reader.FileMode;

public class Benchmark {

    public static void main(String[] args) throws IOException,
            MaxMindDbException {
        File file = new File("GeoIP2-City.mmdb");

        Reader r = new Reader(file, FileMode.MEMORY_MAPPED);
        Random random = new Random();
        int count = 1000000;
        long startTime = System.nanoTime();
        for (int i = 0; i < count; i++) {
            InetAddress ip = InetAddresses.fromInteger(random.nextInt());
            if (i % 50000 == 0) {
                System.out.println(i + " " + ip);
            }
            JsonNode t = r.get(ip);
        }
        long endTime = System.nanoTime();

        long duration = endTime - startTime;
        System.out.println("Requests per second: " + count * 1000000000.0
                / (duration));
    }
}
