import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.net.InetAddresses;
import com.maxmind.maxminddb.InvalidDatabaseException;
import com.maxmind.maxminddb.MaxMindDbReader;
import com.maxmind.maxminddb.MaxMindDbReader.FileMode;

public class Benchmark {

    public static void main(String[] args) throws IOException,
            InvalidDatabaseException {
        File file = new File("GeoLite2-City.mmdb");

        MaxMindDbReader r = new MaxMindDbReader(file, FileMode.MEMORY_MAPPED);
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
