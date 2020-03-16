import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class Main {

    public static void main(String[] args) throws IOException {
	// write your code here
        System.out.println("Creating indexes for empId, date, gender");
        Instant start = Instant.now();
        BitMapIndex bitMapIndex = new BitMapIndex(args[0]);
        bitMapIndex.createIndex(false);
        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.printf("Indexes created in %f seconds\n",timeElapsed/1000.0);

//        bitMapIndex.createIndex(false);
    }
}
