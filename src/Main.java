import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
	// write your code here
        BitMapIndex bitMapIndex = new BitMapIndex(args[0]);
        bitMapIndex.createIndex(true);
        bitMapIndex.createIndex(false);
    }
}
