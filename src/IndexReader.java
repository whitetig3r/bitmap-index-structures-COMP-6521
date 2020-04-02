import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

public class IndexReader {
    RandomAccessFile raf;
    Entry<String, ArrayList<Integer>> index;
    private int fieldLength;
    String path;
    int counter = 1;
    int tupleLength = 0;
    int lineLength = 0;

    public IndexReader(String path, int numberOfRecords, int fieldLength) throws IOException {
        this.path = path;
        this.fieldLength = fieldLength;
        raf = new RandomAccessFile(this.path, "r");
        this.lineLength = fieldLength + numberOfRecords + System.lineSeparator().length();
        this.tupleLength = (int) Math.ceil(raf.length() / (float) lineLength);
    }

    public Entry<String, ArrayList<Integer>> getNextIndexReversed() throws IOException {

        if (counter == tupleLength) {
            return null;
        }

        int seekPos = (int) (raf.length() - this.lineLength * counter);
        byte[] key = new byte[fieldLength];
        raf.seek(seekPos);
        raf.read(key);
        ArrayList<Integer> ones = findOnes();
        counter++;
        return new SimpleEntry<>(new String(key), ones);
    }

    public Entry<String, ArrayList<Integer>> getNextIndex() throws IOException {

        if(counter == tupleLength) {
            return null;
        }

        byte[] key = new byte[fieldLength];
        raf.read(key);
        ArrayList<Integer> ones = findOnes();
        counter++;
        return new SimpleEntry<>(new String(key), ones);
    }

    private ArrayList<Integer> findOnes() throws IOException {

        ArrayList<Integer> ones = new ArrayList<>();
        try {
            char c;
            for (int i = 0; i < lineLength - fieldLength; i++) {
                c = (char) raf.read();
                if (c == '0' || c == '\n' || c == '\r') {
                    continue;
                }
                ones.add(i);
            }
        }
        catch(EOFException e) {
            return ones;
        }

        return ones;
    }

}
