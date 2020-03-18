import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeMap;

public class PartialIndexReader {

  BufferedReader bufferedReader;
  TreeMap<String, ArrayList<Integer>> indexes;
  private int tuplesInABuffer;
  private int fieldLength;

  public PartialIndexReader(String path, int tuplesInABuffer, int fieldLength) throws IOException {
    bufferedReader = Files.newBufferedReader(Paths.get(path));
    this.tuplesInABuffer = tuplesInABuffer;
    this.fieldLength = fieldLength;
    this.indexes = new TreeMap<>();
  }

  public Entry<String, ArrayList<Integer>> getNextIndex() throws IOException {
    char[] key = new char[fieldLength];
    bufferedReader.read(key);
    if(String.valueOf(key).trim().isEmpty()){
      return null;
    }
    int c;
    int runLength = 0;
    ArrayList<Integer> runs = new ArrayList<>();
    while ((c = bufferedReader.read()) != -1) {
      char currentChar = (char) c;
      if (currentChar == '\n') {
        break;
      }
      if (currentChar == '\r') {
        bufferedReader.skip(1);
        break;
      }
      if (currentChar != '0') {
        runLength++;
        continue;
      }
      // count the last 0 followed by 1's
      runLength++;
      char run[] = new char[runLength];
      bufferedReader.read(run);
      runs.add(Integer.valueOf(String.valueOf(run), 2));
      runLength = 0;
    }
    return new SimpleEntry<>(String.valueOf(key), runs);
  }
}
