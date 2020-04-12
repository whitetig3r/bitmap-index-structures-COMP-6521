import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

public class PartialIndexReader {

  BufferedReader bufferedReader;
  RandomAccessFile raf;
  Entry<String, ArrayList<Integer>> index;
  private int fieldLength;
  String path;
  int counter = 1;

  public PartialIndexReader(String path, int fieldLength) throws IOException {
    bufferedReader = Files.newBufferedReader(Paths.get(path));
    this.path = path;
    this.fieldLength = fieldLength;
  }

  public Entry<String, ArrayList<Integer>> getNextIndex(BitMapIndex bmp) throws IOException {
    char[] key = new char[fieldLength];
    int localBytesReadCounter = 0;
    bufferedReader.read(key);
    localBytesReadCounter += fieldLength;
    if(String.valueOf(key).trim().isEmpty()){
      index = null;
      return index;
    }
    int c;
    int runLength = 0;
    ArrayList<Integer> runs = new ArrayList<>();
    while ((c = bufferedReader.read()) != -1) {
      localBytesReadCounter += 1;
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
      localBytesReadCounter += runLength;
      runs.add(Integer.valueOf(String.valueOf(run), 2));
      runLength = 0;
    }
    index = new SimpleEntry<>(String.valueOf(key), runs);
    if(localBytesReadCounter < 4096) bmp.readCount += 1;
    else bmp.readCount += (int)Math.ceil(localBytesReadCounter/4096.0);
    return index;
  }

  public Entry<String, ArrayList<Integer>> getCurrentIndex() {
    return index;
  }
}
