import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class BitMapIndex {

  public final int RECORD_SIZE = 101;
  private File file;
  private int numberOfRecords;

  public BitMapIndex(String fileLocation) {
    file = new File(fileLocation);
    numberOfRecords = (int) (file.length() / RECORD_SIZE);
  }

  int log2(int x) {
    return (int) (Math.log(x) / Math.log(2));
  }

  public void createIndex(boolean isCompressed) throws IOException {
    BufferedReader bufferedReader = Files.newBufferedReader(file.toPath());
    SortedMap<String, ArrayList<Integer>> empIdBitVectors = new TreeMap<>();
    SortedMap<String, ArrayList<Integer>> dateBitVectors = new TreeMap<>();
    SortedMap<String, ArrayList<Integer>> genderBitVectors = new TreeMap<>();
    String line;
    int i = 0;
    while ((line = bufferedReader.readLine()) != null) {
      String empId = FieldEnum.EMP_ID.getValue(line);
      String date = FieldEnum.DATE.getValue(line);
      String gender = FieldEnum.GENDER.getValue(line);
      addRecordToIndex(empIdBitVectors, i, empId);
      addRecordToIndex(dateBitVectors, i, date);
      addRecordToIndex(genderBitVectors, i, gender);
      i++;
    }
    bufferedReader.close();
    generateIndex("empId", empIdBitVectors, isCompressed);
    generateIndex("date", dateBitVectors, isCompressed);
    generateIndex("gender", genderBitVectors, isCompressed);

  }

  private void generateIndex(String indexField,
      SortedMap<String, ArrayList<Integer>> bitVectors, boolean isCompressed) throws IOException {
    String fileName = file.toPath().getFileName().toString();
    Path indexPath = Paths.get(
        String.format("data/output/index/%s-index-%s-%s", indexField, isCompressed ? "compressed"
            : "uncompressed", fileName));

    BufferedWriter bufferedWriter = Files.newBufferedWriter(indexPath);
    for (Entry<String, ArrayList<Integer>> entry : bitVectors.entrySet()) {
      String encodedLine = compressRuns(entry.getValue());
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(entry.getKey());
      stringBuilder.append(isCompressed ? encodedLine : decompressRuns(encodedLine));
      stringBuilder.append("\n");
      bufferedWriter.append(stringBuilder.toString());
    }
    bufferedWriter.close();
  }

  private void addRecordToIndex(SortedMap<String, ArrayList<Integer>> bitVectors, int i,
      String field) {
    if (bitVectors.containsKey(field)) {
      ArrayList<Integer> currentList = bitVectors.get(field);
      // get the lastIndex of the current bitVector
      int lastIndex = currentList.stream().reduce(0, (a, b) -> a + b + 1);
      // get the run length
      currentList.add(i - lastIndex);
    } else {
      ArrayList<Integer> runs = new ArrayList<>();
      runs.add(i);
      // initial run
      bitVectors.put(field, runs);
    }
  }

  private String compressRuns(ArrayList<Integer> runs) {
    // concatenate runs to a single string
    return runs.stream().map(this::encodeRunLength).collect(Collectors.joining(""));
  }

  private String decompressRuns(String encodedLine) {
    ArrayList<Integer> runs = decodeRunLength(encodedLine);
    StringBuilder stringBuilder = new StringBuilder();
    for (int run : runs) {
      // set run bits to 0
      for (int i = 0; i < run; i++) {
        stringBuilder.append("0");
      }
      // set the final bit to 1
      stringBuilder.append("1");
    }
    // trailing zeros if any
    while (stringBuilder.length() < numberOfRecords) {
      stringBuilder.append("0");
    }
    return stringBuilder.toString();
  }

  private String encodeRunLength(int i) {
    int j = log2(i);
    StringBuilder sb = new StringBuilder();
    // j - 1 bits to 1
    for (int bit = 0; bit < j; bit++) {
      sb.append("1");
    }
    // last bit to zero
    sb.append("0");
    // append the actual run length
    sb.append(Integer.toBinaryString(i));
    return sb.toString();
  }

  private ArrayList<Integer> decodeRunLength(String encodedLine) {
    int runLength = 0;
    ArrayList<Integer> runs = new ArrayList<>();
    for (int i = 0; i < encodedLine.length(); i++) {
      // count all 1's
      if (encodedLine.charAt(i) != '0') {
        runLength++;
        continue;
      }
      // count the last 0 followed by 1's
      runLength++;
      // get the run
      String run = encodedLine.substring(i + 1, (i + 1) + runLength);
      runs.add(Integer.valueOf(run, 2));
      // move to the next run
      i += runLength;
      runLength = 0;
    }
    return runs;
  }
}