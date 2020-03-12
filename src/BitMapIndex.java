import java.io.BufferedReader;
import java.io.BufferedWriter;
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

  public BitMapIndex() {
  }

  int log2(int x) {
    return (int) (Math.log(x) / Math.log(2));
  }

  public void createIndex(String fileLocation) throws IOException {
    Path filePath = Paths.get(fileLocation);
    String fileName = filePath.getFileName().toString();
    decodeRunLength("1110110111101101");
    Path empIdIndexPath = Paths.get("data/output/index/empId-index-" + fileName);
    Path dateIndexPath = Paths.get("data/output/index/date-index-" + fileName);
    Path genderIndexPath = Paths.get("data/output/index/gender-index-" + fileName);
    BufferedReader bufferedReader = Files.newBufferedReader(filePath);
    SortedMap<String, ArrayList<Integer>> empIdBitVectors = new TreeMap<>();
    SortedMap<String, ArrayList<Integer>> dateBitVectors = new TreeMap<>();
    SortedMap<String, ArrayList<Integer>> genderBitVectors = new TreeMap<>();
    String line;
    int i = 0;
    while ((line = bufferedReader.readLine()) != null) {
      String compressedBitMap = encodeRunLength(i);
      String empId = line.substring(0, 8);
      String date = line.substring(8, 18);
      String gender = line.substring(43, 44);
      addRecordToIndex(empIdBitVectors, i, empId);
      addRecordToIndex(dateBitVectors, i, date);
      addRecordToIndex(genderBitVectors, i, gender);
      i++;
    }
    bufferedReader.close();
    generateIndex(empIdIndexPath, empIdBitVectors);
    generateIndex(dateIndexPath, dateBitVectors);
    generateIndex(genderIndexPath, genderBitVectors);

  }

  private void generateIndex(Path indexPath,
      SortedMap<String, ArrayList<Integer>> bitVectors) throws IOException {
    BufferedWriter bufferedWriter = Files.newBufferedWriter(indexPath);
    for (Entry<String, ArrayList<Integer>> entry : bitVectors.entrySet()) {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(entry.getKey());
      stringBuilder.append(compressRunLength(entry.getValue()));
      stringBuilder.append("\n");
      bufferedWriter.append(stringBuilder.toString());
    }
    bufferedWriter.close();
  }

  private void addRecordToIndex(SortedMap<String, ArrayList<Integer>> bitVectors, int i,
      String field) {
    if (bitVectors.containsKey(field)) {
      ArrayList<Integer> currentList = bitVectors.get(field);
      int lastRun = currentList.get(currentList.size() - 1);
      currentList.add(i - lastRun);
    } else {
      ArrayList<Integer> runs = new ArrayList<>();
      runs.add(i);
      bitVectors.put(field, runs);
    }
  }

  private String compressRunLength(ArrayList<Integer> runs) {
    // concatenate runs to a single string
    return runs.stream().map(this::encodeRunLength).collect(Collectors.joining(""));
  }

  private String decompressRunLength(String encodedLine) {
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
