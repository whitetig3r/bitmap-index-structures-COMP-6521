import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class BitMapIndex {

  public final int RECORD_SIZE = 101;
  int tuplesInABuffer;
  private File file;
  private int numberOfRecords;

  public BitMapIndex(String fileLocation, int tuplesInABuffer, boolean doCleanUp)
      throws IOException {
    file = new File(fileLocation);
    this.tuplesInABuffer = tuplesInABuffer;
    numberOfRecords = (int) Math.ceil(file.length() / (float) RECORD_SIZE);
    if (doCleanUp) {
      for (FieldEnum fieldEnum : FieldEnum.values()) {
        cleanUp(fieldEnum.getName());
      }
    }
  }

  public static ArrayList<Integer> decodeRunLength(String encodedLine) {
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

  private void cleanUp(String path) throws IOException {
    Files.walk(Paths.get(String.format("data/output/index/%s", path)))

        .filter(Files::isRegularFile)
        .map(Path::toFile)
        .forEach(File::delete);
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
    int tuplesInLastChunk = numberOfRecords % tuplesInABuffer;
    int readCount = 0;
    int writeCount = 0;
    while ((line = bufferedReader.readLine()) != null) {
      String empId = FieldEnum.EMP_ID.getValue(line);
      String date = FieldEnum.DATE.getValue(line);
      String gender = FieldEnum.GENDER.getValue(line);
      addRecordToIndex(empIdBitVectors, i, empId);
      addRecordToIndex(dateBitVectors, i, date);
      addRecordToIndex(genderBitVectors, i, gender);
      i++;
      if (i % tuplesInABuffer == 0 || i == numberOfRecords) {
        int run = (int) Math.ceil(i / (float) tuplesInABuffer);
        readCount += 1;
        generateIndex(FieldEnum.EMP_ID.getName(), empIdBitVectors, isCompressed, run);
        generateIndex(FieldEnum.DATE.getName(), dateBitVectors, isCompressed, run);
        generateIndex(FieldEnum.GENDER.getName(), genderBitVectors, isCompressed, run);
        writeCount += 3;
      }
    }
    System.out.printf("Reads:%d, Writes:%d\n", readCount, writeCount);
    bufferedReader.close();


  }

  private void generateIndex(String indexField,
      SortedMap<String, ArrayList<Integer>> bitVectors, boolean isCompressed, int run)
      throws IOException {
    String fileName = file.toPath().getFileName().toString();
    Path indexPath = Paths.get(
        String.format("data/output/index/%s/%s-index-%d-%s-%s", indexField, indexField, run,
            isCompressed ? "compressed"
                : "uncompressed", fileName));

    BufferedWriter bufferedWriter = Files.newBufferedWriter(indexPath);
    for (Entry<String, ArrayList<Integer>> entry : bitVectors.entrySet()) {
      bufferedWriter.append(entry.getKey());
      for (Integer runLength : entry.getValue()) {
        String encodedRun =
            isCompressed ? encodeRunLength(runLength) : encodeRunLengthCompressed(runLength);
        bufferedWriter.append(encodedRun);
      }
      bufferedWriter.append("\n");
    }
    bitVectors.clear();
    bufferedWriter.close();
  }

  public void mergePartialIndexes(FieldEnum fieldEnum, boolean isCompressed)
      throws IOException {
    String fileName = file.toPath().getFileName().toString();
    Path mergedIndexPath = Paths.get(
        String.format("data/output/merged/%s-index-%s-%s", fieldEnum.getName(),
            isCompressed ? "compressed"
                : "uncompressed", fileName));

    BufferedWriter bufferedWriter = Files.newBufferedWriter(mergedIndexPath);

    List<PartialIndexReader> readerList = new ArrayList<>();
    Path directory = Paths.get("data/output/index/" + fieldEnum.getName());

    // For each file, get a buffered reader
    Files.walk(directory)
        .filter(Files::isRegularFile)
        .forEach(f -> {
          try {
            readerList.add(
                new PartialIndexReader(f.toString(), tuplesInABuffer, fieldEnum.getFieldLength()));
          } catch (IOException e) {
            e.printStackTrace();
          }
        });

    // Key = empId, Value = runs as the array list
    // Initial Fill
    for (PartialIndexReader partialIndexReader : readerList) {
      partialIndexReader.getNextIndex();
    }

    String minIndex;
    while ((minIndex = getMinOfAllPartialIndexes(readerList))
        != null) {
      bufferedWriter.append(minIndex);
      int lastIndex = 0;
      for (int i = 0; i < readerList.size(); i++) {
        PartialIndexReader reader = readerList.get(i);
        Entry<String, ArrayList<Integer>> currentIndex = reader.getCurrentIndex();
        if (currentIndex != null && currentIndex.getKey().equals(minIndex)) {
          ArrayList<Integer> currentRuns = currentIndex.getValue();
          int firstIndex = i * tuplesInABuffer + currentRuns.get(0);
          currentRuns.set(0, firstIndex - lastIndex);
          lastIndex = currentRuns.stream().reduce(0, (a, b) -> a + b + 1);
          for (Integer run : currentRuns) {
            bufferedWriter.append(encodeRunLength(run));
          }
          // point to the next index in the partialReader
          reader.getNextIndex();
        }
      }
      bufferedWriter.append("\n");
    }
    bufferedWriter.close();
  }

  private String getMinOfAllPartialIndexes(
      List<PartialIndexReader> readerList) {
    return readerList.stream().map(PartialIndexReader::getCurrentIndex)
        .filter(Objects::nonNull)
        .map(Entry::getKey)
        .min(String::compareTo).orElse(null);
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
      runs.add(i % 10);
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

  private String encodeRunLengthCompressed(int run) {
    StringBuilder stringBuilder = new StringBuilder();
    // set run bits to 0
    for (int i = 0; i < run; i++) {
      stringBuilder.append("0");
    }
    // set the final bit to 1
    stringBuilder.append("1");
    return stringBuilder.toString();
  }
}
