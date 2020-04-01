import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.UnaryOperator;

public class BitMapIndex {

  public final int RECORD_SIZE = 101;
  int tuplesInABuffer;
  private String inputFileName;
  private File file;
  private int numberOfRecords;

  public BitMapIndex(String fileLocation, String inputFileName, int tuplesInABuffer,
      boolean doCleanUp)
      throws IOException {
    file = new File(fileLocation);
    this.tuplesInABuffer = tuplesInABuffer;
    this.inputFileName = inputFileName;
    numberOfRecords = (int) Math.ceil(file.length() / (float) RECORD_SIZE);
    if (doCleanUp) {
      for (FieldEnum fieldEnum : FieldEnum.values()) {
        cleanUp(String.format("data/output/index/%s/%s", fieldEnum.getName(), inputFileName));
      }
      cleanUp("data/output/merged/compressed");
      cleanUp("data/output/merged/uncompressed");
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
    Files.walk(Paths.get(path))
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
    SortedMap<String, ArrayList<Integer>> deptBitVectors = new TreeMap<>();
    String line;
    int i = 0;
    int tuplesInLastChunk = numberOfRecords % tuplesInABuffer;
    int readCount = 0;
    int writeCount = 0;
    while ((line = bufferedReader.readLine()) != null) {
      String empId = FieldEnum.EMP_ID.getValue(line);
      String date = FieldEnum.DATE.getValue(line);
      String gender = FieldEnum.GENDER.getValue(line);
      String dept = FieldEnum.DEPT.getValue(line);
      addRecordToIndex(empIdBitVectors, i, empId);
      addRecordToIndex(dateBitVectors, i, date);
      addRecordToIndex(genderBitVectors, i, gender);
      addRecordToIndex(deptBitVectors, i, dept);
      i++;
      if (i % tuplesInABuffer == 0 || i == numberOfRecords) {
        int run = (int) Math.ceil(i / (float) tuplesInABuffer);
        readCount += 1;
        generateIndex(FieldEnum.EMP_ID.getName(), empIdBitVectors, isCompressed, run);
        generateIndex(FieldEnum.DATE.getName(), dateBitVectors, isCompressed, run);
        generateIndex(FieldEnum.GENDER.getName(), genderBitVectors, isCompressed, run);
        generateIndex(FieldEnum.DEPT.getName(), deptBitVectors, isCompressed, run);
        // TODO check if this disk IO count is correct
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
        String.format("data/output/index/%s/%s/%s-index-%03d-%s-%s", indexField, inputFileName,
            indexField, run,
            isCompressed ? "compressed"
                : "uncompressed", fileName));

    BufferedWriter bufferedWriter = Files.newBufferedWriter(indexPath);
    for (Entry<String, ArrayList<Integer>> entry : bitVectors.entrySet()) {
      bufferedWriter.append(entry.getKey());
      for (Integer runLength : entry.getValue()) {
        if (isCompressed) {
          encodeRunLength(bufferedWriter, runLength);
        } else {
          encodeRunLengthUnCompressed(bufferedWriter, runLength);
        }
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
        String.format("data/output/merged/compressed/%s-index-%s-%s", fieldEnum.getName(),
            "compressed", fileName));

    BufferedWriter bufferedWriter = Files.newBufferedWriter(mergedIndexPath);

    List<PartialIndexReader> readerList = new ArrayList<>();
    Path directory = Paths.get("data/output/index/" + fieldEnum.getName() + "/" + inputFileName);

    // For each file, get a buffered reader
    Files.walk(directory)
        .filter(Files::isRegularFile)
        .sorted()
        .forEach(f -> {
          try {
            readerList.add(
                new PartialIndexReader(f.toString(), fieldEnum.getFieldLength()));
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
          lastIndex += currentRuns.stream().reduce(0, (a, b) -> a + b + 1);
          for (Integer run : currentRuns) {
            encodeRunLength(bufferedWriter, run);
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
      currentList.add((i % tuplesInABuffer) - lastIndex);
    } else {
      ArrayList<Integer> runs = new ArrayList<>();
      runs.add(i % tuplesInABuffer);
      // initial run
      bitVectors.put(field, runs);
    }
  }

  public void unCompressRuns(FieldEnum fieldEnum)
      throws IOException {
    String fileName = file.toPath().getFileName().toString();
    Path mergedIndexPath = Paths.get(
        String.format("data/output/merged/compressed/%s-index-%s-%s", fieldEnum.getName(),
            "compressed", fileName));
    Path uncompressedMergedIndexPath = Paths.get(
        String.format("data/output/merged/uncompressed/%s-index-%s-%s", fieldEnum.getName(),
            "uncompressed", fileName));
    PartialIndexReader partialIndexReader = new PartialIndexReader(mergedIndexPath.toString(),
        fieldEnum.getFieldLength());
    BufferedWriter bufferedWriterUncompressed = Files
        .newBufferedWriter(uncompressedMergedIndexPath);
    Entry<String, ArrayList<Integer>> entry;
    while ((entry = partialIndexReader.getNextIndex()) != null) {
      bufferedWriterUncompressed.append(entry.getKey());
      int sum = 0;
      for (Integer run : entry.getValue()) {
        sum += run + 1;
        encodeRunLengthUnCompressed(bufferedWriterUncompressed, run);
      }
      while (sum < numberOfRecords) {
        bufferedWriterUncompressed.append('0');
        sum++;
      }
      bufferedWriterUncompressed.append("\n");
    }
    bufferedWriterUncompressed.close();
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

  private void encodeRunLength(BufferedWriter bufferedWriter, int i) throws IOException {
    int j = log2(i);
    // j - 1 bits to 1
    for (int bit = 0; bit < j; bit++) {
      bufferedWriter.append('1');
    }
    // last bit to zero
    bufferedWriter.append('0');
    // append the actual run length
    bufferedWriter.append(Integer.toBinaryString(i));
  }

  private void encodeRunLengthUnCompressed(BufferedWriter bufferedWriter, int run)
      throws IOException {
    // set run bits to 0
    for (int i = 0; i < run; i++) {
      bufferedWriter.append('0');
    }
    // set the final bit to 1
    bufferedWriter.append('1');
  }

  public void eliminateDuplicates() throws IOException {

    String empFileName = file.toPath().getFileName().toString();
    Path empPath = Paths.get(
        String.format("data/output/merged/uncompressed/%s-index-%s-%s", FieldEnum.EMP_ID.getName(),
            "uncompressed", empFileName));
    IndexReader empReader = new IndexReader(empPath.toString(), numberOfRecords,
        FieldEnum.EMP_ID.getFieldLength());

    String dateFileName = file.toPath().getFileName().toString();
    Path datePath = Paths.get(
        String.format("data/output/merged/uncompressed/%s-index-%s-%s", FieldEnum.DATE.getName(),
            "uncompressed", dateFileName));

    Entry<String, Integer> latestRecord;
    BufferedWriter writer = Files
        .newBufferedWriter(Paths.get("data/output/merged/final/empId-" + inputFileName + ".txt"));
    while ((latestRecord = getLatestDate(empReader, datePath)) != null) {
      writer.append(latestRecord.getKey());
      writer.append(",");
      writer.append(String.valueOf(latestRecord.getValue()));
      writer.append("\n");
    }
    writer.close();
  }

  private Entry<String, Integer> getLatestDate(IndexReader empReader, Path datePath)
      throws IOException {
    Entry<String, ArrayList<Integer>> currentId;
    while ((currentId = empReader.getNextIndex()) != null) {
      if (currentId.getValue().size() > 1) {
        IndexReader dateReader = new IndexReader(datePath.toString(), numberOfRecords,
            FieldEnum.DATE.getFieldLength());
        while (true) {
          Entry<String, ArrayList<Integer>> currentDate = dateReader.getNextIndexReversed();
          for (Integer index : currentDate.getValue()) {
            if (currentId.getValue().contains(index)) {
              int latestIndex = currentId.getValue().indexOf(index);
              Integer latestDate = currentId.getValue().get(latestIndex);
              return new AbstractMap.SimpleEntry<>(currentId.getKey(), latestDate);
            }
          }
        }
      } else {
        Integer latestDate = currentId.getValue().get(0);
        return new AbstractMap.SimpleEntry<>(currentId.getKey(), latestDate);
      }
    }
    return null;
  }

  private String getLatestRecord(IndexReader empReader, RandomAccessFile raf) throws IOException {
    UnaryOperator<Integer> getSeek = (index) -> index * RECORD_SIZE;
    Entry<String, ArrayList<Integer>> currentId = empReader.getNextIndex();

    if (currentId == null) {
      return null;
    }
    // fetch first record
    long seekCurrentIndex = getSeek.apply(currentId.getValue().get(0));
    String currentLatestRecord = Main.getRecord(seekCurrentIndex, raf);
    // fetch next record
    ArrayList<Integer> value = currentId.getValue();
    for (int i = 1; i < value.size(); i++) {
      Integer nextIndex = value.get(i);
      long seekNextIndex = getSeek.apply(nextIndex);
      String nextRecord = Main.getRecord(seekNextIndex, raf);
      if (currentLatestRecord.compareTo(nextRecord) <= 0) {
        currentLatestRecord = nextRecord;
      }
    }
    return currentLatestRecord;

  }

  public static void eliminateDuplicates(BitMapIndex t1, BitMapIndex t2) throws IOException {

    String empFileNameT1 = t1.file.toPath().getFileName().toString();
    Path empPathT1 = Paths.get(
        String.format("data/output/merged/uncompressed/%s-index-%s-%s", FieldEnum.EMP_ID.getName(),
            "uncompressed", empFileNameT1));
    IndexReader empReaderT1 = new IndexReader(empPathT1.toString(), t1.numberOfRecords,
        FieldEnum.EMP_ID.getFieldLength());
    RandomAccessFile rafT1 = new RandomAccessFile(t1.file, "r");

    String empFileNameT2 = t2.file.toPath().getFileName().toString();
    Path empPathT2 = Paths.get(
        String.format("data/output/merged/uncompressed/%s-index-%s-%s", FieldEnum.EMP_ID.getName(),
            "uncompressed", empFileNameT2));
    IndexReader empReaderT2 = new IndexReader(empPathT2.toString(), t2.numberOfRecords,
        FieldEnum.EMP_ID.getFieldLength());
    RandomAccessFile rafT2 = new RandomAccessFile(t2.file, "r");

    BufferedWriter bufferedWriter = Files
        .newBufferedWriter(Paths.get("data/output/merged/final/records.txt"));

    String lineT1 = t1.getLatestRecord(empReaderT1,rafT1);
    String lineT2 = t2.getLatestRecord(empReaderT2,rafT2);
    while (true) {

      if(lineT1 == null && lineT2 == null) {
        break;
      }

      if(lineT1 == null) {
        while(lineT2 != null) {
          bufferedWriter.append(lineT2).append(System.lineSeparator());
          lineT2 = t2.getLatestRecord(empReaderT2,rafT2);
        }
        break;
      }

      if(lineT2 == null) {
        while(lineT1 != null) {
          bufferedWriter.append(lineT1).append(System.lineSeparator());
          lineT1 = t1.getLatestRecord(empReaderT1,rafT1);
        }
        break;
      }

      int empT1 = Integer.parseInt(lineT1.substring(0,8));
      int empT2 = Integer.parseInt(lineT2.substring(0,8));

      if(empT1 < empT2) {
        bufferedWriter.append(lineT1).append(System.lineSeparator());
        lineT1 = t1.getLatestRecord(empReaderT1,rafT1);
      }
      else if(empT1 > empT2) {
        bufferedWriter.append(lineT2).append(System.lineSeparator());
        lineT2 = t2.getLatestRecord(empReaderT2,rafT2);
      }
      else {
        String dateT1 = lineT1.substring(8, 18);
        String dateT2 = lineT2.substring(8, 18);

        if(dateT1.compareTo(dateT2) > 0) {
          bufferedWriter.append(lineT1).append(System.lineSeparator());
        }
        else {
          bufferedWriter.append(lineT2).append(System.lineSeparator());
        }
        lineT1 = t1.getLatestRecord(empReaderT1,rafT1);
        lineT2 = t2.getLatestRecord(empReaderT2,rafT2);
      }
    }
    rafT1.close();
    rafT2.close();
    bufferedWriter.close();
  }

}


