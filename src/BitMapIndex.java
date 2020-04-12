import java.io.*;
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

  public int RECORD_SIZE;
  int tuplesInABuffer;
  private String inputFileName;
  private File file;
  private int numberOfRecords;
  public  int writeCount = 0;
  public  int readCount = 0;

  public BitMapIndex(String fileLocation, String inputFileName, int tuplesInABuffer,
      boolean doCleanUp)
      throws IOException {
    file = new File(fileLocation);
    this.tuplesInABuffer = tuplesInABuffer;
    this.inputFileName = inputFileName;
    this.RECORD_SIZE = Main.determineRecordSize(file);
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
    SortedMap<String, ArrayList<Integer>> genderBitVectors = new TreeMap<>();
    SortedMap<String, ArrayList<Integer>> deptBitVectors = new TreeMap<>();
    String line;
    int i = 0;

    while ((line = bufferedReader.readLine()) != null) {
      String empId = FieldEnum.EMP_ID.getValue(line);
      String gender = FieldEnum.GENDER.getValue(line);
      String dept = FieldEnum.DEPT.getValue(line);
      addRecordToIndex(empIdBitVectors, i, empId);
      addRecordToIndex(genderBitVectors, i, gender);
      addRecordToIndex(deptBitVectors, i, dept);
      i++;
      if (i % tuplesInABuffer == 0 || i == numberOfRecords) {
        int run = (int) Math.ceil(i / (float) tuplesInABuffer);
        generateIndex(FieldEnum.EMP_ID.getName(), empIdBitVectors, isCompressed, run);
        generateIndex(FieldEnum.GENDER.getName(), genderBitVectors, isCompressed, run);
        generateIndex(FieldEnum.DEPT.getName(), deptBitVectors, isCompressed, run);
      }
    }
    if(numberOfRecords < 40) readCount += 1;
    else readCount += (int)Math.ceil(numberOfRecords/40.0);

    bufferedReader.close();

  }

  private void generateIndex(String indexField,
      SortedMap<String, ArrayList<Integer>> bitVectors, boolean isCompressed, int run)
      throws IOException {
    String fileName = file.toPath().getFileName().toString();
    int localBytesWrittenCounter = 0;
    Path indexPath = Paths.get(
        String.format("data/output/index/%s/%s/%s-index-%03d-%s-%s", indexField, inputFileName,
            indexField, run,
            isCompressed ? "compressed"
                : "uncompressed", fileName));

    BufferedWriter bufferedWriter = Files.newBufferedWriter(indexPath);
    for (Entry<String, ArrayList<Integer>> entry : bitVectors.entrySet()) {
      bufferedWriter.append(entry.getKey());
      int loopBytesWrittenCounter = entry.getKey().length();
      for (Integer runLength : entry.getValue()) {
        if (isCompressed) {
          loopBytesWrittenCounter += encodeRunLength(bufferedWriter, runLength);
        } else {
          loopBytesWrittenCounter += encodeRunLengthUnCompressed(bufferedWriter, runLength);
        }
      }
      bufferedWriter.append(System.lineSeparator());
      loopBytesWrittenCounter += System.lineSeparator().length();
      localBytesWrittenCounter += loopBytesWrittenCounter;
    }
    writeCount += (int) Math.ceil(localBytesWrittenCounter/4096.0);
    bitVectors.clear();
    bufferedWriter.close();
  }

  public void mergePartialIndexes(FieldEnum fieldEnum)
      throws IOException {
    String fileName = file.toPath().getFileName().toString();
    int localBytesWrittenBytesCounter = 0;
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
      partialIndexReader.getNextIndex(this);
    }

    String minIndex;
    while ((minIndex = getMinOfAllPartialIndexes(readerList))
        != null) {
      bufferedWriter.append(minIndex);
      int loopBytesWrittenCounter = 0;
      loopBytesWrittenCounter += minIndex.length();
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
            loopBytesWrittenCounter += encodeRunLength(bufferedWriter, run);
          }
          // point to the next index in the partialReader
          reader.getNextIndex(this);
        }
      }
      bufferedWriter.append(System.lineSeparator());
      loopBytesWrittenCounter += System.lineSeparator().length();
      localBytesWrittenBytesCounter += loopBytesWrittenCounter;
    }
    writeCount += (int) Math.ceil(localBytesWrittenBytesCounter/4096.0);
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
    int localBytesWrittenCounter = 0;
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
    while ((entry = partialIndexReader.getNextIndex(this)) != null) {
      int loopBytesWrittenCounter = 0;
      bufferedWriterUncompressed.append(entry.getKey());
      int sum = 0;
      for (Integer run : entry.getValue()) {
        sum += run + 1;
        loopBytesWrittenCounter += encodeRunLengthUnCompressed(bufferedWriterUncompressed, run);
      }
      while (sum < numberOfRecords) {
        bufferedWriterUncompressed.append('0');
        loopBytesWrittenCounter += 1;
        sum++;
      }
      bufferedWriterUncompressed.append(System.lineSeparator());
      loopBytesWrittenCounter += System.lineSeparator().length();
      localBytesWrittenCounter += loopBytesWrittenCounter;
    }
    writeCount += (int)Math.ceil(localBytesWrittenCounter/4096.0);
    bufferedWriterUncompressed.close();
  }

  private int encodeRunLength(BufferedWriter bufferedWriter, int i) throws IOException {
    int j = log2(i);
    int localBytesWrittenCounter = 0;
    // j - 1 bits to 1
    for (int bit = 0; bit < j; bit++) {
      bufferedWriter.append('1');
      localBytesWrittenCounter += 1;
    }
    // last bit to zero
    bufferedWriter.append('0');
    // append the actual run length
    bufferedWriter.append(Integer.toBinaryString(i));
    localBytesWrittenCounter += (1 + Integer.toBinaryString(i).length());
    return localBytesWrittenCounter;
  }

  private int encodeRunLengthUnCompressed(BufferedWriter bufferedWriter, int run)
      throws IOException {
    int localBytesWrittenCounter = 0;
    // set run bits to 0
    for (int i = 0; i < run; i++) {
      bufferedWriter.append('0');
      localBytesWrittenCounter += 1;
    }
    // set the final bit to 1
    bufferedWriter.append('1');
    return localBytesWrittenCounter + 1;
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

      int empT1 = Integer.parseInt(lineT1.substring(0, 8));
      int empT2 = Integer.parseInt(lineT2.substring(0, 8));

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


