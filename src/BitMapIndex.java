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
  public String inputFileName;
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
      if(i%40 == 0) readCount += 1;
      i++;
      if (i % tuplesInABuffer == 0 || i == numberOfRecords) {
        int run = (int) Math.ceil(i / (float) tuplesInABuffer);
        generateIndex(FieldEnum.EMP_ID.getName(), empIdBitVectors, isCompressed, run);
        generateIndex(FieldEnum.GENDER.getName(), genderBitVectors, isCompressed, run);
        generateIndex(FieldEnum.DEPT.getName(), deptBitVectors, isCompressed, run);
      }
    }

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
    int localBytesWrittenCounter = 0;
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
      localBytesWrittenCounter += loopBytesWrittenCounter;
    }
    writeCount += (int) Math.ceil(localBytesWrittenCounter/4096.0);
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
    int recordsWrittenCounter = 0;
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
    RandomAccessFile raf = new RandomAccessFile(file.toString(),"r");
    BufferedWriter bufferedWriterIntermediate = null;
    if(fieldEnum == FieldEnum.EMP_ID) {
      bufferedWriterIntermediate = Files
              .newBufferedWriter(Paths.get(String.format("data/output/merged/final/records-%s.txt",inputFileName, true)));
    }
    while ((entry = partialIndexReader.getNextIndex(this)) != null) {
      int loopBytesWrittenCounter = 0;
      bufferedWriterUncompressed.append(entry.getKey());
      loopBytesWrittenCounter += entry.getKey().length();
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
      if(fieldEnum == FieldEnum.EMP_ID) {
        Integer acc = 0;
        String latestRecord = null;
        for (Integer currentRun : entry.getValue()) {
          acc = acc + currentRun + 1;
          UnaryOperator<Integer> getSeek = (index) -> (index - 1) * RECORD_SIZE;
          long seekCurrentIndex = getSeek.apply(acc);
          String currentLatestRecord = Main.getRecord(seekCurrentIndex, raf, this);
          if (currentLatestRecord == null) break;
          if (latestRecord == null || (latestRecord.compareTo(currentLatestRecord) < 0)) {
            latestRecord = currentLatestRecord;
          }
        }
        bufferedWriterIntermediate.append(latestRecord).append(System.lineSeparator());
        recordsWrittenCounter += 1;
        if(recordsWrittenCounter == 40) {
          recordsWrittenCounter = 0;
          writeCount += 1;
        }
      }
      loopBytesWrittenCounter += System.lineSeparator().length();
      localBytesWrittenCounter += loopBytesWrittenCounter;
    }
    if(fieldEnum == FieldEnum.EMP_ID) bufferedWriterIntermediate.close();
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

}


