import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

public class Main {
  public static int getRecordReads = 0;
  public static String getRecord(long indexT1, RandomAccessFile raf, BitMapIndex bmp) throws IOException {
    raf.seek(indexT1);
    getRecordReads += 1;
    if(getRecordReads == 40) {
      bmp.readCount += 1;
      getRecordReads = 0;
    }
    return raf.readLine();
  }

  public static int determineRecordSize(File file) throws IOException {
    RandomAccessFile raf = new RandomAccessFile(file, "r");
    byte[] testBuf = new byte[101];
    raf.read(testBuf);
    return testBuf[100] == 10 ? 101 : 100;
  }



  public static void main(String[] args) throws IOException {
    System.out.println("Creating indexes for empId, department, gender");

    Instant executionStart = Instant.now();

    BitMapIndex bitMapIndexT1 = new BitMapIndex(args[0], "T1", 7000, true);
    bitMapIndexT1.createIndex(true);

    BitMapIndex bitMapIndexT2 = new BitMapIndex(args[1], "T2", 7000, true);
    bitMapIndexT2.createIndex(true);

    Instant partialIndexFinish = Instant.now();
    long timeElapsedPartialIndex = Duration.between(executionStart, partialIndexFinish).toMillis();

    System.out.printf("Partial Indexes created in %f seconds\n", timeElapsedPartialIndex / 1000.0);

    bitMapIndexT1
        .mergePartialIndexes(FieldEnum.EMP_ID);
    bitMapIndexT1
        .mergePartialIndexes(FieldEnum.GENDER);
    bitMapIndexT1
        .mergePartialIndexes(FieldEnum.DEPT);

    bitMapIndexT2
            .mergePartialIndexes(FieldEnum.EMP_ID);
    bitMapIndexT2
            .mergePartialIndexes(FieldEnum.GENDER);
    bitMapIndexT2
        .mergePartialIndexes(FieldEnum.DEPT);

    System.out.print("\nAfter Compressing T1 -- \n");
    System.out.printf("Reads: %d  Writes: %d\n",bitMapIndexT1.readCount, bitMapIndexT1.writeCount);
    System.out.print("After Compressing T2 -- \n");
    System.out.printf("Reads: %d  Writes: %d\n",bitMapIndexT2.readCount, bitMapIndexT2.writeCount);

    int readsAfterCompressingT1 = bitMapIndexT1.readCount;
    int writesAfterCompressingT1 = bitMapIndexT1.writeCount;
    int readsAfterCompressingT2 = bitMapIndexT2.readCount;
    int writesAfterCompressingT2 = bitMapIndexT2.writeCount;

    Instant compressedIndexFinish = Instant.now();

    long timeElapsedPartialMerge = Duration.between(partialIndexFinish, compressedIndexFinish).toMillis();

    System.out.printf("\nPartial Indexes(compressed) merged in %f seconds\n", timeElapsedPartialMerge / 1000.0);
    System.out.println("Uncompressing indexes...");

    bitMapIndexT1.unCompressRuns(FieldEnum.EMP_ID);
    bitMapIndexT1.unCompressRuns(FieldEnum.GENDER);
    bitMapIndexT1.unCompressRuns(FieldEnum.DEPT);

    bitMapIndexT2.unCompressRuns(FieldEnum.EMP_ID);
    bitMapIndexT2.unCompressRuns(FieldEnum.GENDER);
    bitMapIndexT2.unCompressRuns(FieldEnum.DEPT);

    int readsAfterUncompressingT1 = bitMapIndexT1.readCount;
    int writesAfterUncompressingT1 = bitMapIndexT1.writeCount;
    int readsAfterUncompressingT2 = bitMapIndexT2.readCount;
    int writesAfterUncompressingT2 = bitMapIndexT2.writeCount;

    System.out.print("\nAfter Uncompressing and eliminating duplicates from T1 -- \n");
    System.out.printf("Reads: %d  Writes: %d\n",readsAfterUncompressingT1-readsAfterCompressingT1,writesAfterUncompressingT1-writesAfterCompressingT1);
    System.out.print("After Uncompressing and eliminating duplicates from T2 -- \n");
    System.out.printf("Reads: %d  Writes: %d\n",readsAfterUncompressingT2-readsAfterCompressingT2,writesAfterUncompressingT2-writesAfterCompressingT2);

    Instant executionFinishUncompressed = Instant.now();
    long timeElapsedUncompressed = Duration.between(compressedIndexFinish, executionFinishUncompressed).toMillis();
    System.out.printf("\nTotal time to create Uncompressed Indexes & Eliminate Duplicates - %f seconds\n", timeElapsedUncompressed / 1000.0);
    int[] ioCounts = eliminateDuplicates(String.format("data/output/merged/final/records-%s.txt",bitMapIndexT1.inputFileName), String.format("data/output/merged/final/records-%s.txt",bitMapIndexT2.inputFileName));
    Instant executionFinish = Instant.now();
    System.out.printf("Total Reads: %d | Total Writes: %d\n", bitMapIndexT1.readCount+bitMapIndexT2.readCount+ioCounts[0], bitMapIndexT1.writeCount+bitMapIndexT2.writeCount+ioCounts[1]);
    long timeElapsed = Duration.between(executionStart, executionFinish).toMillis();
    System.out.printf("\nTotal time to create Indexes - %f seconds\n", timeElapsed / 1000.0);
    printFileSizes();
  }

  private static void printFileSizes() throws IOException {
    String compressed = "data/output/merged/compressed";
    String uncompressed = "data/output/merged/uncompressed";
    String finalOutput = "data/output/merged/final/records.txt";
    System.out.println("\nCOMPRESSED INDEX SIZES --");
    Files.walk(Paths.get(compressed))
            .filter(Files::isRegularFile)
            .map(Path::toFile)
            .forEach(file -> System.out.println(file.getName() + " --> SIZE: " + file.length()/1024.0 + " KB"));
    System.out.println("\nUNCOMPRESSED INDEX SIZES --");
    Files.walk(Paths.get(uncompressed))
            .filter(Files::isRegularFile)
            .map(Path::toFile)
            .forEach(file -> System.out.println(file.getName() + " --> SIZE: " + file.length()/1024.0 + " KB"));
    System.out.println("\nFINAL RECORD FILE --");
    System.out.println(new File(finalOutput).length()/1024.0 + " KB");
  }

  private static int[] eliminateDuplicates(String file1, String file2) throws IOException {

    BufferedWriter bufferedWriter = Files
            .newBufferedWriter(Paths.get("data/output/merged/final/records.txt"));

    BufferedReader t1 = Files.newBufferedReader(Paths.get(file1));
    BufferedReader t2 = Files.newBufferedReader(Paths.get(file2));

    int recordsWriteCounter = 0;
    int recordsReadCounter = 0;

    int readIO = 0;
    int writeIO = 0;

    String lineT1 = t1.readLine();
    String lineT2 = t2.readLine();

    recordsReadCounter += 2;
    while (true) {

      if(lineT1 == null && lineT2 == null) {
        break;
      }

      if(lineT1 == null) {
        while(lineT2 != null) {
          bufferedWriter.append(lineT2).append(System.lineSeparator());
          recordsWriteCounter += 1;
          lineT2 = t2.readLine();
          recordsReadCounter += 1;
          if(recordsReadCounter % 40 == 0) {
            readIO += 1;
          }
          if(recordsWriteCounter % 40 == 0) {
            writeIO += 1;
          }
        }
        break;
      }

      if(lineT2 == null) {
        while(lineT1 != null) {
          bufferedWriter.append(lineT1).append(System.lineSeparator());
          lineT1 = t1.readLine();
          recordsWriteCounter +=1;
          recordsReadCounter +=1;
          if(recordsReadCounter % 40 == 0) {
            readIO += 1;
          }
          if(recordsWriteCounter % 40 == 0) {
            writeIO += 1;
          }
        }
        break;
      }

      int empT1 = Integer.parseInt(lineT1.substring(0, 8));
      int empT2 = Integer.parseInt(lineT2.substring(0, 8));

      if(empT1 < empT2) {
        bufferedWriter.append(lineT1).append(System.lineSeparator());
        lineT1 = t1.readLine();
        recordsWriteCounter +=1;
        recordsReadCounter +=1;
        if(recordsReadCounter % 40 == 0) {
          readIO += 1;
        }
        if(recordsWriteCounter % 40 == 0) {
          writeIO += 1;
        }
      }
      else if(empT1 > empT2) {
        bufferedWriter.append(lineT2).append(System.lineSeparator());
        lineT2 = t2.readLine();
        recordsWriteCounter +=1;
        recordsReadCounter +=1;
        if(recordsReadCounter % 40 == 0) {
          readIO += 1;
        }
        if(recordsWriteCounter % 40 == 0) {
          writeIO += 1;
        }
      }
      else {
        String dateT1 = lineT1.substring(8, 18);
        String dateT2 = lineT2.substring(8, 18);

        if(dateT1.compareTo(dateT2) > 0) {
          bufferedWriter.append(lineT1).append(System.lineSeparator());
          recordsWriteCounter +=1;
          if(recordsWriteCounter % 40 == 0) {
            writeIO += 1;
          }
        }
        else {
          bufferedWriter.append(lineT2).append(System.lineSeparator());
          recordsWriteCounter +=1;
          if(recordsWriteCounter % 40 == 0) {
            writeIO += 1;
          }
        }
        lineT1 = t1.readLine();
        lineT2 = t2.readLine();
        recordsReadCounter += 2;
        if(recordsReadCounter % 40 == 0) {
          readIO += 1;
        }
      }
    }
    t1.close();
    t2.close();
    bufferedWriter.close();
    return new int[]{readIO, writeIO};
  }
}
