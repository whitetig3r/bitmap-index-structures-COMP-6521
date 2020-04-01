import java.io.*;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;

public class Main {
  public final static int RECORD_SIZE = 101;

  public static void reconstructRecordFile(String t1, String t2) throws IOException {
    String lineT1, lineT2;

    BufferedReader bufferedReaderT1 =
            Files.newBufferedReader(new File("data/output/merged/final/empId-T1.txt").toPath());
    BufferedReader bufferedReaderT2 =
            Files.newBufferedReader(new File("data/output/merged/final/empId-T2.txt").toPath());
    RandomAccessFile rafT1 = new RandomAccessFile(t1, "r");
    RandomAccessFile rafT2 = new RandomAccessFile(t2, "r");
    BufferedWriter bufferedWriter =
            Files.newBufferedWriter(new File("data/output/merged/final/records.txt").toPath());

    lineT1 = bufferedReaderT1.readLine();
    lineT2 = bufferedReaderT2.readLine();

    while (true) {

      if(lineT1 == null && lineT2 == null) {
        break;
      }

      if(lineT1 == null) {
        long index;
        while(lineT2 != null) {
          index = Long.parseLong(lineT2.split(",")[1]) * RECORD_SIZE;
          bufferedWriter.append(getRecord(index, rafT2)).append(System.lineSeparator());
          lineT2 = bufferedReaderT2.readLine();
        }
        break;
      }

      if(lineT2 == null) {
        long index;
        while(lineT1 != null) {
          index = Long.parseLong(lineT1.split(",")[1]) * RECORD_SIZE;
          bufferedWriter.append(getRecord(index, rafT1)).append(System.lineSeparator());
          lineT1 = bufferedReaderT1.readLine();
        }
        break;
      }

      int empT1 = Integer.parseInt(lineT1.split(",")[0]);
      int empT2 = Integer.parseInt(lineT2.split(",")[0]);
      long indexT1 = Long.parseLong(lineT1.split(",")[1]) * RECORD_SIZE;
      long indexT2 = Long.parseLong(lineT2.split(",")[1]) * RECORD_SIZE;
      String recordT1 = getRecord(indexT1, rafT1);
      String recordT2 = getRecord(indexT2, rafT2);

      if(empT1 < empT2) {
        bufferedWriter.append(recordT1).append(System.lineSeparator());
        lineT1 = bufferedReaderT1.readLine();
      }
      else if(empT1 > empT2) {
        bufferedWriter.append(recordT2).append(System.lineSeparator());
        lineT2 = bufferedReaderT2.readLine();
      }
      else {
        String dateT1 = recordT1.substring(8, 18);
        String dateT2 = recordT2.substring(8, 18);

        if(dateT1.compareTo(dateT2) > 0) {
          bufferedWriter.append(recordT1).append(System.lineSeparator());
        }
        else {
          bufferedWriter.append(recordT2).append(System.lineSeparator());
        }
        lineT1 = bufferedReaderT1.readLine();
        lineT2 = bufferedReaderT2.readLine();
      }
    }

    bufferedWriter.close();
    bufferedReaderT1.close();
    bufferedReaderT2.close();
  }

  public static String getRecord(long indexT1, RandomAccessFile raf) throws IOException {
    raf.seek(indexT1);
    return raf.readLine();
  }

  public static void main(String[] args) throws IOException {
    System.out.println("Creating indexes for empId, date, gender");

    Instant executionStart = Instant.now();

    BitMapIndex bitMapIndexT1 = new BitMapIndex(args[0], "T1", 7000, true);
    bitMapIndexT1.createIndex(true);

    BitMapIndex bitMapIndexT2 = new BitMapIndex(args[1], "T2", 7000, true);
    bitMapIndexT2.createIndex(true);

    Instant partialIndexFinish = Instant.now();
    long timeElapsedPartialIndex = Duration.between(executionStart, partialIndexFinish).toMillis();

    System.out.printf("Partial Indexes created in %f seconds\n", timeElapsedPartialIndex / 1000.0);

    bitMapIndexT1
        .mergePartialIndexes(FieldEnum.EMP_ID, true);
    bitMapIndexT1
        .mergePartialIndexes(FieldEnum.DATE, true);
    bitMapIndexT1
        .mergePartialIndexes(FieldEnum.GENDER, true);
    bitMapIndexT1
        .mergePartialIndexes(FieldEnum.DEPT, true);

    bitMapIndexT2
            .mergePartialIndexes(FieldEnum.EMP_ID, true);
    bitMapIndexT2
            .mergePartialIndexes(FieldEnum.DATE, true);
    bitMapIndexT2
            .mergePartialIndexes(FieldEnum.GENDER, true);
    bitMapIndexT2
        .mergePartialIndexes(FieldEnum.DEPT, true);


    Instant compressedIndexFinish = Instant.now();

    long timeElapsedPartialMerge = Duration.between(partialIndexFinish, compressedIndexFinish).toMillis();

    System.out.printf("Partial Indexes(compressed) merged in %f seconds\n", timeElapsedPartialMerge / 1000.0);
    System.out.println("Uncompressing indexes");

    bitMapIndexT1.unCompressRuns(FieldEnum.EMP_ID);
    bitMapIndexT1.unCompressRuns(FieldEnum.DATE);
    bitMapIndexT1.unCompressRuns(FieldEnum.GENDER);
    bitMapIndexT1.unCompressRuns(FieldEnum.DEPT);

    bitMapIndexT2.unCompressRuns(FieldEnum.EMP_ID);
    bitMapIndexT2.unCompressRuns(FieldEnum.DATE);
    bitMapIndexT2.unCompressRuns(FieldEnum.GENDER);
    bitMapIndexT2.unCompressRuns(FieldEnum.DEPT);

    Instant executionFinishUncompressed = Instant.now();
    long timeElapsedUncompressed = Duration.between(compressedIndexFinish, executionFinishUncompressed).toMillis();
    System.out.printf("total time to create Uncompressed Indexes - %f seconds\n", timeElapsedUncompressed / 1000.0);

    bitMapIndexT1.eliminateDuplicates();
    bitMapIndexT2.eliminateDuplicates();
    Instant executionFinish = Instant.now();

    reconstructRecordFile(args[0], args[1]);

    long timeElapsed = Duration.between(executionStart, executionFinish).toMillis();
    System.out.printf("total time to create Indexes - %f seconds\n", timeElapsed / 1000.0);
  }
}
