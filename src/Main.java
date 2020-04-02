import java.io.*;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;

public class Main {
  public static String getRecord(long indexT1, RandomAccessFile raf) throws IOException {
    raf.seek(indexT1);
    return raf.readLine();
  }

  public static int determineRecordSize(File file) throws IOException {
    RandomAccessFile raf = new RandomAccessFile(file, "r");
    byte[] testBuf = new byte[101];
    raf.read(testBuf);
    return testBuf[100] == 10 ? 101 : 100;
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

    BitMapIndex.eliminateDuplicates(bitMapIndexT1,bitMapIndexT2);
    Instant executionFinish = Instant.now();

    long timeElapsed = Duration.between(executionStart, executionFinish).toMillis();
    System.out.printf("total time to create Indexes - %f seconds\n", timeElapsed / 1000.0);
  }
}
