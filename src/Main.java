import java.io.*;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;

public class Main {
  public static String getRecord(long indexT1, RandomAccessFile raf, BitMapIndex bmp) throws IOException {
    raf.seek(indexT1);
    bmp.dupElimRecordReads += 1;
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

    System.out.print("\nAfter Uncompressing T1 -- \n");
    System.out.printf("Reads: %d  Writes: %d\n",readsAfterUncompressingT1-readsAfterCompressingT1,writesAfterUncompressingT1-writesAfterCompressingT1);
    System.out.print("After Uncompressing T2 -- \n");
    System.out.printf("Reads: %d  Writes: %d\n",readsAfterUncompressingT2-readsAfterCompressingT2,writesAfterUncompressingT2-writesAfterCompressingT2);

    Instant executionFinishUncompressed = Instant.now();
    long timeElapsedUncompressed = Duration.between(compressedIndexFinish, executionFinishUncompressed).toMillis();
    System.out.printf("\nTotal time to create Uncompressed Indexes - %f seconds\n", timeElapsedUncompressed / 1000.0);

    System.out.println("Eliminating duplicates and writing the record file back...\n");

    BitMapIndex.eliminateDuplicates(bitMapIndexT1,bitMapIndexT2);
    Instant executionFinish = Instant.now();

    System.out.print("After Eliminating Duplicates T1 & T2 (final writes not counted) -- \n");
    System.out.printf("Reads from Uncompressed Index File: %d\nReads from Record File: %d\n\n", bitMapIndexT1.dupElimIndexReads+bitMapIndexT2.dupElimIndexReads, bitMapIndexT1.dupElimRecordReads+bitMapIndexT2.dupElimRecordReads);

    bitMapIndexT1.readCount += (bitMapIndexT1.dupElimIndexReads + bitMapIndexT1.dupElimRecordReads);
    bitMapIndexT2.readCount += (bitMapIndexT2.dupElimIndexReads + bitMapIndexT2.dupElimRecordReads);


    System.out.printf("Total Reads: %d | Total Writes: %d\n", bitMapIndexT1.readCount+bitMapIndexT2.readCount, bitMapIndexT1.writeCount+bitMapIndexT2.writeCount);
    long timeElapsed = Duration.between(executionStart, executionFinish).toMillis();
    System.out.printf("\nTotal time to create Indexes - %f seconds\n", timeElapsed / 1000.0);
  }
}
