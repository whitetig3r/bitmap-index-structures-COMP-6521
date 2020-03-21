import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class Main {

  public static void main(String[] args) throws IOException {
    // write your code here
    System.out.println("Creating indexes for empId, date, gender");
    Instant executionStart = Instant.now();
    BitMapIndex bitMapIndex = new BitMapIndex(args[0], 10000, true);
    bitMapIndex.createIndex(true);
    Instant partialIndexFinish = Instant.now();
    long timeElapsedPartialIndex = Duration.between(executionStart, partialIndexFinish).toMillis();
    System.out.printf("Partial Indexes created in %f seconds\n", timeElapsedPartialIndex / 1000.0);

    bitMapIndex
        .mergePartialIndexes(FieldEnum.EMP_ID, true);
    bitMapIndex
        .mergePartialIndexes(FieldEnum.DATE, true);
    bitMapIndex
        .mergePartialIndexes(FieldEnum.GENDER, true);
    Instant compressedIndexFinish = Instant.now();
    long timeElapsedPartialMerge = Duration.between(partialIndexFinish, compressedIndexFinish).toMillis();
    System.out.printf("Partial Indexes(compressed) merged in %f seconds\n", timeElapsedPartialMerge / 1000.0);
    System.out.println("Uncompressing indexes");
    bitMapIndex.unCompressRuns(FieldEnum.EMP_ID);
    bitMapIndex.unCompressRuns(FieldEnum.DATE);
bitMapIndex.unCompressRuns(FieldEnum.GENDER);
    Instant executionFinish = Instant.now();

    long timeElapsed = Duration.between(executionStart, executionFinish).toMillis();
    System.out.printf("total time to create Indexes - %f seconds\n", timeElapsed / 1000.0);

//        bitMapIndex.createIndex(false);
  }
}
