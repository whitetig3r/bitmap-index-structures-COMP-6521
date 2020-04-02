import static org.junit.Assert.assertArrayEquals;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;

import org.junit.Test;

class MainTest {
@Test
  void duplicateRemoval() throws IOException{
  BufferedReader br1 = Files.newBufferedReader(Paths.get("data/input/custom-test.txt"));
  BufferedReader br2 = Files.newBufferedReader(Paths.get("data/input/custom-test-2.txt"));
  BufferedReader br3 = Files.newBufferedReader(Paths.get("data/output/merged/final/records.txt"));
  ArrayList<String>  lines1 = br1.lines().sorted().collect(Collectors.toCollection(ArrayList::new));
  ArrayList<String>  lines2 = br2.lines().sorted().collect(Collectors.toCollection(ArrayList::new));
  ArrayList<String>  lines3 = br3.lines().collect(Collectors.toCollection(ArrayList::new));
  lines1.addAll(lines2);
  Collections.sort(lines1);
  lines2.clear();
  String currentRecord = lines1.get(0);
  for (int i = 1; i < lines1.size(); i++) {
    String nextRecord = lines1.get(i);
    if(nextRecord.startsWith(currentRecord.substring(0, 8))){
      currentRecord = nextRecord;
      if(i == lines1.size() - 1){
        lines2.add(currentRecord);
      }
    }
    else{
      lines2.add(currentRecord);
      currentRecord = nextRecord;
    }
  }
  assertArrayEquals(lines2.toArray(),lines3.toArray());

}
}