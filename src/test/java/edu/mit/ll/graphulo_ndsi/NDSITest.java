package edu.mit.ll.graphulo_ndsi;

import edu.mit.ll.graphulo.examples.ExampleUtil;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Test NDSI Query
 */
public class NDSITest extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(NDSITest.class);

  /**
   *
   */
  @Test
  public void testNDSIQuery() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tR;
    {
      String[] names = getUniqueNames(2);
      tA = names[0];
      tR = names[1];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
//    {
//      Map<Key, Value> input = new HashMap<>();
//      input.put(new Key("v0", "", "v1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
//      input.put(new Key("v1", "", "v2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
//      input.put(new Key("v2", "", "v0"), new Value("4".getBytes(StandardCharsets.UTF_8)));
//      SortedSet<Text> splits = new TreeSet<>();
//      splits.add(new Text("v15"));
//      TestUtil.createTestTable(conn, tA, splits, input);
//    }

    File file = ExampleUtil.getDataFile("ndsi_snippet.csv");

    NDSIIngester ingester = new NDSIIngester(conn);
    ingester.ingestFile(file, tA, true);
    long
        minX = 0,
        minY = 50,
        maxX = 1,
        maxY = 149;
    double binsizeX = 1, binsizeY = 25;

    NDSIGraphulo graphulo = new NDSIGraphulo(conn, tester.getPassword());
    long cnt = graphulo.windowSubset(tA, tR, minX, minY, maxX, maxY, binsizeX, binsizeY);
    log.info("Number of entries processed: " + cnt);

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
      System.out.println(entry.getKey().toStringNoTime()+" -> "+entry.getValue());
    }
    scanner.close();
//    Assert.assertEquals(expect, actual);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tR);
  }



}
