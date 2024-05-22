package edu.mit.ll.graphulo;

import com.google.common.base.Strings;
import edu.mit.ll.graphulo.skvi.RemoteSourceIterator;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static edu.mit.ll.graphulo.AccumuloApiTest.NO_VISIBILITY;
import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_BYTES;

/**
 * Holds tests that reproduce Accumulo bugs.
 */
public class AccumuloBugTest extends AccumuloTestBase {
  private static final byte[] BIG_CHUNK = new byte[1000];
  static {
    Arrays.fill(AccumuloBugTest.BIG_CHUNK, (byte)42);
  }
  private static final Logger log = LoggerFactory.getLogger(AccumuloBugTest.class);

  @BeforeClass
  public static void printWarning() {
    log.warn("This class will break Accumulo. Do not run on a production instance.");
  }

  @Test
  public void testNoDeadlockWithFewScans() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    // less than 16 tablets will not deadlock.
    testScansOnTablets(14);
  }

  /**
   * This will deadlock an Accumulo tablet server if numtablets >= 16 and there is only one tablet server.
   */
//  @Ignore("KnownBug ACCUMULO-3975")
  @Test
  public void testDeadlockWithManyScans() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    // 16 is the default max number of threads allowed in the read-ahead pool. >16 tablets means a deadlock is possible.
    testScansOnTablets(50);
  }

  private void testScansOnTablets(final int numtablets) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Connector connector = tester.getConnector();
    final String tA = getUniqueNames(1)[0];

    // create a table with given number of tablets, inserting one entry into each tablet
    Map<Key, Value> input = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 0; i < numtablets; i++) {
      String row = Strings.padStart(Integer.toString(1+2*i), 2, '0');                  // 01, 03, 05, ...
      input.put(new Key(row, "", ""), new Value(Integer.toString(1+2*i).getBytes(StandardCharsets.UTF_8)));
      if (i > 0)
        splits.add(new Text(Strings.padStart(Integer.toString(2 * (i - 1)), 2, '0'))); // --, 02, 04, ...
    }
    TestUtil.createTestTable(connector, tA, splits, input);

    IteratorSetting itset = RemoteSourceIterator.iteratorSetting(
        25, connector.getInstance().getZooKeepers(), 5000, connector.getInstance().getInstanceName(),
        tA, connector.whoami(), new String(tester.getPassword().getPassword(), StandardCharsets.UTF_8), Authorizations.EMPTY, null, null,
        false, null);

    BatchScanner bs = connector.createBatchScanner(tA, Authorizations.EMPTY, numtablets); // scan every tablet
    bs.setRanges(Collections.singleton(new Range()));
    bs.addScanIterator(itset);

    SortedMap<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    try {
      for (Map.Entry<Key, Value> entry : bs) {                    // DEADLOCK TabletServer!
        actual.put(entry.getKey(), entry.getValue());
        log.debug("Entry "+actual.size()+": "+entry.getKey()+"    "+entry.getValue());
      }
    } finally {
      bs.close();
    }
    Assert.assertEquals(input, actual);

    connector.tableOperations().delete(tA);
  }


  /**
   * Break Accumulo by creating too low a split threshold, resulting in too many files.
   */
  @Test
  public void testSplitDuringWriteHeavy() throws TableNotFoundException, AccumuloSecurityException, AccumuloException, InterruptedException {
    Connector conn = tester.getConnector();
    TableOperations tops = conn.tableOperations();
    final String t = getUniqueNames(1)[0];
    TestUtil.createTestTable(conn, t);

    String threshold = "50K";
    try {
      tops.setProperty(t, "table.split.threshold", threshold);
    } catch (AccumuloException | AccumuloSecurityException e) {
      Assert.fail("cannot set table.split.threshold property for " + t + " to "+threshold);
    }

    {
      BatchWriterConfig bwc = new BatchWriterConfig();
      bwc.setMaxWriteThreads(2);
      bwc.setMaxMemory(50000);
      BatchWriter bw = conn.createBatchWriter(t, new BatchWriterConfig());

      for (int i = 0; i < 2000005; i++) {
        StringBuilder sb = new StringBuilder(Integer.toString(i));
        String vStr = sb.toString();
        byte[] v = vStr.getBytes(StandardCharsets.UTF_8);
        String rowStr = sb.reverse().toString();
        byte[] row = rowStr.getBytes(StandardCharsets.UTF_8);

        Mutation m = new Mutation(row);
        long ts = System.currentTimeMillis();
        m.put(EMPTY_BYTES, EMPTY_BYTES, NO_VISIBILITY, ts, BIG_CHUNK);
        bw.addMutation(m);
//        expect.put(new Key(row, EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES, ts, false), new Value(v));

        if (i % 100000 == 0) {
//          tops.flush(t, null, null, false);
//          Thread.sleep(500);
        }
//        if (i % 200000 == 0) {
//          SortedSet<Text> splits = new TreeSet<>();
//          splits.add(new Text(Integer.toString(i/50000).getBytes(StandardCharsets.UTF_8)));
//          tops.addSplits(t, splits);
//        }
      }
      bw.close();

//      BatchScanner scanner = conn.createBatchScanner(t, Authorizations.EMPTY, 2);
//      scanner.setRanges(Collections.singleton(new Range()));
//      for (Map.Entry<Key, Value> entry : scanner) {
//        actual.put(entry.getKey(), entry.getValue());
//      }
//      scanner.close();
//      System.out.println("expect.size="+expect.size()+" actual.size="+actual.size());
//      System.out.println("expect==actual is "+expect.equals(actual));
//      System.out.println("splits are "+tops.listSplits(t));

//      Set<Map.Entry<Key, Value>> es = expect.entrySet();
//      es.removeAll(actual.entrySet());
//      System.out.println("expect - actual is "+es);

//      Assert.assertEquals(expect, actual);
    }
    conn.tableOperations().delete(t);
  }


}
