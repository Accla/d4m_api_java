package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.skvi.SmallLargeRowFilter;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Test {@link SmallLargeRowFilter}.
 */
public class TableFilterTest extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(TableFilterTest.class);

  @Test
  public void testSmallLargeRowFilter() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Connector conn = tester.getConnector();
    final String tA;
    {
      String[] names = getUniqueNames(1);
      tA = names[0];
    }
    Map<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("r", "", "C1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("r", "", "C2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("r", "", "C3"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("r", "", "C4"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("g", "", "C1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("h", "", "C1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      expect.put(new Key("h", "", "C1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("h", "", "C2"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      expect.put(new Key("h", "", "C2"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("a", "", "C1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("b"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }

    BatchScanner bs = conn.createBatchScanner(tA, Authorizations.EMPTY, 2);
    bs.setRanges(Collections.singleton(new Range()));
    IteratorSetting is = new IteratorSetting(12,SmallLargeRowFilter.class);
    SmallLargeRowFilter.setMaxColumns(is, 3);
    SmallLargeRowFilter.setMinColumns(is, 2);
    bs.addScanIterator(is);
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : bs) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);
    bs.close();
    conn.tableOperations().delete(tA);
  }



}
