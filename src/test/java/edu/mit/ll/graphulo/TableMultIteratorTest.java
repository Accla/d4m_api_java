package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.skvi.TwoTableIterator;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.accumulo.core.iterators.user.ColumnSliceFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Test {@link TwoTableIterator}.
 */
public class TableMultIteratorTest extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(TableMultIteratorTest.class);

  /**
   * <pre>
   *      C1 C2         C1 C2 C3
   * A1 [ 2  2 ] * B1 [    3  3  ] =
   * A2 [ 2    ]   B2 [ 3  3     ]
   *
   *      A1 A2         B1 B2        B1  B2
   * C1 [ 2  2 ]   C1 [    3 ]  A1 [ 6   6,6 ]
   * C2 [ 2    ]   C2 [ 3  3 ]  A2 [     6   ]
   *               C3 [ 3    ]
   *
   * </pre>
   */
  @Test
  public void testDotMultIteratorScan() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    String[] names = getUniqueNames(3);

    final String tableNameA = names[0];
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("C1", "", "A1"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("C2", "", "A1"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("C1", "", "A2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      TestUtil.createTestTable(conn, tableNameA, null, input);
    }

    final String tableNameBT = names[1];
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("C2", "", "B1"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("C3", "", "B1"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("C1", "", "B2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("C2", "", "B2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      TestUtil.createTestTable(conn, tableNameBT, null, input);
    }

    final String tableNameC = names[2];
    TestUtil.createTestTable(conn, tableNameC, null);

    Scanner scanner = conn.createScanner(tableNameC, Authorizations.EMPTY);
    Map<String, String> itprops = new HashMap<>();
    itprops.put("AT.instanceName", conn.getInstance().getInstanceName());
    itprops.put("AT.tableName", tableNameA);
    itprops.put("AT.zookeeperHost", conn.getInstance().getZooKeepers());
    itprops.put("AT.username", tester.getUsername());
    itprops.put("AT.password", new String(tester.getPassword().getPassword(), StandardCharsets.UTF_8));
    itprops.put("B.instanceName", conn.getInstance().getInstanceName());
    itprops.put("B.tableName", tableNameBT);
    itprops.put("B.zookeeperHost", conn.getInstance().getZooKeepers());
    itprops.put("B.username", tester.getUsername());
    itprops.put("B.password", new String(tester.getPassword().getPassword(), StandardCharsets.UTF_8));
    itprops.put("dotmode", TwoTableIterator.DOTMODE.ROW.name());
    IteratorSetting itset = new IteratorSetting(25, TwoTableIterator.class, itprops);
    scanner.addScanIterator(itset);
//        scanner.addScanIterator(new IteratorSetting(26, DebugIterator.class, Collections.<String,String>emptyMap()));

    // compact
//        List<IteratorSetting> listset = new ArrayList<>();
//        listset.add(itset);
//        listset.add(new IteratorSetting(26, DebugInfoIterator.class, Collections.<String, String>emptyMap()));
//        conn.tableOperations().compact(tableNameC, null, null, listset, true, true); // block

    Map<Key, Value> expect = new HashMap<>();
    expect.put(new Key("A1", "", "B2"), new Value("6".getBytes(StandardCharsets.UTF_8)));
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes(StandardCharsets.UTF_8)));
    expect.put(new Key("A1", "", "B2"), new Value("6".getBytes(StandardCharsets.UTF_8)));
    expect.put(new Key("A2", "", "B2"), new Value("6".getBytes(StandardCharsets.UTF_8)));
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ

    log.debug("Results of scan on table " + tableNameC + " with A=" + tableNameA + " and BT=" + tableNameBT + ':');
    for (Map.Entry<Key, Value> entry : scanner) {
      log.debug(entry.toString());
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);
    scanner.close();


    // Test use of a filter on B.
//    conn.tableOperations().delete(tableNameC);
//    TestUtil.createTestTable(conn, tableNameC, null);
    scanner = conn.createScanner(tableNameC, Authorizations.EMPTY);
    expect = new HashMap<>();
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes(StandardCharsets.UTF_8)));
    actual.clear();

    IteratorSetting itsetFilter = new IteratorSetting(1, ColumnSliceFilter.class);
    ColumnSliceFilter.setSlice(itsetFilter, "B", "B15");
    DynamicIteratorSetting dis = new DynamicIteratorSetting(10, null);
    dis.append(itsetFilter);
    itprops.putAll(dis.buildSettingMap("B.diter."));
    itset = new IteratorSetting(25, TwoTableIterator.class, itprops);
    scanner.clearScanIterators();
    scanner.addScanIterator(itset);

    log.debug("Results of scan on table " + tableNameC + " with AT=" + tableNameA + " and B=" + tableNameBT + ':');
    for (Map.Entry<Key, Value> entry : scanner) {
      log.debug(entry.toString());
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);
    scanner.close();


    conn.tableOperations().delete(tableNameA);
    conn.tableOperations().delete(tableNameBT);
    conn.tableOperations().delete(tableNameC);
  }

  /**
   * <pre>
   *      C1 C2        C1 C2 C3          B1          B1  B2
   * A1 [ 2  2 ] * B1 [   3  3  ] + A1 [ 1  ] = A1 [ 7   12 ]
   * A2 [ 2    ]   B2 [3  3     ]               A2 [     6  ]
   * </pre>
   */
//    @Ignore("New version only works with BatchWriter")
  @Test
  public void testTableMultIterator() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tablePrefix = "testTableMult_";

    final String tableNameAT = tablePrefix + "AT";
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A2", "", "C1"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tableNameAT, null, input);
    }
    SortedSet<Text> splitSet = new TreeSet<>();
    splitSet.add(new Text("A15"));
    //conn.tableOperations().addSplits(tableNameAT, splitSet);

    final String tableNameB = tablePrefix + "B";
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tableNameB, null, input);
    }

    final String tableNameC = tablePrefix + "C";
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "B1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      TestUtil.createTestTable(conn, tableNameC, splitSet, input);
      Map<String, String> optSum = new HashMap<>();
      optSum.put("all", "true");
      conn.tableOperations().attachIterator(tableNameC,
          new IteratorSetting(1, BigDecimalCombiner.BigDecimalSummingCombiner.class, optSum));
    }

    BatchScanner scannerB = conn.createBatchScanner(tableNameB, Authorizations.EMPTY, 2);
    scannerB.setRanges(Collections.singleton(new Range()));

    // test reading entries directly

    {
      Map<String, String> itprops = new HashMap<>();
      itprops.put("AT.instanceName", conn.getInstance().getInstanceName());
      itprops.put("AT.tableName", tableNameAT);
      itprops.put("AT.zookeeperHost", conn.getInstance().getZooKeepers());
      itprops.put("AT.username", tester.getUsername());
      itprops.put("AT.password", new String(tester.getPassword().getPassword(), StandardCharsets.UTF_8));
//            itprops.put("B.instanceName",conn.getInstance().getInstanceName());
//            itprops.put("B.tableName",tableNameB);
//            itprops.put("B.zookeeperHost",conn.getInstance().getZooKeepers());
//            itprops.put("B.username",tester.getUsername());
//            itprops.put("B.password",new String(tester.getPassword().getPassword()));
      itprops.put("dotmode", TwoTableIterator.DOTMODE.ROW.name());
      IteratorSetting itset = GraphuloUtil.tableMultIterator(itprops, 15, null);
      scannerB.addScanIterator(itset);

      Map<Key, Integer> expect = new HashMap<>();
      expect.put(new Key("A1", "", "B1"), 6);
      expect.put(new Key("A1", "", "B2"), 12);
      expect.put(new Key("A2", "", "B2"), 6);
      //scannerB.addScanIterator(new IteratorSetting(16, DebugInfoIterator.class, Collections.<String,String>emptyMap()));
      {
        Map<Key, Integer> actual = new HashMap<>();
        log.info("Scanning tableB " + tableNameB + ":");
        for (Map.Entry<Key, Value> entry : scannerB) {
          log.info(entry.toString());
          Key k = entry.getKey(); // don't copy vis or timestamp; we don't care about comparing those
          Key k2 = new Key(k.getRow(), k.getColumnFamily(), k.getColumnQualifier());
          Integer vold = actual.get(k2);
          if (vold == null)
            vold = 0;
          int vnew = vold + Integer.parseInt(entry.getValue().toString());
          actual.put(k2, vnew);
        }
        Assert.assertEquals(expect, actual);
      }
    }

    // Test use of a filter on B.
    {
      Map<String, String> itprops = new HashMap<>();
      itprops.put("AT.instanceName", conn.getInstance().getInstanceName());
      itprops.put("AT.tableName", tableNameAT);
      itprops.put("AT.zookeeperHost", conn.getInstance().getZooKeepers());
      itprops.put("AT.username", tester.getUsername());
      itprops.put("AT.password", new String(tester.getPassword().getPassword(), StandardCharsets.UTF_8));
//            itprops.put("B.instanceName",conn.getInstance().getInstanceName());
//            itprops.put("B.tableName",tableNameB);
//            itprops.put("B.zookeeperHost",conn.getInstance().getZooKeepers());
//            itprops.put("B.username",tester.getUsername());
//            itprops.put("B.password",new String(tester.getPassword().getPassword()));
      itprops.put("dotmode", TwoTableIterator.DOTMODE.ROW.name());

      IteratorSetting itsetFilter = new IteratorSetting(1, ColumnSliceFilter.class);
      ColumnSliceFilter.setSlice(itsetFilter, "B", "B15");
      DynamicIteratorSetting dis = new DynamicIteratorSetting(10, null);
      dis.append(itsetFilter);
      itprops.putAll(dis.buildSettingMap("B.diter."));

      scannerB.clearScanIterators();
      IteratorSetting itset = GraphuloUtil.tableMultIterator(itprops, 15, null);
      scannerB.addScanIterator(itset);

      Map<Key, Integer> expect = new HashMap<>();
      expect.put(new Key("A1", "", "B1"), 6);
//      expect.put(new Key("A1", "", "B2"), 12);
//      expect.put(new Key("A2", "", "B2"), 6);
      //scannerB.addScanIterator(new IteratorSetting(16, DebugInfoIterator.class, Collections.<String,String>emptyMap()));

      {
        Map<Key, Integer> actual = new HashMap<>();
        log.info("Scanning tableB " + tableNameB + ":");
        for (Map.Entry<Key, Value> entry : scannerB) {
          log.info(entry.toString());
          Key k = entry.getKey(); // don't copy vis or timestamp; we don't care about comparing those
          Key k2 = new Key(k.getRow(), k.getColumnFamily(), k.getColumnQualifier());
          Integer vold = actual.get(k2);
          if (vold == null)
            vold = 0;
          int vnew = vold + Integer.parseInt(entry.getValue().toString());
          actual.put(k2, vnew);
        }
        Assert.assertEquals(expect, actual);
      }
    }


    // test writing to C with monitoring
    {
      Map<String, String> itprops = new HashMap<>();
      itprops.put("AT.instanceName", conn.getInstance().getInstanceName());
      itprops.put("AT.tableName", tableNameAT);
      itprops.put("AT.zookeeperHost", conn.getInstance().getZooKeepers());
      itprops.put("AT.username", tester.getUsername());
      itprops.put("AT.password", new String(tester.getPassword().getPassword(), StandardCharsets.UTF_8));
      itprops.put("C.instanceName", conn.getInstance().getInstanceName());
      itprops.put("C.tableName", tableNameC);
      itprops.put("C.zookeeperHost", conn.getInstance().getZooKeepers());
      itprops.put("C.username", tester.getUsername());
      itprops.put("C.password", new String(tester.getPassword().getPassword(), StandardCharsets.UTF_8));
      itprops.put("C.numEntriesCheckpoint", "1");
      itprops.put("dotmode",TwoTableIterator.DOTMODE.ROW.name());
      IteratorSetting itset = GraphuloUtil.tableMultIterator(itprops, 15, null);
      scannerB.clearScanIterators();
      scannerB.addScanIterator(itset);
      for (Map.Entry<Key, Value> entry : scannerB) {
      }
      scannerB.close();

      Map<Key, Value> expect = new HashMap<>();
      expect.put(new Key("A1", "", "B1"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      expect.put(new Key("A1", "", "B2"), new Value("12".getBytes(StandardCharsets.UTF_8)));
      expect.put(new Key("A2", "", "B2"), new Value("6".getBytes(StandardCharsets.UTF_8)));

      Scanner scannerC = conn.createScanner(tableNameC, Authorizations.EMPTY);
      //scannerC.addScanIterator(new IteratorSetting(16, DebugInfoIterator.class, Collections.<String,String>emptyMap()));
      {
        Map<Key, Value> actual = new HashMap<>();
        log.info("Scanning tableC " + tableNameC + ":");
        for (Map.Entry<Key, Value> entry : scannerC) {
          log.info(entry.toString());
          Key k = entry.getKey(); // don't copy vis or timestamp; we don't care about comparing those
          actual.put(new Key(k.getRow(), k.getColumnFamily(), k.getColumnQualifier()), entry.getValue());
        }
        Assert.assertEquals(expect, actual);
      }
      scannerC.close();
    }

    conn.tableOperations().delete(tableNameAT);
    conn.tableOperations().delete(tableNameB);
    conn.tableOperations().delete(tableNameC);
  }

}
