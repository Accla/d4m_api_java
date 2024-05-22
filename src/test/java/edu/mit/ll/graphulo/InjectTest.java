package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.skvi.HardListIterator;
import edu.mit.ll.graphulo.skvi.InjectIterator;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DebugIterator;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

//import org.apache.accumulo.core.client.admin.CompactionConfig;
//import org.apache.accumulo.core.client.impl.CompactionStrategyConfigUtil;

/**
 * Tests to inject entries into the Accumulo iterator stream.
 */
public class InjectTest extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(InjectTest.class);

  /**
   * Test ordinary Accumulo insert and scan, for sanity.
   */
  @Test
  public void testInsertScan() throws Exception {
    Connector conn = tester.getConnector();

    // create table, add table split, write data
    final String tableName = getUniqueNames(1)[0];
    Map<Key, Value> input = new HashMap<>();
    input.put(new Key("aTablet1", "", "cq"), new Value("7".getBytes(StandardCharsets.UTF_8)));
    input.put(new Key("kTablet2", "", "cq"), new Value("7".getBytes(StandardCharsets.UTF_8)));
    input.put(new Key("zTablet2", "", "cq"), new Value("7".getBytes(StandardCharsets.UTF_8)));
    SortedSet<Text> splitset = new TreeSet<>();
    splitset.add(new Text("f"));
    TestUtil.createTestTable(conn, tableName, splitset, input);

    IteratorSetting itset = new IteratorSetting(16, DebugIterator.class);
    conn.tableOperations().attachIterator(tableName, itset, EnumSet.of(IteratorUtil.IteratorScope.scan));

    // expected data back
    Map<Key, Value> expect = new HashMap<>(input);

    // read back both tablets in parallel
    BatchScanner scan = conn.createBatchScanner(tableName, Authorizations.EMPTY, 2);
//        Key startKey = new Key(new Text("d"), cf, cq);
    Range rng = new Range();
    scan.setRanges(Collections.singleton(rng));
    log.debug("Results of scan on table " + tableName + ':');
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scan) {
      Key k = entry.getKey();
      Value v = entry.getValue();
      log.debug(k + " " + v);
      actual.put(k, v);
    }
    Assert.assertEquals(expect, actual);

    // delete test data
    conn.tableOperations().delete(tableName);
  }

  /**
   * Test injecting data into an empty table at scan.
   */
  @Test
//  @Ignore("KnownBug: ACCUMULO-3646")
  public void testInjectOnScan_Empty() throws Exception {
    Connector conn = tester.getConnector();

    // create table, add table split, write data
    final String tableName = getUniqueNames(1)[0];
    SortedSet<Text> splitset = new TreeSet<>();
    splitset.add(new Text("f"));
    TestUtil.createTestTable(conn, tableName, splitset);

    // expected data back
    Map<Key, Value> expect = new HashMap<>(HardListIterator.allEntriesToInject);

    // attach InjectIterator
    IteratorSetting itset = new IteratorSetting(15, InjectIterator.class);
    conn.tableOperations().attachIterator(tableName, itset, EnumSet.of(IteratorUtil.IteratorScope.scan));
    itset = new IteratorSetting(16, DebugIterator.class);
    conn.tableOperations().attachIterator(tableName, itset, EnumSet.of(IteratorUtil.IteratorScope.scan));

    // read back both tablets in parallel
    BatchScanner scan = conn.createBatchScanner(tableName, Authorizations.EMPTY, 2);
//        Key startKey = new Key(new Text("d"), cf, cq);
    Range rng = new Range();
    scan.setRanges(Collections.singleton(rng));
    log.debug("Results of scan on table " + tableName + ':');
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scan) {
      Key k = entry.getKey();
      Value v = entry.getValue();
      log.debug(k + " " + v);
      // fails on BatchScanner when iterator generates entries past its range
      Assert.assertFalse("Duplicate entry found: " + k, actual.containsKey(k));
      actual.put(k, v);
    }
    Assert.assertEquals(expect, actual);

    // delete test data
    conn.tableOperations().delete(tableName);
  }

  /**
   * Test injecting data into an empty table at scan. Use a regular scanner.
   */
  @Test
  public void testInjectOnScan_Empty_Reg() throws Exception {
    Connector conn = tester.getConnector();

    // create table, add table split, write data
    final String tableName = getUniqueNames(1)[0];
    SortedSet<Text> splitset = new TreeSet<>();
    splitset.add(new Text("f"));
    TestUtil.createTestTable(conn, tableName, splitset);

    // expected data back
    Map<Key, Value> expect = new HashMap<>(HardListIterator.allEntriesToInject);

    // attach InjectIterator
    IteratorSetting itset = new IteratorSetting(15, InjectIterator.class);
    conn.tableOperations().attachIterator(tableName, itset, EnumSet.of(IteratorUtil.IteratorScope.scan));
    itset = new IteratorSetting(16, DebugIterator.class);
    conn.tableOperations().attachIterator(tableName, itset, EnumSet.of(IteratorUtil.IteratorScope.scan));

    // read back both tablets in parallel
    Scanner scan = conn.createScanner(tableName, Authorizations.EMPTY);
//        Key startKey = new Key(new Text("d"), cf, cq);

    log.debug("Results of scan on table " + tableName + ':');
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scan) {
      Key k = entry.getKey();
      Value v = entry.getValue();
      log.debug(k + " " + v);
      Assert.assertFalse("Duplicate entry found: " + k, actual.containsKey(k)); // passes on normal scanner
      actual.put(k, v);
    }
    Assert.assertEquals(expect, actual);

    // delete test data
    conn.tableOperations().delete(tableName);
  }

  /**
   * Test injecting data into a table with other data at scan.
   */
  @Test
//  @Ignore("KnownBug: ACCUMULO-3646")
  public void testInjectOnScan() throws Exception {
    Connector conn = tester.getConnector();

    // create table, add table split, write data
    final String tableName = getUniqueNames(1)[0];
    Map<Key, Value> input = new HashMap<>();
    input.put(new Key("aTablet1", "", "cq"), new Value("7".getBytes(StandardCharsets.UTF_8)));
    input.put(new Key("kTablet2", "", "cq"), new Value("7".getBytes(StandardCharsets.UTF_8)));
    input.put(new Key("zTablet2", "", "cq"), new Value("7".getBytes(StandardCharsets.UTF_8)));
    SortedSet<Text> splitset = new TreeSet<>();
    splitset.add(new Text("f"));
    TestUtil.createTestTable(conn, tableName, splitset, input);

    // expected data back
    Map<Key, Value> expect = new HashMap<>(input);
    expect.putAll(HardListIterator.allEntriesToInject);

    // attach InjectIterator
    IteratorSetting itset = new IteratorSetting(15, InjectIterator.class);
    conn.tableOperations().attachIterator(tableName, itset, EnumSet.of(IteratorUtil.IteratorScope.scan));
    itset = new IteratorSetting(16, DebugIterator.class);
    conn.tableOperations().attachIterator(tableName, itset, EnumSet.of(IteratorUtil.IteratorScope.scan));

    // read back both tablets in parallel
    BatchScanner scan = conn.createBatchScanner(tableName, Authorizations.EMPTY, 2);
//        Key startKey = new Key(new Text("d"), cf, cq);
    Range rng = new Range();
    scan.setRanges(Collections.singleton(rng));
    log.debug("Results of scan on table " + tableName + ':');
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scan) {
      Key k = entry.getKey();
      Value v = entry.getValue();
      log.debug(k + " " + v);
      Assert.assertFalse("Duplicate entry found: " + k, actual.containsKey(k)); // Fails
      actual.put(k, v);
    }
    //TestUtil.assertEqualEntriesRowColFColQ(expect, actual);
    Assert.assertEquals(expect, actual);

    // delete test data
    conn.tableOperations().delete(tableName);
  }

  /**
   * Test injecting data into a table with other data at manual full major compaction.
   */
  @Test
  public void testInjectOnCompact() throws Exception {
    Connector conn = tester.getConnector();

    // create table, add table split, write data
    final String tableName = getUniqueNames(1)[0];
    Map<Key, Value> input = new HashMap<>();
    input.put(new Key("aTablet1", "", "cq"), new Value("7".getBytes(StandardCharsets.UTF_8)));
    input.put(new Key("kTablet2", "", "cq"), new Value("7".getBytes(StandardCharsets.UTF_8)));
    input.put(new Key("zTablet2", "", "cq"), new Value("7".getBytes(StandardCharsets.UTF_8)));
    SortedSet<Text> splitset = new TreeSet<>();
    splitset.add(new Text("f"));
    TestUtil.createTestTable(conn, tableName, splitset, input);

    // expected data back
    Map<Key, Value> expect = new HashMap<>(input);
    expect.putAll(HardListIterator.allEntriesToInject);
    // we will not read back the entry that has a column visibility because this is a compaction write and we don't have the credentials
    for (Iterator<Map.Entry<Key, Value>> iterator = expect.entrySet().iterator();
         iterator.hasNext();
        ) {
      if (iterator.next().getKey().getColumnVisibilityData().length() > 0)
        iterator.remove();
    }

    // attach InjectIterator, flush and compact. Compaction blocks.
    IteratorSetting itset = new IteratorSetting(15, InjectIterator.class);
    List<IteratorSetting> itlist = new ArrayList<>();
    itlist.add(itset);
    itset = new IteratorSetting(16, DebugIterator.class);
    itlist.add(itset);
    StopWatch sw = new StopWatch();
    sw.start();
    conn.tableOperations().compact(tableName, null, null, itlist, true, true);

//        CompactionConfig cc = new CompactionConfig()
//                .setCompactionStrategy(CompactionStrategyConfigUtil.DEFAULT_STRATEGY)
//                .setStartRow(null).setEndRow(null).setIterators(itlist)
//                .setFlush(true).setWait(true);
//        conn.tableOperations().compact(tableName, cc);

    sw.stop();
    log.debug("compaction took " + sw.getTime() + " ms");

    // read back both tablets in parallel
    BatchScanner scan = conn.createBatchScanner(tableName, Authorizations.EMPTY, 2);
//        Key startKey = new Key(new Text("d"), cf, cq);
    Range rng = new Range();
    scan.setRanges(Collections.singleton(rng));
    log.debug("Results of scan on table " + tableName + ':');
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scan) {
      Key k = entry.getKey();
      Value v = entry.getValue();
      log.debug(k + " " + v);
      Assert.assertFalse("Duplicate entry found: " + k, actual.containsKey(k)); // passes because not scan-time iterator
      actual.put(k, v);
    }
    Assert.assertEquals(expect, actual);

    // delete test data
    conn.tableOperations().delete(tableName);
  }

  /**
   * Test injecting data into an empty table at manual full major compaction.
   */
  @Test
  @Ignore("KnownBug: ACCUMULO-3645 Fixed in Accumulo 1.7.0")
  public void testInjectOnCompact_Empty() throws Exception {
    Connector conn = tester.getConnector();

    // create table, add table split, write data
    final String tableName = getUniqueNames(1)[0];
    SortedSet<Text> splitset = new TreeSet<>();
    splitset.add(new Text("f"));
    TestUtil.createTestTable(conn, tableName, splitset);

    // expected data back
    Map<Key, Value> expect = new HashMap<>(HardListIterator.allEntriesToInject);
    // we will not read back the entry that has a column visibility because this is a compaction write and we don't have the credentials
    for (Iterator<Map.Entry<Key, Value>> iterator = expect.entrySet().iterator();
         iterator.hasNext();
        ) {
      if (iterator.next().getKey().getColumnVisibilityData().length() > 0)
        iterator.remove();
    }

    // attach InjectIterator, flush and compact. Compaction blocks.
    IteratorSetting itset = new IteratorSetting(15, InjectIterator.class);
    StopWatch sw = new StopWatch();
    sw.start();
    conn.tableOperations().compact(tableName, null, null, Collections.singletonList(itset), true, true);
    sw.stop();
    log.debug("compaction took " + sw.getTime() + " ms");

    // read back both tablets in parallel
    BatchScanner scan = conn.createBatchScanner(tableName, Authorizations.EMPTY, 2);
//        Key startKey = new Key(new Text("d"), cf, cq);
    Range rng = new Range();
    scan.setRanges(Collections.singleton(rng));
    log.debug("Results of scan on table " + tableName + ':');
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scan) {
      Key k = entry.getKey();
      Value v = entry.getValue();
      log.debug(k + " " + v);
      Assert.assertFalse("Duplicate entry found: " + k, actual.containsKey(k));
      actual.put(k, v);
    }
    Assert.assertEquals(expect, actual);

    // delete test data
    conn.tableOperations().delete(tableName);
  }
}
