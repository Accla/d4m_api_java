package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.reducer.GatherColQReducer;
import edu.mit.ll.graphulo.skvi.RemoteMergeIterator;
import edu.mit.ll.graphulo.skvi.RemoteSourceIterator;
import edu.mit.ll.graphulo.skvi.RemoteWriteIterator;
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
import org.apache.accumulo.core.iterators.user.ColumnSliceFilter;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Test RemoteSourceIterator and RemoteMergeIterator.
 */
public class RemoteIteratorTest extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(RemoteIteratorTest.class);

  /**
   * Also test setUniqueColQs.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testWriteTableTranspose() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tA, tR, tRT;
    {
      String[] names = getUniqueNames(3);
      tA = names[0];
      tR = names[1];
      tRT = names[2];
    }
    Map<Key,Value> expectR = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
      expectRT = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    HashSet<String> setUniqueColQsExpect = new HashSet<>(), setUniqueColQsActual;
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes()));
      setUniqueColQsExpect.add("C1");
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes()));
      setUniqueColQsExpect.add("C2");
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes()));
      setUniqueColQsExpect.add("C1");
      expectR.putAll(input);
      expectRT.putAll(GraphuloUtil.transposeMap(input));

      input.put(new Key("A00", "", "C1"), new Value("21".getBytes()));
      input.put(new Key("ZZZ", "", "C1"), new Value("22".getBytes()));

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("A15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }
    TestUtil.createTestTable(conn, tR);
    TestUtil.createTestTable(conn, tRT);

    BatchScanner bs = conn.createBatchScanner(tA, Authorizations.EMPTY, 2);
    bs.setRanges(Collections.singleton(new Range("A1",true,"B",true)));
    Map<String,String> opt = new HashMap<>();
    opt.put("zookeeperHost", conn.getInstance().getZooKeepers());
    opt.put("instanceName", conn.getInstance().getInstanceName());
    opt.put("tableName", tR);
    opt.put("tableNameTranspose", tRT);
    opt.put("username", conn.whoami());
    opt.put("password", new String(tester.getPassword().getPassword()));
    opt.put("reducer", GatherColQReducer.class.getName());
    IteratorSetting is = new IteratorSetting(12,RemoteWriteIterator.class, opt);
    bs.addScanIterator(is);

    GatherColQReducer reducer = new GatherColQReducer();
    reducer.init(Collections.<String,String>emptyMap(), null);
    for (Map.Entry<Key, Value> entry : bs) {
      RemoteWriteIterator.decodeValue(entry.getValue(), reducer);
//      setUniqueColQsActual.addAll((HashSet<String>) SerializationUtils.deserialize(entry.getValue().get()));
    }
    setUniqueColQsActual = reducer.getSerializableForClient();
    Assert.assertEquals(setUniqueColQsExpect, setUniqueColQsActual);
    bs.close();

    Scanner scan = conn.createScanner(tR, Authorizations.EMPTY);
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scan) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expectR, actual);
    scan.close();

    scan = conn.createScanner(tRT, Authorizations.EMPTY);
    actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scan) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expectRT, actual);
    scan.close();

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  @Test
  public void testSource() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tableName, tableName2;
    {
      String[] names = getUniqueNames(2);
      tableName = names[0];
      tableName2 = names[1];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("ccc", "", "cq"), new Value("7".getBytes()));
      input.put(new Key("ddd", "", "cq"), new Value("7".getBytes()));
      input.put(new Key("pogo", "", "cq"), new Value("7".getBytes()));
      input.put(new Key("ddd", "", "cq2"), new Value("8".getBytes()));
      input.put(new Key("ggg", "", "cq2"), new Value("8".getBytes()));
      input.put(new Key("pogo", "", "cq2"), new Value("8".getBytes()));
      input.put(new Key("xyz", "", "cq2"), new Value("8".getBytes()));
      TestUtil.createTestTable(conn, tableName, null, input);
    }
    TestUtil.createTestTable(conn, tableName2);

    List<SortedMap<Key, Value>> expectList = new ArrayList<>();
    {
      SortedMap<Key, Value> smap = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
      smap.put(new Key("ccc", "", "cq"), new Value("7".getBytes()));
      expectList.add(smap);
    }
    {
      SortedMap<Key, Value> smap = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
      smap.put(new Key("ddd", "", "cq"), new Value("7".getBytes()));
      smap.put(new Key("ddd", "", "cq2"), new Value("8".getBytes()));
      expectList.add(smap);
    }
    {
      SortedMap<Key, Value> smap = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
      smap.put(new Key("ggg", "", "cq2"), new Value("8".getBytes()));
      expectList.add(smap);
    }
    {
      SortedMap<Key, Value> smap = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
      smap.put(new Key("pogo", "", "cq"), new Value("7".getBytes()));
      smap.put(new Key("pogo", "", "cq2"), new Value("8".getBytes()));
      expectList.add(smap);
    }
    {
      SortedMap<Key, Value> smap = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
      smap.put(new Key("xyz", "", "cq2"), new Value("8".getBytes()));
      expectList.add(smap);
    }

    Scanner scanner = conn.createScanner(tableName2, Authorizations.EMPTY);
    Map<String, String> itprops = new HashMap<>();
    itprops.put("instanceName", conn.getInstance().getInstanceName());
    itprops.put("tableName", tableName);
    itprops.put("zookeeperHost", conn.getInstance().getZooKeepers());
    //itprops.put("timeout","5000");
    itprops.put("username", tester.getUsername());
    itprops.put("password", new String(tester.getPassword().getPassword()));
    itprops.put("doWholeRow", "true"); // *
    IteratorSetting itset = new IteratorSetting(5, RemoteSourceIterator.class, itprops); //"edu.mit.ll.graphulo.skvi.RemoteSourceIterator", itprops);
    scanner.addScanIterator(itset);
//    log.info("Results of scan on table " + tableName2 + " remote to " + tableName + ':');
    Iterator<SortedMap<Key, Value>> expectIter = expectList.iterator();
    for (Map.Entry<Key, Value> entry : scanner) {
      SortedMap<Key, Value> actualMap = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
      actualMap.putAll(WholeRowIterator.decodeRow(entry.getKey(), entry.getValue()));
      Assert.assertTrue(expectIter.hasNext());
      SortedMap<Key, Value> expectMap = expectIter.next();
      Assert.assertEquals(expectMap, actualMap);
    }
    Assert.assertFalse(expectIter.hasNext());

    conn.tableOperations().delete(tableName);
    conn.tableOperations().delete(tableName2);
  }

  @Test
  public void testSourceSubset() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tableName, tableName2;
    {
      String[] names = getUniqueNames(2);
      tableName = names[0];
      tableName2 = names[1];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("ccc", "", "cq"), new Value("7".getBytes()));
      input.put(new Key("ddd", "", "cq"), new Value("7".getBytes()));
      input.put(new Key("pogo", "", "cq"), new Value("7".getBytes()));
      input.put(new Key("ddd", "", "cq2"), new Value("8".getBytes()));
      input.put(new Key("ggg", "", "cq2"), new Value("8".getBytes()));
      input.put(new Key("pogo", "", "cq2"), new Value("8".getBytes()));
      input.put(new Key("xyz", "", "cq2"), new Value("8".getBytes()));
      TestUtil.createTestTable(conn, tableName, null, input);
    }
    TestUtil.createTestTable(conn, tableName2);

    List<SortedMap<Key, Value>> expectList = new ArrayList<>();
    {
      SortedMap<Key, Value> smap = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
      smap.put(new Key("ddd", "", "cq"), new Value("7".getBytes()));
      smap.put(new Key("ddd", "", "cq2"), new Value("8".getBytes()));
      expectList.add(smap);
    }
    {
      SortedMap<Key, Value> smap = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
      smap.put(new Key("ggg", "", "cq2"), new Value("8".getBytes()));
      expectList.add(smap);
    }
    {
      SortedMap<Key, Value> smap = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
      smap.put(new Key("pogo", "", "cq"), new Value("7".getBytes()));
      smap.put(new Key("pogo", "", "cq2"), new Value("8".getBytes()));
      expectList.add(smap);
    }

    Scanner scanner = conn.createScanner(tableName2, Authorizations.EMPTY);
    Map<String, String> itprops = new HashMap<>();
    itprops.put("instanceName", conn.getInstance().getInstanceName());
    itprops.put("tableName", tableName);
    itprops.put("zookeeperHost", conn.getInstance().getZooKeepers());
    //itprops.put("timeout","5000");
    itprops.put("username", tester.getUsername());
    itprops.put("password", new String(tester.getPassword().getPassword()));
    itprops.put("doWholeRow", "true"); // *
    IteratorSetting itset = new IteratorSetting(5, RemoteSourceIterator.class, itprops); //"edu.mit.ll.graphulo.skvi.RemoteSourceIterator", itprops);
    scanner.addScanIterator(itset);

    Range range = new Range("ddd", "xxx");
    scanner.setRange(range);
    Iterator<SortedMap<Key, Value>> expectIter = expectList.iterator();
    for (Map.Entry<Key, Value> entry : scanner) {
      SortedMap<Key, Value> actualMap = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
      actualMap.putAll(WholeRowIterator.decodeRow(entry.getKey(), entry.getValue()));
      Assert.assertTrue(expectIter.hasNext());
      SortedMap<Key, Value> expectMap = expectIter.next();
      Assert.assertEquals(expectMap, actualMap);
    }
    Assert.assertFalse(expectIter.hasNext());

    // should work the same if we set rowRanges instead of setting the Scanner range
    scanner.setRange(new Range());
    scanner.clearScanIterators();
    itprops.put("rowRanges", GraphuloUtil.rangesToD4MString(Collections.singleton(range)));
    itset = new IteratorSetting(5, RemoteSourceIterator.class, itprops); //"edu.mit.ll.graphulo.skvi.RemoteSourceIterator", itprops);
    scanner.addScanIterator(itset);
    expectIter = expectList.iterator();
    for (Map.Entry<Key, Value> entry : scanner) {
      SortedMap<Key, Value> actualMap = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
      actualMap.putAll(WholeRowIterator.decodeRow(entry.getKey(), entry.getValue()));
      Assert.assertTrue(expectIter.hasNext());
      SortedMap<Key, Value> expectMap = expectIter.next();
      Assert.assertEquals(expectMap, actualMap);
    }
    Assert.assertFalse(expectIter.hasNext());

    scanner.close();
    conn.tableOperations().delete(tableName);
    conn.tableOperations().delete(tableName2);
  }


  /** Now with column subsets in addition to row subsets. */
  @Test
  public void testSourceSubsetColumns() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tableName, tableName2;
    {
      String[] names = getUniqueNames(2);
      tableName = names[0];
      tableName2 = names[1];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("ccc", "", "cq"), new Value("7".getBytes()));
      input.put(new Key("ddd", "", "cq"), new Value("7".getBytes()));
      input.put(new Key("pogo", "", "cq"), new Value("7".getBytes()));
      input.put(new Key("ddd", "", "cq2"), new Value("8".getBytes()));
      input.put(new Key("ggg", "", "cq2"), new Value("8".getBytes()));
      input.put(new Key("pogo", "", "cq2"), new Value("8".getBytes()));
      input.put(new Key("xyz", "", "cq2"), new Value("8".getBytes()));
      TestUtil.createTestTable(conn, tableName, null, input);
    }
    TestUtil.createTestTable(conn, tableName2);

    SortedMap<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("ddd", "", "cq"), new Value("7".getBytes()));
    expect.put(new Key("pogo", "", "cq"), new Value("7".getBytes()));

    Scanner scanner = conn.createScanner(tableName2, Authorizations.EMPTY);
    Map<String, String> itprops = new HashMap<>();
    itprops.put("instanceName", conn.getInstance().getInstanceName());
    itprops.put("tableName", tableName);
    itprops.put("zookeeperHost", conn.getInstance().getZooKeepers());
    //itprops.put("timeout","5000");
    itprops.put("username", tester.getUsername());
    itprops.put("password", new String(tester.getPassword().getPassword()));
    itprops.put("colFilter", "cq,"); // *
    IteratorSetting itset = new IteratorSetting(5, RemoteSourceIterator.class, itprops); //"edu.mit.ll.graphulo.skvi.RemoteSourceIterator", itprops);
    scanner.addScanIterator(itset);

    SortedMap<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    Range range = new Range("ddd", "xxx");
    scanner.setRange(range);
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);

    // now repeat using a column range
    scanner.clearScanIterators();
    itprops.put("colFilter", "c,:,cq15,"); // *
    itset = new IteratorSetting(5, RemoteSourceIterator.class, itprops); //"edu.mit.ll.graphulo.skvi.RemoteSourceIterator", itprops);
    scanner.addScanIterator(itset);

    actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    scanner.setRange(range);
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);

    // now repeat using a multi-column range
    scanner.clearScanIterators();
    itprops.put("colFilter", "a,b,:,b2,b3,c,:,cq15,"); // *
    itset = new IteratorSetting(5, RemoteSourceIterator.class, itprops); //"edu.mit.ll.graphulo.skvi.RemoteSourceIterator", itprops);
    scanner.addScanIterator(itset);

    actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    scanner.setRange(range);
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);


    // what if we put the filter on manually using DynamicIterator?
    scanner.clearScanIterators();
    itprops.remove("colFilter"); //, "a,b,:,b2,b3,c,:,cq15,"); // *
    IteratorSetting itsetFilter = new IteratorSetting(1, ColumnSliceFilter.class);
    ColumnSliceFilter.setSlice(itsetFilter, "c", "cq15");
    DynamicIteratorSetting dis = new DynamicIteratorSetting();
    dis.append(itsetFilter);
    itprops.putAll(dis.buildSettingMap("diter."));

    itset = new IteratorSetting(5, RemoteSourceIterator.class, itprops); //"edu.mit.ll.graphulo.skvi.RemoteSourceIterator", itprops);
    scanner.addScanIterator(itset);

    actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    scanner.setRange(range);
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);

    scanner.close();
    conn.tableOperations().delete(tableName);
    conn.tableOperations().delete(tableName2);
  }




  @Test
  public void testMerge() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException, IOException {
    Connector conn = tester.getConnector();

    final String tableName, tableName2;
    {
      String[] names = getUniqueNames(2);
      tableName = names[0];
      tableName2 = names[1];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("ccc", "", "cq"), new Value("7".getBytes()));
      input.put(new Key("ddd", "", "cq"), new Value("7".getBytes()));
      input.put(new Key("pogo", "", "cq"), new Value("7".getBytes()));
      TestUtil.createTestTable(conn, tableName, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("ddd", "", "cq2"), new Value("8".getBytes()));
      input.put(new Key("ggg", "", "cq2"), new Value("8".getBytes()));
      input.put(new Key("pogo", "", "cq2"), new Value("8".getBytes()));
      input.put(new Key("xyz", "", "cq2"), new Value("8".getBytes()));
      TestUtil.createTestTable(conn, tableName2, null, input);
    }

    SortedMap<Key, Value> expectMap = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expectMap.put(new Key("ccc", "", "cq"), new Value("7".getBytes()));
    expectMap.put(new Key("ddd", "", "cq"), new Value("7".getBytes()));
    expectMap.put(new Key("ddd", "", "cq2"), new Value("8".getBytes()));
    expectMap.put(new Key("ggg", "", "cq2"), new Value("8".getBytes()));
    expectMap.put(new Key("pogo", "", "cq"), new Value("7".getBytes()));
    expectMap.put(new Key("pogo", "", "cq2"), new Value("8".getBytes()));
    expectMap.put(new Key("xyz", "", "cq2"), new Value("8".getBytes()));

    Scanner scanner = conn.createScanner(tableName2, Authorizations.EMPTY);
    Map<String, String> itprops = new HashMap<>();
    itprops.put(RemoteMergeIterator.PREFIX_RemoteIterator + "instanceName", conn.getInstance().getInstanceName());
    itprops.put(RemoteMergeIterator.PREFIX_RemoteIterator + "tableName", tableName);
    itprops.put(RemoteMergeIterator.PREFIX_RemoteIterator + "zookeeperHost", conn.getInstance().getZooKeepers());
    //itprops.put(RemoteMergeIterator.PREFIX_RemoteIterator+"timeout","5000");
    itprops.put(RemoteMergeIterator.PREFIX_RemoteIterator + "username", tester.getUsername());
    itprops.put(RemoteMergeIterator.PREFIX_RemoteIterator + "password", new String(tester.getPassword().getPassword()));
    //itprops.put(RemoteMergeIterator.PREFIX_RemoteIterator + "doWholeRow", "true"); // *
    IteratorSetting itset = new IteratorSetting(5, RemoteMergeIterator.class, itprops); //"edu.mit.ll.graphulo.skvi.RemoteSourceIterator", itprops);
    scanner.addScanIterator(itset);
    SortedMap<Key, Value> actualMap = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    for (Map.Entry<Key, Value> entry : scanner) {
      actualMap.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expectMap, actualMap);

    conn.tableOperations().delete(tableName);
    conn.tableOperations().delete(tableName2);
  }

}
