package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.rowmult.SelectorRowMultiply;
import edu.mit.ll.graphulo.skvi.TwoTableIterator;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by dhutchis on 6/22/15.
 */
public class RowMultiplyTest extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(RowMultiplyTest.class);

  /**
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   */
  @Test
  public void testSelectorRowMultiply() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tADeg;
    {
      String[] names = getUniqueNames(2);
      tA = names[0];
      tADeg = names[1];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      expect.putAll(input);
      input.put(new Key("vBig", "", "v0"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "v1"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "v2"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
//      input.put(new Key("vBig", "", "3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tADeg, splits, input);
    }

    Map<String, String> opt = new HashMap<>();
    String instance = conn.getInstance().getInstanceName();
    String zookeepers = conn.getInstance().getZooKeepers();
    String user = conn.whoami();
    opt.put("AT.zookeeperHost", zookeepers);
    opt.put("AT.instanceName", instance);
    opt.put("AT.tableName", tADeg);
    opt.put("AT.username", user);
    opt.put("AT.password", new String(tester.getPassword().getPassword(), StandardCharsets.UTF_8));
    opt.put("rowMultiplyOp", SelectorRowMultiply.class.getName());
    opt.put("dotmode", TwoTableIterator.DOTMODE.ROW.name());
    IteratorSetting itset = new IteratorSetting(1, TwoTableIterator.class, opt);

    BatchScanner scanner = conn.createBatchScanner(tA, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    scanner.addScanIterator(itset);
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expect, actual);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tADeg);
  }

}
