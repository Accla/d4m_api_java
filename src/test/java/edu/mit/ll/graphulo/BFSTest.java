package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.GraphuloUtil;
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
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 *
 */
public class BFSTest extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(BFSTest.class);

  /**
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   */
  @Test
  public void testAdjBFS() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tADeg, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tADeg = names[1];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
      actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
      expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
      actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      expect.putAll(input);
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
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
      input.put(new Key("vBig", "", "3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tADeg, splits, input);
    }

    String v0 = "v0,";
    Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v0,vBig,");

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, null, -1, tADeg, "", true, 1, 2, Graphulo.PLUS_ITERATOR_BIGDECIMAL);
    Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expect, actual);

    scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actualTranspose.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expectTranspose, actualTranspose);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tADeg);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  /**
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   */
  @Test
  public void testAdjBFSUnion() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tADeg, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tADeg = names[1];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      expect.putAll(input);
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
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
      input.put(new Key("vBig", "", "3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tADeg, splits, input);
    }

    String v0 = "v0,";
    Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v0,v1,v2,vBig,");

    MutableLong numEntriesWritten = new MutableLong();
    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, null, -1, tADeg, "", true, 1, 2, Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        null, null, true, numEntriesWritten);
    Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));
    Assert.assertEquals(6l, numEntriesWritten.longValue());

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expect, actual);

    scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actualTranspose.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expectTranspose, actualTranspose);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tADeg);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  /**
   * Send to client instead of a new table.
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   */
  @Test
  public void testAdjBFSToClient() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
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
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      expect.putAll(input);
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
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
      input.put(new Key("vBig", "", "3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tADeg, splits, input);
    }

    String v0 = "v0,vBig,";
    Collection<Text> u1expect = GraphuloUtil.d4mRowToTexts("v1,vBig,");

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    String u1actual = graphulo.AdjBFS(tA, v0, 1, null, null, actual, -1, tADeg, "", true, 1, 2, Graphulo.PLUS_ITERATOR_BIGDECIMAL);
    Assert.assertEquals(u1expect, GraphuloUtil.d4mRowToTexts(u1actual));
    Assert.assertEquals(expect, actual);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tADeg);
  }

  /**
   * Vary degree tables.
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   */
  @Test
  public void testAdjBFSVaryDegreeTable() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tADeg, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tADeg = names[1];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      expect.putAll(input);
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
      input.put(new Key("vBig", "", "v0"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "v1"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "v2"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v2", "", "deg"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v0", "", "deg"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "deg"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "deg"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tADeg, splits, input);
    }

    String v0 = "v0,";
    Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v0,vBig,");

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    {
      String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, null, -1, tADeg, "deg", false, 1, 2, Graphulo.PLUS_ITERATOR_BIGDECIMAL);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectTranspose, actualTranspose);
    }


    // now put degree in column with prefix
    conn.tableOperations().delete(tADeg);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "d|2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "d|2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "d|2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "d|3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tADeg, splits, input);
    }
    {
      String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, null, -1, tADeg, "d|", true, 1, 2, null);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectTranspose, actualTranspose);
    }

    // now put in range expression for v0
    v0 = "v0,:,v000,";
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
    {
      String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, null, -1, tADeg, "d|", true, 1, 2, null);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectTranspose, actualTranspose);
    }


    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tADeg);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  /**
   * Same as above but no degree table.
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   */
  @Test
  public void testAdjBFSNoDegTable() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
            actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
            expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
            actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      expect.putAll(input);
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
      input.put(new Key("vBig", "", "v0"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "v1"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "v2"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }

    String v0 = "v0,";
    Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v0,vBig,");

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    {
      String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, null, -1, null, null, true, 1, 2, Graphulo.PLUS_ITERATOR_BIGDECIMAL);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectTranspose, actualTranspose);
    }

    // now put in range expression for v0
    v0 = "v0,:,v000,";
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
    {
      String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, null, -1, null, null, true, 1, 2, null);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectTranspose, actualTranspose);
    }


    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  /**
   * Same as above but do all nodes. Effectively copies table and its transpose that passes filter.
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   */
  @Test
  public void testAdjBFSAll() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      expect.putAll(input);
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
      input.put(new Key("vBig", "", "v0"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "v1"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "v2"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }

    String v0 = ""; // all nodes
    Collection<Text> u1expect = GraphuloUtil.d4mRowToTexts("v0,v1,v2,vBig,");

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    String u1actual = graphulo.AdjBFS(tA, v0, 1, tR, tRT, null, -1, null, null, true, 1, 2, null);
    Assert.assertEquals(u1expect, GraphuloUtil.d4mRowToTexts(u1actual));

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expect, actual);

    scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actualTranspose.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expectTranspose, actualTranspose);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  /**
   * Same as above but no filtering. Effectively copies table and its transpose.
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   */
  @Test
  public void testAdjBFSNoFilter() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "v0"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "v1"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "v2"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      expect.putAll(input);
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }

    String v0 = ""; // all nodes
    Collection<Text> u1expect = GraphuloUtil.d4mRowToTexts("v0,v1,v2,vBig,");

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    String u1actual = graphulo.AdjBFS(tA, v0, 1, tR, tRT, null, -1, null, null, true, 1, Integer.MAX_VALUE, null);
    Assert.assertEquals(u1expect, GraphuloUtil.d4mRowToTexts(u1actual));

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expect, actual);

    scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actualTranspose.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expectTranspose, actualTranspose);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }


  /**
   * <pre>
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   * </pre>
   */
  @Test
  public void testEdgeBFS() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tE, tETDeg, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tE = names[0];
      tETDeg = names[1];
      tR = names[2];
      tRT = names[3];
    }
    GraphuloUtil.deleteTables(conn, tR, tRT);
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("e0", "", "out|v0"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e0", "", "in|v1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e1", "", "out|v1"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e1", "", "in|v2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e2", "", "out|v2"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e2", "", "in|v0"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e3", "", "out|v0"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e3", "", "in|vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e4", "", "out|v1"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e4", "", "in|vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e5", "", "out|v2"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e5", "", "in|vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      expect.putAll(input);
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
      input.put(new Key("e6", "", "out|vBig"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e6", "", "in|v0"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e7", "", "out|vBig"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e7", "", "in|v1"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e8", "", "out|vBig"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e8", "", "in|v2"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("e33"));
      TestUtil.createTestTable(conn, tE, splits, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tETDeg, splits, input);
    }

    String v0 = "v0,";
    Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v0,vBig,");

    {
      MutableLong numEntriesWritten = new MutableLong();
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.EdgeBFS(tE, v0, 3, tR, tRT, "out|,", "in|,", tETDeg, "", true, 1, 2,
          Graphulo.PLUS_ITERATOR_BIGDECIMAL, 1, Authorizations.EMPTY, Authorizations.EMPTY, null, true, false,
          numEntriesWritten);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));
      Assert.assertEquals(12l, numEntriesWritten.longValue());

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectTranspose, actualTranspose);
    }

    // check keep original timestamp
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
    actual.clear();
    actualTranspose.clear();
    {
      MutableLong numEntriesWritten = new MutableLong();
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.EdgeBFS(tE, v0, 3, tR, tRT, "out|,", "in|,", tETDeg, "", true, 1, 2,
          null, 1, Authorizations.EMPTY, Authorizations.EMPTY, null, false, false,
          numEntriesWritten);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));
      Assert.assertEquals(12l, numEntriesWritten.longValue());

      Map<Key,Value> e = new TreeMap<>(), a = new TreeMap<>();
      TestUtil.scanTableToMap(conn, tR, a);
      BatchScanner scanner = conn.createBatchScanner(tE, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        int i = entry.getKey().getRow().toString().charAt(1) - '0';
        if (i >= 0 && i <= 5)
          e.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
//      TestUtil.printExpectActual(e, a);
      Assert.assertEquals(e, a);
    }

    // sanity check out prefixes
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
    actual.clear();
    actualTranspose.clear();
    {
      MutableLong numEntriesWritten = new MutableLong();
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.EdgeBFS(tE, v0, 3, tR, tRT, "out|,afedfafe,", "in|,", tETDeg, "", true, 1, 2,
          Graphulo.PLUS_ITERATOR_BIGDECIMAL, -1, Authorizations.EMPTY, Authorizations.EMPTY, "", true, false,
          numEntriesWritten);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));
      Assert.assertEquals(12l, numEntriesWritten.longValue());

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectTranspose, actualTranspose);
    }

    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
    actual.clear();
    actualTranspose.clear();
    v0 = "v0,:,v000,";
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.EdgeBFS(tE, v0, 3, tR, tRT, "out|,", "in|,", tETDeg, "", true, 1, 2,
          Graphulo.PLUS_ITERATOR_BIGDECIMAL, -1, Authorizations.EMPTY, Authorizations.EMPTY, "", true, false, null);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectTranspose, actualTranspose);
    }

    conn.tableOperations().delete(tE);
    conn.tableOperations().delete(tETDeg);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  /**
   * <pre>
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   * </pre>
   */
  @Test
  public void testEdgeBFS_Multi_Hyper() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tE, tETDeg, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tE = names[0];
      tETDeg = names[1];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("e0", "", "outA|v0"), new Value("5".getBytes(StandardCharsets.UTF_8)));   /////1
      input.put(new Key("e0", "", "in|v1"), new Value("5".getBytes(StandardCharsets.UTF_8)));     /////1 v1
      input.put(new Key("e1", "", "out|v1"), new Value("2".getBytes(StandardCharsets.UTF_8)));    /////2
      input.put(new Key("e1", "", "HEYHEY|v2"), new Value("2".getBytes(StandardCharsets.UTF_8))); /////2 v2
      input.put(new Key("e1", "", "HEYHEY|vBig"), new Value("2".getBytes(StandardCharsets.UTF_8))); /////2 vBig ! Repeated twice
      input.put(new Key("e2", "", "outB|v2"), new Value("4".getBytes(StandardCharsets.UTF_8)));   /////3
      input.put(new Key("e2", "", "in|v0"), new Value("4".getBytes(StandardCharsets.UTF_8)));     /////3 v0
      input.put(new Key("e3", "", "out|v0"), new Value("7".getBytes(StandardCharsets.UTF_8)));    /////1
      input.put(new Key("e3", "", "in|vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));   /////1 vBig
      input.put(new Key("e4", "", "v1"), new Value("7".getBytes(StandardCharsets.UTF_8)));        /////2
      input.put(new Key("e4", "", "in|vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));   /////2 vBig
      input.put(new Key("e5", "", "outA|v2"), new Value("7".getBytes(StandardCharsets.UTF_8)));   /////3
      input.put(new Key("e5", "", "in|vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));   /////3 vBig
      expect.putAll(input);
      expect.put(new Key("e1", "", "out|v1"), new Value("4".getBytes(StandardCharsets.UTF_8))); // double b/c repeated twice
      expectTranspose.putAll(GraphuloUtil.transposeMap(expect));
      input.put(new Key("e6", "", "outA|vBig"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e6", "", "HEYHEY|v0"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e7", "", "out|vBig"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e7", "", "in|v1"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e8", "", "out|vBig"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e8", "", "in|v2"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("e33"));
      TestUtil.createTestTable(conn, tE, splits, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tETDeg, splits, input);
    }

    String v0 = "v0,";
    Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v0,vBig,");

    {
      MutableLong numEntriesWritten = new MutableLong();
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.EdgeBFS(tE, v0, 3, tR, tRT, "out|,outB|,outA|,,", "in|,HEYHEY|,", tETDeg, "", true,
          1, 2, Graphulo.PLUS_ITERATOR_BIGDECIMAL, -1, Authorizations.EMPTY, Authorizations.EMPTY, "", true, false,
          numEntriesWritten);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));
//      DebugUtil.printTable("hyper e1", conn, tR, 9);
      Assert.assertEquals(14l, numEntriesWritten.longValue());

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
//      TestUtil.printExpectActual(expect, actual);
      Assert.assertEquals(expect, actual);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
//      TestUtil.printExpectActual(expectTranspose, actualTranspose);
      Assert.assertEquals(expectTranspose, actualTranspose);
    }
    conn.tableOperations().delete(tE);
    conn.tableOperations().delete(tETDeg);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  @Test
  public void testEdgeBFS2() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tE, tETDeg, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tE = names[0];
//      tETDeg = names[1];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("one", "", "outNode|string"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("one", "", "inNode|2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("two", "", "outNode|string"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("two", "", "inNode|3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      expect.putAll(input);
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
      input.put(new Key("three", "", "outNode|3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("three", "", "inNode|4"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("e33"));
      TestUtil.createTestTable(conn, tE, splits, input);
    }
//    {
//      Map<Key, Value> input = new HashMap<>();
//      input.put(new Key("v0", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
//      input.put(new Key("v1", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
//      input.put(new Key("v2", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
//      input.put(new Key("vBig", "", "3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
//      SortedSet<Text> splits = new TreeSet<>();
//      splits.add(new Text("v15"));
//      TestUtil.createTestTable(conn, tETDeg, splits, input);
//    }
//    byte[] by = "string".getBytes(StandardCharsets.UTF_8);
//    log.debug("Printing characters of string: "+ Key.toPrintableString(by,0,by.length,100));

    String v0 = "string,";
    Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("2,3,");

    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.EdgeBFS(tE, v0, 1, tR, tRT, "outNode|,", "inNode|,", //tETDeg
      null , null, false, 0, Integer.MAX_VALUE, Graphulo.PLUS_ITERATOR_BIGDECIMAL, 1, Authorizations.EMPTY, Authorizations.EMPTY, "", true, false, null);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
        System.out.println(entry.getKey().toStringNoTime()+" "+entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectTranspose, actualTranspose);
    }

    conn.tableOperations().delete(tE);
//    conn.tableOperations().delete(tETDeg);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }


  /**
   * Undirected.
   * <pre>
   *       v9
   *       |
   *    - vBig -
   *   /   |    \
   *  /    |     \
   * v0----v1----v2
   * </pre>
   */
  @Test
  public void testSingleBFS() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tS, tR;
    {
      String[] names = getUniqueNames(2);
      tS = names[0];
      tR = names[1];
    }
    Map<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        degex = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        degin = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0|v1", "", "edge"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1|v0", "", "edge"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1|v2", "", "edge"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2|v1", "", "edge"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v0|vBig", "", "edge"), new Value("6".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1|vBig", "", "edge"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2|vBig", "", "edge"), new Value("8".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v9|vBig", "", "edge"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig|v0", "", "edge"), new Value("6".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig|v1", "", "edge"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig|v2", "", "edge"), new Value("8".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig|v9", "", "edge"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v0", "", "deg"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "deg"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "deg"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v9", "", "deg"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "deg"), new Value("4".getBytes(StandardCharsets.UTF_8)));

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tS, splits, input);

      expect.put(new Key("v0|v1", "", "edge"), new Value("15".getBytes(StandardCharsets.UTF_8))); //x3
      expect.put(new Key("v1|v0", "", "edge"), new Value("15".getBytes(StandardCharsets.UTF_8)));
      expect.put(new Key("v1|v2", "", "edge"), new Value("4".getBytes(StandardCharsets.UTF_8))); //x2
      expect.put(new Key("v2|v1", "", "edge"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      expect.put(new Key("v0|vBig", "", "edge"), new Value("12".getBytes(StandardCharsets.UTF_8))); //x2
      expect.put(new Key("v1|vBig", "", "edge"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      expect.put(new Key("v2|vBig", "", "edge"), new Value("8".getBytes(StandardCharsets.UTF_8)));
//      expect.put(new Key("v9|vBig", "", "edge"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      expect.put(new Key("vBig|v0", "", "edge"), new Value("12".getBytes(StandardCharsets.UTF_8)));
      expect.put(new Key("vBig|v1", "", "edge"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      expect.put(new Key("vBig|v2", "", "edge"), new Value("8".getBytes(StandardCharsets.UTF_8)));
//      expect.put(new Key("vBig|v9", "", "edge"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      degex.put(new Key("v0", "", "deg"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      degex.put(new Key("v1", "", "deg"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      degex.put(new Key("v2", "", "deg"), new Value("2".getBytes(StandardCharsets.UTF_8)));
//      degex.put(new Key("v9", "",   "deg"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      degin.put(new Key("vBig", "", "deg"), new Value("3".getBytes(StandardCharsets.UTF_8))); // 3, not 4!!

    }

    IteratorSetting sumSetting = new IteratorSetting(6, SummingCombiner.class);
    LongCombiner.setEncodingType(sumSetting, LongCombiner.Type.STRING);
    Combiner.setColumns(sumSetting, Collections.singletonList(new IteratorSetting.Column("", "edge")));
    // ^^^^^^^^ Important: Combiner only applies to edge column, not to the degree column
    // Want to treat degree as the number of columns, not the sum of weights


    // Below code for experimenting:
//    {
//      // 1 step
//      boolean copyOutDegrees = true, computeInDegrees = false, outputUnion = false;
//      String v0 = "v0,";
//      Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v1,vBig,");
//      {
//        Graphulo graphulo = new Graphulo(conn, tester.getPassword());
//        String u3actual = graphulo.SingleBFS(tS, "edge", '|', v0, 1, tR,
//            tS, "deg", false, copyOutDegrees, computeInDegrees, 1, 3, sumSetting, outputUnion, true);
//
//
//        BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
//        scanner.setRanges(Collections.singleton(new Range()));
//        for (Map.Entry<Key, Value> entry : scanner) {
//          actual.put(entry.getKey(), entry.getValue());
//          System.out.println(entry.getKey().toStringNoTime()+" -> "+entry.getValue());
//        }
//        scanner.close();
//  //      TestUtil.printExpectActual(expect, actual);
//        Assert.assertEquals(expect, actual);
//        Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));
//      }
//    }
//    if (true) return;
//    conn.tableOperations().delete(tR);

    MutableLong numEntriesWritten = new MutableLong();
    boolean copyOutDegrees = false, computeInDegrees = false, outputUnion = false;
    String v0 = "v0,";
    Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v1,vBig,");
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.SingleBFS(tS, "edge", '|', v0, 3, tR,
          tS, "deg", copyOutDegrees, computeInDegrees, null, null, 1, 3, sumSetting, outputUnion, Authorizations.EMPTY,
          numEntriesWritten);
      Assert.assertEquals(18l, numEntriesWritten.longValue());

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
//      TestUtil.printExpectActual(expect, actual);
      Assert.assertEquals(expect, actual);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));
    }

    conn.tableOperations().delete(tR);
    copyOutDegrees = false; computeInDegrees = false; outputUnion = true;
    u3expect = GraphuloUtil.d4mRowToTexts("v0,v1,v2,vBig,");
    v0 = "v0,:,v000,";
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.SingleBFS(tS, "edge", '|', v0, 3, tR,
          tS, "deg", copyOutDegrees, computeInDegrees, null, null, 1, 3, sumSetting, outputUnion, Authorizations.EMPTY,
          numEntriesWritten);
      Assert.assertEquals(18l, numEntriesWritten.longValue());
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
    }

    conn.tableOperations().delete(tR);
    copyOutDegrees = true; computeInDegrees = false; outputUnion = true;
    v0 = "v0,";
    expect.putAll(degex);
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.SingleBFS(tS, "edge", '|', v0, 3, tR,
          tS, "deg", copyOutDegrees, computeInDegrees, null, null, 1, 3, sumSetting, outputUnion, Authorizations.EMPTY,
          numEntriesWritten);
      Assert.assertEquals(22l, numEntriesWritten.longValue());
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      //TestUtil.printExpectActual(expect, actual);
      Assert.assertEquals(expect, actual);
    }

    conn.tableOperations().delete(tR);
    copyOutDegrees = true; computeInDegrees = false; outputUnion = false;
    u3expect = GraphuloUtil.d4mRowToTexts("v1,vBig,");
    v0 = "v0,:,v000,";
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.SingleBFS(tS, "edge", '|', v0, 3, tR,
          tS, "deg", copyOutDegrees, computeInDegrees, null, null, 1, 3, sumSetting, outputUnion, Authorizations.EMPTY, null);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
    }

    conn.tableOperations().delete(tR);
    copyOutDegrees = true; computeInDegrees = true; outputUnion = false;
    u3expect = GraphuloUtil.d4mRowToTexts("v1,vBig,");
    v0 = "v0,";
    expect.putAll(degin);
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.SingleBFS(tS, "edge", '|', v0, 3, tR,
          tS, "deg", copyOutDegrees, computeInDegrees, null, null, 1, 3, sumSetting, outputUnion, Authorizations.EMPTY,
          numEntriesWritten);
      Assert.assertEquals(23l, numEntriesWritten.longValue());
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
//      TestUtil.printExpectActual(expect, actual);
      Assert.assertEquals(expect, actual);
    }


    conn.tableOperations().delete(tS);
    conn.tableOperations().delete(tR);
  }



  @Test
  public void testGenerateDegreeTable() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tE, tET, tEDegOutIn, tEDegOut, tEDegIn, tEDegEdge, tEDegWeightEdge;
    {
      String[] names = getUniqueNames(7);
      tE = names[0];
      tET = names[1];
      tEDegOutIn = names[2];
      tEDegOut = names[3];
      tEDegIn = names[4];
      tEDegEdge = names[5];
      tEDegWeightEdge = names[6];
    }
    Map<Key,Value>
//        expectOut = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
//        actualOut = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
//        expectIn = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
//        actualIn = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
//        expectOutIn = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
//        actualOutIn = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectEdgeDeg = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualEdgeDeg = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectEdgeDegWeight = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualEdgeDegWeight = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("e0", "", "out|v0"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e0", "", "in|v1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e1", "", "out|v1"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e1", "", "in|v2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e2", "", "out|v2"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e2", "", "in|v0"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e3", "", "out|v0"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e3", "", "in|vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e4", "", "out|v1"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e4", "", "in|vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e5", "", "out|v2"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e5", "", "in|vBig"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e6", "", "out|vBig"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e6", "", "in|v0"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e7", "", "out|vBig"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e7", "", "in|v1"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e8", "", "out|vBig"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e8", "", "in|v2"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("e33"));
      TestUtil.createTestTable(conn, tE, splits, input);
//      splits.clear();
//      splits.add(new Text("out|v11"));
//      TestUtil.createTestTable(conn, tET, splits, TestUtil.transposeMap(input));
    }
    {
//      expectOut.put(new Key("v0", "",   ""), new Value("2".getBytes(StandardCharsets.UTF_8)));
//      expectOut.put(new Key("v1", "",   ""), new Value("2".getBytes(StandardCharsets.UTF_8)));
//      expectOut.put(new Key("v2", "",   ""), new Value("2".getBytes(StandardCharsets.UTF_8)));
//      expectOut.put(new Key("vBig", "", ""), new Value("3".getBytes(StandardCharsets.UTF_8)));
//
//      expectIn.put(new Key("v0", "",   ""), new Value("2".getBytes(StandardCharsets.UTF_8)));
//      expectIn.put(new Key("v1", "",   ""), new Value("2".getBytes(StandardCharsets.UTF_8)));
//      expectIn.put(new Key("v2", "",   ""), new Value("2".getBytes(StandardCharsets.UTF_8)));
//      expectIn.put(new Key("vBig", "", ""), new Value("3".getBytes(StandardCharsets.UTF_8)));
//
//      expectOutIn.put(new Key("v0", "",   ""), new Value("4".getBytes(StandardCharsets.UTF_8)));
//      expectOutIn.put(new Key("v1", "",   ""), new Value("4".getBytes(StandardCharsets.UTF_8)));
//      expectOutIn.put(new Key("v2", "",   ""), new Value("4".getBytes(StandardCharsets.UTF_8)));
//      expectOutIn.put(new Key("vBig", "", ""), new Value("6".getBytes(StandardCharsets.UTF_8)));

      expectEdgeDeg.put(new Key("e0", "", ""), new Value("2".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDeg.put(new Key("e1", "", ""), new Value("2".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDeg.put(new Key("e2", "", ""), new Value("2".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDeg.put(new Key("e3", "", ""), new Value("2".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDeg.put(new Key("e4", "", ""), new Value("2".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDeg.put(new Key("e5", "", ""), new Value("2".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDeg.put(new Key("e6", "", ""), new Value("2".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDeg.put(new Key("e7", "", ""), new Value("2".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDeg.put(new Key("e8", "", ""), new Value("2".getBytes(StandardCharsets.UTF_8)));

      expectEdgeDegWeight.put(new Key("e0", "", ""), new Value("10".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDegWeight.put(new Key("e1", "", ""), new Value("4".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDegWeight.put(new Key("e2", "", ""), new Value("8".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDegWeight.put(new Key("e3", "", ""), new Value("14".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDegWeight.put(new Key("e4", "", ""), new Value("14".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDegWeight.put(new Key("e5", "", ""), new Value("14".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDegWeight.put(new Key("e6", "", ""), new Value("18".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDegWeight.put(new Key("e7", "", ""), new Value("18".getBytes(StandardCharsets.UTF_8)));
      expectEdgeDegWeight.put(new Key("e8", "", ""), new Value("18".getBytes(StandardCharsets.UTF_8)));
    }

    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long numRows = graphulo.generateDegreeTable(tE, tEDegEdge, true);

      BatchScanner scanner = conn.createBatchScanner(tEDegEdge, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualEdgeDeg.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectEdgeDeg, actualEdgeDeg);
      Assert.assertEquals(9, numRows);
    }
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long numRows = graphulo.generateDegreeTable(tE, tEDegWeightEdge, false);

      BatchScanner scanner = conn.createBatchScanner(tEDegWeightEdge, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualEdgeDegWeight.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectEdgeDegWeight, actualEdgeDegWeight);
      Assert.assertEquals(9, numRows);
    }

    conn.tableOperations().delete(tE);
//    conn.tableOperations().delete(tET);
    conn.tableOperations().delete(tEDegEdge);
    conn.tableOperations().delete(tEDegWeightEdge);
//    conn.tableOperations().delete(tEDegOut);
  }


}
