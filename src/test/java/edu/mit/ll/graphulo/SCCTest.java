package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.GraphuloUtil;
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
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Test the SCC algorithm in {@link SCCGraphulo}.
 */
public class SCCTest extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(SCCTest.class);

    /**
     *      vBig
     *   /   |    \
     *  v    v     v
     * v0--->v1--->v2--v
     *  ^--<------<----/
     */
  @Test
  public void testSCC() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tRf;
    {
      String[] names = getUniqueNames(2);
      tA = names[0];
      tRf = names[1];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ), actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key,Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "v0"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "v0"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "v1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("vBig", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));

      Map<Key,Value> e = new HashMap<>();
      e.put(new Key("v0", "", "v1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      e.put(new Key("v0", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      e.put(new Key("v0", "", "v0"), new Value("1".getBytes(StandardCharsets.UTF_8)));

      e.put(new Key("v1", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      e.put(new Key("v1", "", "v0"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      e.put(new Key("v1", "", "v1"), new Value("1".getBytes(StandardCharsets.UTF_8)));

      e.put(new Key("v2", "", "v0"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      e.put(new Key("v2", "", "v1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      e.put(new Key("v2", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));

      expect.putAll(e);
      TestUtil.createTestTable(conn, tA, null, input);
    }

    SCCGraphulo sccgraphulo = new SCCGraphulo(conn, tester.getPassword());
    sccgraphulo.SCC(tA, tRf, 4, false);

    BatchScanner scanner = conn.createBatchScanner(tRf, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key,Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expect, actual);

    // Test interpreting SCC table
    SortedSet<String> expectSet = new TreeSet<>(new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        Collection<Text> t1 = GraphuloUtil.d4mRowToTexts(o1),
            t2 = GraphuloUtil.d4mRowToTexts(o2);
        if (t1.size() != t2.size())
          return t1.size() - t2.size();
        return t1.equals(t2) ? 0 : Integer.MAX_VALUE;
      }
    });
    expectSet.add("v0,v1,v2,");
    Assert.assertEquals(expectSet, sccgraphulo.SCCQuery(tRf, "v0,"));
    Assert.assertTrue(sccgraphulo.SCCQuery(tRf, "vBig,").isEmpty());
    Assert.assertEquals(expectSet, sccgraphulo.SCCQuery(tRf, null));

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tRf);
  }

}
