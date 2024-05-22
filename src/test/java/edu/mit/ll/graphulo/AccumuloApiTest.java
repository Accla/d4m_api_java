package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.GraphuloUtil;
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
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static edu.mit.ll.graphulo.Graphulo.DEFAULT_COMBINER_PRIORITY;
import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_BYTES;

/**
 * Test features of the Accumulo API that Graphulo relies on.
 */
public class AccumuloApiTest extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(AccumuloApiTest.class);

  /**
   * Tests Accumulo API to make sure Combiners pick up writes to the same key with same and differing timestamps,
   * in the same batch and across batches.
   */
  @Test
  public void testCombineSameWrites() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Connector conn = tester.getConnector();
    final String tR;
    {
      String[] names = getUniqueNames(1);
      tR = names[0];
    }

    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("v2", "", "v5"), new Value("1800".getBytes(StandardCharsets.UTF_8)));
    IteratorSetting RPlusIteratorSetting = new DynamicIteratorSetting(DEFAULT_COMBINER_PRIORITY, null)
        .append(MathTwoScalar.combinerSetting(1, null, MathTwoScalar.ScalarOp.PLUS, MathTwoScalar.ScalarType.LONG_OR_DOUBLE, false))
        .toIteratorSetting();

    // first test writing different timestamps. Some can be same.
    TestUtil.createTestTable(conn, tR);
    GraphuloUtil.applyIteratorSoft(RPlusIteratorSetting, conn.tableOperations(), tR);
    {
      BatchWriter bw = conn.createBatchWriter(tR, new BatchWriterConfig());
      Key k = new Key("v2", "", "v5");
      Value v = new Value("1".getBytes(StandardCharsets.UTF_8));
      Mutation m;

      for (int i = 0; i < 1800; i++) {
        m = new Mutation(k.getRowData().toArray());
        m.put(k.getColumnFamilyData().toArray(), k.getColumnQualifierData().toArray(),
            k.getColumnVisibilityParsed(), 20+((int)(Math.random()*100)), v.get());
        bw.addMutation(m);
        if (Math.random() < 0.2)
          bw.flush();
      }
      bw.close();

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
        log.debug("Entry "+actual.size()+": "+entry.getKey()+"    "+entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
    }

    // now test writing all same timestamp
    conn.tableOperations().delete(tR);
    TestUtil.createTestTable(conn, tR);
    GraphuloUtil.applyIteratorSoft(RPlusIteratorSetting, conn.tableOperations(), tR);
    {
      BatchWriter bw = conn.createBatchWriter(tR, new BatchWriterConfig());
      Key k = new Key("v2", "", "v5");
      Value v = new Value("1".getBytes(StandardCharsets.UTF_8));
      Mutation m;

      for (int i = 0; i < 1800; i++) {
        m = new Mutation(k.getRowData().toArray());
        m.put(k.getColumnFamilyData().toArray(), k.getColumnQualifierData().toArray(),
            k.getColumnVisibilityParsed(), 9, v.get());
        bw.addMutation(m);
        if (Math.random() < 0.1)
          bw.flush();
      }
      bw.close();

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
        log.debug("Entry "+actual.size()+": "+entry.getKey()+"    "+entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
    }

    conn.tableOperations().delete(tR);
  }

  static final ColumnVisibility NO_VISIBILITY = new ColumnVisibility();

  /**
   * Test behavior of Accumulo when splitting during writes.
   */
  @Test
  public void testSplitDuringWrite() throws TableNotFoundException, AccumuloSecurityException, AccumuloException, InterruptedException {
    Connector conn = tester.getConnector();
    TableOperations tops = conn.tableOperations();
    final String t = getUniqueNames(1)[0];
    TestUtil.createTestTable(conn, t);

    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    String threshold = "20K";
    try {
      tops.setProperty(t, "table.split.threshold", threshold);
    } catch (AccumuloException | AccumuloSecurityException e) {
      Assert.fail("cannot set table.split.threshold property for " + t + " to "+threshold);
    }

    {
      BatchWriterConfig bwc = new BatchWriterConfig();
      bwc.setMaxMemory(10000);
      BatchWriter bw = conn.createBatchWriter(t, new BatchWriterConfig());

      for (int i = 0; i < 500005; i++) {
        StringBuilder sb = new StringBuilder(Integer.toString(i));
        String vStr = sb.toString();
        byte[] v = vStr.getBytes(StandardCharsets.UTF_8);
        String rowStr = sb.reverse().toString();
        byte[] row = rowStr.getBytes(StandardCharsets.UTF_8);

        Mutation m = new Mutation(row);
        long ts = System.currentTimeMillis();
        m.put(EMPTY_BYTES, EMPTY_BYTES, NO_VISIBILITY, ts, v);
        bw.addMutation(m);
        expect.put(new Key(row, EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES, ts, false), new Value(v));

        if (i % 100000 == 0) {
          tops.flush(t, null, null, true);
//          Thread.sleep(500);
        }
        if (i % 200000 == 0) {
          SortedSet<Text> splits = new TreeSet<>();
          splits.add(new Text(Integer.toString(i/50000).getBytes(StandardCharsets.UTF_8)));
          tops.addSplits(t, splits);
        }
      }
      bw.close();

      BatchScanner scanner = conn.createBatchScanner(t, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      System.out.println("expect.size="+expect.size()+" actual.size="+actual.size());
      System.out.println("splits are "+tops.listSplits(t));

//      Set<Map.Entry<Key, Value>> es = expect.entrySet();
//      es.removeAll(actual.entrySet());
//      System.out.println("expect - actual is "+es);

      Assert.assertEquals(expect, actual);
    }
    conn.tableOperations().delete(t);
  }

}
