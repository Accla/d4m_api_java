package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.tricount.FixedIntegerLexicoder;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static edu.mit.ll.graphulo.Graphulo.TRICOUNT_TEMP_TABLE_SUFFIX;
import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_BYTES;

/**
 * Test kTruss, Jaccard and other algorithms.
 */
public class AlgorithmTest extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(AlgorithmTest.class);

  private enum KTrussAdjAlg { Normal, Fused, Client_Sparse, Client_Dense, Smart }

  @Test
  public void testkTrussAdj_Normal() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    testkTrussAdj_Inner(KTrussAdjAlg.Normal);
  }

  @Test
  public void testkTrussAdj_Fused() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    testkTrussAdj_Inner(KTrussAdjAlg.Fused);
  }

  @Test
  public void testkTrussAdj_Client_Dense() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    testkTrussAdj_Inner(KTrussAdjAlg.Client_Dense);
  }

  @Test
  public void testkTrussAdj_Client_Sparse() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    testkTrussAdj_Inner(KTrussAdjAlg.Client_Sparse);
  }

  @Test
  public void testkTrussAdj_Smart() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    testkTrussAdj_Inner(KTrussAdjAlg.Smart);
  }


  private void testkTrussAdj_Inner(KTrussAdjAlg alg) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    final Connector conn = tester.getConnector();
    final String tA, tR;
    {
      String[] names = getUniqueNames(2);
      tA = names[0];
      tR = names[1];
    }

    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v1", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "v3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "v4"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "v3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v3", "", "v4"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.putAll(GraphuloUtil.transposeMap(input));
      expect.putAll(input);
      input.put(new Key("v2", "", "v5"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v5", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long nnzkTruss;
      switch(alg) {
        case Normal:
          nnzkTruss = graphulo.kTrussAdj(tA, tR, 3, null, true, Authorizations.EMPTY, "");
          break;
        case Fused:
          nnzkTruss = graphulo.kTrussAdj_Fused(tA, tR, 3, null, true, Authorizations.EMPTY, "");
          break;
        case Client_Sparse:
          nnzkTruss = graphulo.kTrussAdj_Client(tA, tR, 3, null, Authorizations.EMPTY, "", true, Integer.MAX_VALUE);
          break;
        case Client_Dense:
          nnzkTruss = graphulo.kTrussAdj_Client(tA, tR, 3, null, Authorizations.EMPTY, "", false, Integer.MAX_VALUE);
          break;
        case Smart:
          nnzkTruss = graphulo.kTrussAdj_Smart(tA, tR, 3, null, true, Authorizations.EMPTY, "", Integer.MAX_VALUE, null);
          break;
        default: throw new AssertionError();
      }
      log.info("3-Truss has " + nnzkTruss + " nnz");

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      if (alg == KTrussAdjAlg.Normal)
        Assert.assertEquals(10, nnzkTruss);
      Assert.assertEquals(expect, actual);
    }

    conn.tableOperations().delete(tR);
    // Now test 4-truss
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v2", "", "v4"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v4", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      expect.putAll(input);
      GraphuloUtil.writeEntries(conn, input, tA, false);
//      Thread.sleep(200);
    }
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long nnzkTruss;
      switch(alg) {
        case Normal:
          nnzkTruss = graphulo.kTrussAdj(tA, tR, 4, null, true, Authorizations.EMPTY, "");
          break;
        case Fused:
          nnzkTruss = graphulo.kTrussAdj_Fused(tA, tR, 4, null, true, Authorizations.EMPTY, "");
          break;
        case Client_Sparse:
          nnzkTruss = graphulo.kTrussAdj_Client(tA, tR, 4, null, Authorizations.EMPTY, "", true, Integer.MAX_VALUE);
          break;
        case Client_Dense:
          nnzkTruss = graphulo.kTrussAdj_Client(tA, tR, 4, null, Authorizations.EMPTY, "", false, Integer.MAX_VALUE);
          break;
        case Smart:
          nnzkTruss = graphulo.kTrussAdj_Smart(tA, tR, 4, null, true, Authorizations.EMPTY, "", Integer.MAX_VALUE, null);
          break;
        default: throw new AssertionError();
      }
      log.info("4-Truss has " + nnzkTruss + " nnz");

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      if (alg == KTrussAdjAlg.Normal)
        Assert.assertEquals(12, nnzkTruss);
      Assert.assertEquals(expect, actual);
    }

    conn.tableOperations().delete(tR);
    // Now test 4-truss with filter
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String filterRowCol = "v1,:,v4,";
      long nnzkTruss;
      switch(alg) {
        case Normal:
          nnzkTruss = graphulo.kTrussAdj(tA, tR, 4, filterRowCol, true, Authorizations.EMPTY, "");
          break;
        case Fused:
          nnzkTruss = graphulo.kTrussAdj_Fused(tA, tR, 4, filterRowCol, true, Authorizations.EMPTY, "");
          break;
        case Client_Sparse:
          nnzkTruss = graphulo.kTrussAdj_Client(tA, tR, 4, filterRowCol, Authorizations.EMPTY, "", true, Integer.MAX_VALUE);
          break;
        case Client_Dense:
          nnzkTruss = graphulo.kTrussAdj_Client(tA, tR, 4, filterRowCol, Authorizations.EMPTY, "", false, Integer.MAX_VALUE);
          break;
        case Smart:
          nnzkTruss = graphulo.kTrussAdj_Smart(tA, tR, 4, null, true, Authorizations.EMPTY, "", Integer.MAX_VALUE, null);
          break;
        default: throw new AssertionError();
      }
      log.info("4-Truss has " + nnzkTruss + " nnz");

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      if (alg == KTrussAdjAlg.Normal)
        Assert.assertEquals(12, nnzkTruss);
      Assert.assertEquals(expect, actual);

      // ensure same answer after compacting
      conn.tableOperations().compact(tR, null, null, true, true);
      scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      if (alg == KTrussAdjAlg.Normal)
        Assert.assertEquals(12, nnzkTruss);
      Assert.assertEquals(expect, actual);
    }

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tR);
  }

  private final static FixedIntegerLexicoder FIL = FixedIntegerLexicoder.INSTANCE;
  // works with both String 1s and byte-encoded 1s. Does not work with non-1 Stings, but these are illegal in an unweighted adjacency matrix.
  private final static Value VALUE_ONE = new Value("1".getBytes(StandardCharsets.UTF_8)); // new Value(UINTEGER_LEXICODER.encode(1));
  private final static Value VALUE_EMPTY = new Value();

  @Test
  public void testTriCount()  throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    final Connector conn = tester.getConnector();
    final String tA = getUniqueNames(1)[0];

    {
      final Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v1", "", "v2"), VALUE_ONE);
      input.put(new Key("v1", "", "v3"), VALUE_ONE);
      input.put(new Key("v1", "", "v4"), VALUE_ONE);
      input.put(new Key("v2", "", "v3"), VALUE_ONE);
      input.put(new Key("v3", "", "v4"), VALUE_ONE);
      input.putAll(GraphuloUtil.transposeMap(input));
//      expect.putAll(input);
      input.put(new Key("v2", "", "v5"), VALUE_ONE);
      input.put(new Key("v5", "", "v2"), VALUE_ONE);
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }
    {
      final Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      final long triangles = graphulo.triCount(tA, null, Authorizations.EMPTY, null);
      log.info("triCount " + triangles + " triangles");
      Assert.assertEquals(2, triangles);
    }
    GraphuloUtil.deleteTables(conn, tA, tA+TRICOUNT_TEMP_TABLE_SUFFIX);
  }

  private byte[] bothBytes(final byte[] a, final byte[] b) {
    final byte[] r = new byte[8];
    System.arraycopy(a, 0, r, 0, 4);
    System.arraycopy(b, 0, r, 4, 4);
    return r;
  }

  @Test
  public void testTriCountAdjEdge()  throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    final Connector conn = tester.getConnector();
    final String tA, tE;
    {
      final String[] names = getUniqueNames(2);
      tA = names[0];
      tE = names[1];
    }

    {
      final Map<Key, Value> input = new HashMap<>();
      input.put(new Key(FIL.encode(2), EMPTY_BYTES, FIL.encode(1)), VALUE_EMPTY);
      input.put(new Key(FIL.encode(3), EMPTY_BYTES, FIL.encode(1)), VALUE_EMPTY);
      input.put(new Key(FIL.encode(4), EMPTY_BYTES, FIL.encode(1)), VALUE_EMPTY);
      input.put(new Key(FIL.encode(3), EMPTY_BYTES, FIL.encode(2)), VALUE_EMPTY);
      input.put(new Key(FIL.encode(4), EMPTY_BYTES, FIL.encode(3)), VALUE_EMPTY);
//      input.putAll(GraphuloUtil.transposeMap(input));
//      expect.putAll(input);
      input.put(new Key(FIL.encode(2), EMPTY_BYTES, FIL.encode(5)), VALUE_EMPTY);
//      input.put(new Key("v5", "", "v2"), VALUE_ONE);
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(FIL.encode(2)));
      TestUtil.createTestTable(conn, tA, splits, input);
    }
    {
      final Map<Key, Value> input = new HashMap<>();
      input.put(new Key(FIL.encode(1), EMPTY_BYTES, bothBytes(FIL.encode(1), FIL.encode(2))), VALUE_EMPTY);
      input.put(new Key(FIL.encode(1), EMPTY_BYTES, bothBytes(FIL.encode(1), FIL.encode(3))), VALUE_EMPTY);
      input.put(new Key(FIL.encode(1), EMPTY_BYTES, bothBytes(FIL.encode(1), FIL.encode(4))), VALUE_EMPTY);
      input.put(new Key(FIL.encode(2), EMPTY_BYTES, bothBytes(FIL.encode(2), FIL.encode(3))), VALUE_EMPTY);
      input.put(new Key(FIL.encode(3), EMPTY_BYTES, bothBytes(FIL.encode(3), FIL.encode(4))), VALUE_EMPTY);
      input.put(new Key(FIL.encode(2), EMPTY_BYTES, bothBytes(FIL.encode(2), FIL.encode(5))), VALUE_EMPTY);

      input.put(new Key(FIL.encode(2), EMPTY_BYTES, bothBytes(FIL.encode(1), FIL.encode(2))), VALUE_EMPTY);
      input.put(new Key(FIL.encode(3), EMPTY_BYTES, bothBytes(FIL.encode(1), FIL.encode(3))), VALUE_EMPTY);
      input.put(new Key(FIL.encode(4), EMPTY_BYTES, bothBytes(FIL.encode(1), FIL.encode(4))), VALUE_EMPTY);
      input.put(new Key(FIL.encode(3), EMPTY_BYTES, bothBytes(FIL.encode(2), FIL.encode(3))), VALUE_EMPTY);
      input.put(new Key(FIL.encode(4), EMPTY_BYTES, bothBytes(FIL.encode(3), FIL.encode(4))), VALUE_EMPTY);
      input.put(new Key(FIL.encode(5), EMPTY_BYTES, bothBytes(FIL.encode(2), FIL.encode(5))), VALUE_EMPTY);
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(FIL.encode(2)));
      TestUtil.createTestTable(conn, tE, splits, input);
    }
    {
      final Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      final long triangles = graphulo.triCountAdjEdge(tA, tE, null, Authorizations.EMPTY, null, null);
      log.info("triCountAdjEdge " + triangles + " triangles");
      Assert.assertEquals(2, triangles);
    }
    GraphuloUtil.deleteTables(conn, tA, tA+TRICOUNT_TEMP_TABLE_SUFFIX, tE);
  }

  private byte[] encodeRowNum(int i) {
    return new byte[] {
        ReverseByteTable[i & 0xFF],
        (byte) (i >> 24),
        (byte) (i >> 16),
        (byte) (i >> 8),
        (byte) i
    };
  }
  private byte[] encodeColQ(int i) {
    return new byte[] {
        (byte) (i >> 24),
        (byte) (i >> 16),
        (byte) (i >> 8),
        (byte) i
    };
  }

  private static final byte[] ReverseByteTable = new byte[] {
      (byte)0x00, (byte)0x80, (byte)0x40, (byte)0xc0, (byte)0x20, (byte)0xa0, (byte)0x60, (byte)0xe0,
      (byte)0x10, (byte)0x90, (byte)0x50, (byte)0xd0, (byte)0x30, (byte)0xb0, (byte)0x70, (byte)0xf0,
      (byte)0x08, (byte)0x88, (byte)0x48, (byte)0xc8, (byte)0x28, (byte)0xa8, (byte)0x68, (byte)0xe8,
      (byte)0x18, (byte)0x98, (byte)0x58, (byte)0xd8, (byte)0x38, (byte)0xb8, (byte)0x78, (byte)0xf8,
      (byte)0x04, (byte)0x84, (byte)0x44, (byte)0xc4, (byte)0x24, (byte)0xa4, (byte)0x64, (byte)0xe4,
      (byte)0x14, (byte)0x94, (byte)0x54, (byte)0xd4, (byte)0x34, (byte)0xb4, (byte)0x74, (byte)0xf4,
      (byte)0x0c, (byte)0x8c, (byte)0x4c, (byte)0xcc, (byte)0x2c, (byte)0xac, (byte)0x6c, (byte)0xec,
      (byte)0x1c, (byte)0x9c, (byte)0x5c, (byte)0xdc, (byte)0x3c, (byte)0xbc, (byte)0x7c, (byte)0xfc,
      (byte)0x02, (byte)0x82, (byte)0x42, (byte)0xc2, (byte)0x22, (byte)0xa2, (byte)0x62, (byte)0xe2,
      (byte)0x12, (byte)0x92, (byte)0x52, (byte)0xd2, (byte)0x32, (byte)0xb2, (byte)0x72, (byte)0xf2,
      (byte)0x0a, (byte)0x8a, (byte)0x4a, (byte)0xca, (byte)0x2a, (byte)0xaa, (byte)0x6a, (byte)0xea,
      (byte)0x1a, (byte)0x9a, (byte)0x5a, (byte)0xda, (byte)0x3a, (byte)0xba, (byte)0x7a, (byte)0xfa,
      (byte)0x06, (byte)0x86, (byte)0x46, (byte)0xc6, (byte)0x26, (byte)0xa6, (byte)0x66, (byte)0xe6,
      (byte)0x16, (byte)0x96, (byte)0x56, (byte)0xd6, (byte)0x36, (byte)0xb6, (byte)0x76, (byte)0xf6,
      (byte)0x0e, (byte)0x8e, (byte)0x4e, (byte)0xce, (byte)0x2e, (byte)0xae, (byte)0x6e, (byte)0xee,
      (byte)0x1e, (byte)0x9e, (byte)0x5e, (byte)0xde, (byte)0x3e, (byte)0xbe, (byte)0x7e, (byte)0xfe,
      (byte)0x01, (byte)0x81, (byte)0x41, (byte)0xc1, (byte)0x21, (byte)0xa1, (byte)0x61, (byte)0xe1,
      (byte)0x11, (byte)0x91, (byte)0x51, (byte)0xd1, (byte)0x31, (byte)0xb1, (byte)0x71, (byte)0xf1,
      (byte)0x09, (byte)0x89, (byte)0x49, (byte)0xc9, (byte)0x29, (byte)0xa9, (byte)0x69, (byte)0xe9,
      (byte)0x19, (byte)0x99, (byte)0x59, (byte)0xd9, (byte)0x39, (byte)0xb9, (byte)0x79, (byte)0xf9,
      (byte)0x05, (byte)0x85, (byte)0x45, (byte)0xc5, (byte)0x25, (byte)0xa5, (byte)0x65, (byte)0xe5,
      (byte)0x15, (byte)0x95, (byte)0x55, (byte)0xd5, (byte)0x35, (byte)0xb5, (byte)0x75, (byte)0xf5,
      (byte)0x0d, (byte)0x8d, (byte)0x4d, (byte)0xcd, (byte)0x2d, (byte)0xad, (byte)0x6d, (byte)0xed,
      (byte)0x1d, (byte)0x9d, (byte)0x5d, (byte)0xdd, (byte)0x3d, (byte)0xbd, (byte)0x7d, (byte)0xfd,
      (byte)0x03, (byte)0x83, (byte)0x43, (byte)0xc3, (byte)0x23, (byte)0xa3, (byte)0x63, (byte)0xe3,
      (byte)0x13, (byte)0x93, (byte)0x53, (byte)0xd3, (byte)0x33, (byte)0xb3, (byte)0x73, (byte)0xf3,
      (byte)0x0b, (byte)0x8b, (byte)0x4b, (byte)0xcb, (byte)0x2b, (byte)0xab, (byte)0x6b, (byte)0xeb,
      (byte)0x1b, (byte)0x9b, (byte)0x5b, (byte)0xdb, (byte)0x3b, (byte)0xbb, (byte)0x7b, (byte)0xfb,
      (byte)0x07, (byte)0x87, (byte)0x47, (byte)0xc7, (byte)0x27, (byte)0xa7, (byte)0x67, (byte)0xe7,
      (byte)0x17, (byte)0x97, (byte)0x57, (byte)0xd7, (byte)0x37, (byte)0xb7, (byte)0x77, (byte)0xf7,
      (byte)0x0f, (byte)0x8f, (byte)0x4f, (byte)0xcf, (byte)0x2f, (byte)0xaf, (byte)0x6f, (byte)0xef,
      (byte)0x1f, (byte)0x9f, (byte)0x5f, (byte)0xdf, (byte)0x3f, (byte)0xbf, (byte)0x7f, (byte)0xff
  };

  @Test
  public void testTriCountMagic2()  throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    final Connector conn = tester.getConnector();
    final String tA = getUniqueNames(1)[0];

    {
      final Map<Key, Value> input = new HashMap<>();
      input.put(new Key(encodeRowNum(1), EMPTY_BYTES, encodeColQ(2)), VALUE_EMPTY);
      input.put(new Key(encodeRowNum(1), EMPTY_BYTES, encodeColQ(3)), VALUE_EMPTY);
      input.put(new Key(encodeRowNum(1), EMPTY_BYTES, encodeColQ(4)), VALUE_EMPTY);
      input.put(new Key(encodeRowNum(2), EMPTY_BYTES, encodeColQ(3)), VALUE_EMPTY);
      input.put(new Key(encodeRowNum(3), EMPTY_BYTES, encodeColQ(4)), VALUE_EMPTY);
      input.put(new Key(encodeRowNum(2), EMPTY_BYTES, encodeColQ(1)), VALUE_EMPTY);
      input.put(new Key(encodeRowNum(3), EMPTY_BYTES, encodeColQ(1)), VALUE_EMPTY);
      input.put(new Key(encodeRowNum(4), EMPTY_BYTES, encodeColQ(1)), VALUE_EMPTY);
      input.put(new Key(encodeRowNum(3), EMPTY_BYTES, encodeColQ(2)), VALUE_EMPTY);
      input.put(new Key(encodeRowNum(4), EMPTY_BYTES, encodeColQ(3)), VALUE_EMPTY);
      input.put(new Key(encodeRowNum(2), EMPTY_BYTES, encodeColQ(5)), VALUE_EMPTY);
      input.put(new Key(encodeRowNum(5), EMPTY_BYTES, encodeColQ(2)), VALUE_EMPTY);
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(new byte[]{3}));
      splits.add(new Text(new byte[]{7}));
      splits.add(new Text(new byte[]{11}));
      TestUtil.createTestTable(conn, tA, splits, input);
    }
    {
      final Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      final int triangles = graphulo.triCountMagic2(tA, null, Authorizations.EMPTY, null);
      log.info("triCount " + triangles + " triangles");
      Assert.assertEquals(2, triangles);
    }
    GraphuloUtil.deleteTables(conn, tA, tA+TRICOUNT_TEMP_TABLE_SUFFIX);
  }



  @Test
  public void testkTrussEdge() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Connector conn = tester.getConnector();
    final String tE, tET, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tE = names[0];
      tET = names[1];
      tR = names[2];
      tRT = names[3];
    }

    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("e1", "", "v1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e1", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e2", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e2", "", "v3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e3", "", "v1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e3", "", "v4"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e4", "", "v3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e4", "", "v4"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e5", "", "v1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e5", "", "v3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      expect.putAll(input);
      expectTranspose.putAll(GraphuloUtil.transposeMap(expect));
      input.put(new Key("e6", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e6", "", "v5"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("e22"));
      TestUtil.createTestTable(conn, tE, splits, input);
      splits.clear();
      splits.add(new Text("v22"));
      TestUtil.createTestTable(conn, tET, splits, GraphuloUtil.transposeMap(input));
    }
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long nnzkTruss = graphulo.kTrussEdge(tE, tET, tR, tRT, 3, null, true, Authorizations.EMPTY);
      log.info("3Truss has " + nnzkTruss + " nnz");

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
      Assert.assertEquals(10, nnzkTruss);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectTranspose, actualTranspose);
      Assert.assertEquals(10, nnzkTruss);
    }

    // Now test 4-truss
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("e7", "", "v4"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e7", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      expect.putAll(input);
      GraphuloUtil.writeEntries(conn, input, tE, false);
      Map<Key, Value> inputTranspose = GraphuloUtil.transposeMap(input);
      expectTranspose.putAll(inputTranspose);
      GraphuloUtil.writeEntries(conn, inputTranspose, tET, false);
    }
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long nnzkTruss = graphulo.kTrussEdge(tE, tET, tR, tRT, 3, null, true, Authorizations.EMPTY);
      log.info("4Truss has " + nnzkTruss + " nnz");

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(12, nnzkTruss);
      Assert.assertEquals(expect, actual);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectTranspose, actualTranspose);
      Assert.assertEquals(12, nnzkTruss);
    }
    conn.tableOperations().delete(tE);
    conn.tableOperations().delete(tET);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  private enum JaccardAlg { Normal, Client }

  @Test
  public void testJaccard_Normal() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    testJaccard_Inner(JaccardAlg.Normal);
  }

  @Test
  public void testJaccard_Client() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    testJaccard_Inner(JaccardAlg.Client);
  }

  public void testJaccard_Inner(JaccardAlg jalg) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Connector conn = tester.getConnector();
    final String tA, tADeg, tR;
    {
      String[] names = getUniqueNames(3);
      tA = names[0];
      tADeg = names[1];
      tR = names[2];
    }

    Map<Key,Double> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v1", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "v3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v1", "", "v4"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "v3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v3", "", "v4"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.putAll(GraphuloUtil.transposeMap(input));
      input.put(new Key("v2", "", "v5"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v5", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);

      input.clear();
      input.put(new Key("v1", "", "deg"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v2", "", "deg"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v3", "", "deg"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v4", "", "deg"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("v5", "", "deg"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      TestUtil.createTestTable(conn, tADeg, splits, input);

      expect.put(new Key("v1", "", "v2"), 0.2);
      expect.put(new Key("v1", "", "v3"), 0.5);
      expect.put(new Key("v1", "", "v4"), 0.25);
      expect.put(new Key("v1", "", "v5"), 1.0 / 3.0);
      expect.put(new Key("v2", "", "v3"), 0.2);
      expect.put(new Key("v2", "", "v4"), 2.0 / 3.0);
      expect.put(new Key("v3", "", "v4"), 0.25);
      expect.put(new Key("v3", "", "v5"), 1.0 / 3.0);
    }

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long npp;
    switch (jalg) {
      case Normal:
        npp = graphulo.Jaccard(tA, tADeg, tR, null, Authorizations.EMPTY, "");
        break;
      case Client:
        npp = graphulo.Jaccard_Client(tA, tR, null, Authorizations.EMPTY, "");
        break;
      default:
        throw new AssertionError("jalg: "+jalg);
    }
    log.info("Jaccard table has "+npp+" #partial products sent to "+tR);

    // Just for fun, let's compact and ensure idempotence.
    conn.tableOperations().compact(tR, null, null, true, true);

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), Double.valueOf(entry.getValue().toString()));
    }
    scanner.close();
    System.out.println("Jaccard test:");
    TestUtil.printExpectActual(expect, actual);
//    switch (jalg) {
//      case Normal:
//        Assert.assertEquals(10, npp);
//        break;
//      case Client:
//        Assert.assertEquals(8, npp);
//        break;
//    }
    // need to be careful about comparing doubles
    for (Map.Entry<Key, Double> actualEntry : actual.entrySet()) {
      double actualValue = actualEntry.getValue();
      Assert.assertTrue(expect.containsKey(actualEntry.getKey()));
      double expectValue = expect.get(actualEntry.getKey());
      Assert.assertEquals(expectValue, actualValue, 0.001);
    }

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tADeg);
    conn.tableOperations().delete(tR);
  }


  @Test
  public void testNMF() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Connector conn = tester.getConnector();
    final String tE, tET, tW, tWT, tH, tHT, tWH, tR;
    {
      String[] names = getUniqueNames(8);
      tE = names[0];
      tET = names[1];
      tW = names[2];
      tWT = names[3];
      tH = names[4];
      tHT = names[5];
      tWH = names[6];
      tR = names[7];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("e1", "", "v1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e1", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e2", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e2", "", "v3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e3", "", "v1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e3", "", "v4"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e4", "", "v3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e4", "", "v4"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e5", "", "v1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e5", "", "v3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e6", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e6", "", "v5"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("e22"));
      TestUtil.createTestTable(conn, tE, splits, input);
      splits.clear();
      splits.add(new Text("v22"));
      TestUtil.createTestTable(conn, tET, splits, GraphuloUtil.transposeMap(input));
    }

//    DistributedTrace.enable("testNMF");

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    int maxIter = 3;
    long t = System.currentTimeMillis();
    int K = 3;
    double error = graphulo.NMF(tE, tET, tW, tWT, tH, tHT, K, maxIter, true, 0, 2);
    System.out.println("Trace is "+ org.apache.htrace.Trace.isTracing()+"; NMF time "+(System.currentTimeMillis()-t));
    log.info("NMF error " + error);

    DistributedTrace.disable();

    System.out.println("A:");
    Scanner scanner = conn.createScanner(tE, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();

    System.out.println("W:");
    scanner = conn.createScanner(tW, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();

    System.out.println("H:");
    scanner = conn.createScanner(tH, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();

//    try {
//      Thread.sleep(5000);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }

    graphulo.TableMult(tWT, tH, tWH, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.DOUBLE, "", false),
        MathTwoScalar.combinerSetting(Graphulo.PLUS_ITERATOR_BIGDECIMAL.getPriority(), null, MathTwoScalar.ScalarOp.PLUS, MathTwoScalar.ScalarType.DOUBLE, false),
        null, null, null, false, false, -1);

    System.out.println("WH:");
    scanner = conn.createScanner(tWH, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();


    // last test for the doHT_HHTinv method
    graphulo.doHT_HHTinv(tH, tHT, K, tR, true);


    conn.tableOperations().delete(tE);
    conn.tableOperations().delete(tET);
    conn.tableOperations().delete(tW);
    conn.tableOperations().delete(tWT);
    conn.tableOperations().delete(tH);
    conn.tableOperations().delete(tHT);
    conn.tableOperations().delete(tWH);
    conn.tableOperations().delete(tR);
  }

  @Test
  public void testNMF_Client() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Connector conn = tester.getConnector();
    final String tE, tET, tW, tWT, tH, tHT, tWH;
    {
      String[] names = getUniqueNames(7);
      tE = names[0];
      tW = names[2];
      tH = names[4];
//      tWH = names[6];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("e1", "", "v1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e1", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e2", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e2", "", "v3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e3", "", "v1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e3", "", "v4"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e4", "", "v3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e4", "", "v4"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e5", "", "v1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e5", "", "v3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e6", "", "v2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e6", "", "v5"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("e22"));
      TestUtil.createTestTable(conn, tE, splits, input);
//      splits.clear();
//      splits.add(new Text("v22"));
//      TestUtil.createTestTable(conn, tET, splits, TestUtil.transposeMap(input));
    }

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    int K = 3;
    int maxIter = 25;
    boolean trace = false;
    long t = System.currentTimeMillis();
    double error = graphulo.NMF_Client(tE, false, tW, false, tH, false, K, maxIter, 0.0, 3);
    System.out.println("Trace is " + trace + "; Client NMF time " + (System.currentTimeMillis() - t));
    log.info("NMF error " + error);

    System.out.println("A:");
    Scanner scanner = conn.createScanner(tE, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();

    System.out.println("W:");
    scanner = conn.createScanner(tW, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();

    System.out.println("H:");
    scanner = conn.createScanner(tH, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();

//    graphulo.TableMult(tWT, tH, tWH, null, -1,
//        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.DOUBLE),
//        MathTwoScalar.combinerSetting(Graphulo.PLUS_ITERATOR_BIGDECIMAL.getPriority(), null, MathTwoScalar.ScalarOp.PLUS, MathTwoScalar.ScalarType.DOUBLE),
//        null, null, null, false, false, -1, false);
//
//    System.out.println("WH:");
//    scanner = conn.createScanner(tWH, Authorizations.EMPTY);
//    for (Map.Entry<Key, Value> entry : scanner) {
//      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
//    }
//    scanner.close();

    conn.tableOperations().delete(tE);
//    conn.tableOperations().delete(tET);
    conn.tableOperations().delete(tW);
//    conn.tableOperations().delete(tWT);
    conn.tableOperations().delete(tH);
//    conn.tableOperations().delete(tHT);
//    conn.tableOperations().delete(tWH);
  }

  /**
   * ans =
     7     6     0     0     0
     2     0     6     0     0
     3     0     0     0     1
     0     2     0     0     0
     0     0     0     9     1
     2     0     1     4     0
     ans =
     0.4246    0.5071         0         0         0
     0.1971         0    0.8240         0         0
     0.5913         0         0         0    0.2747
     0    1.0986         0         0         0
     0         0         0    0.9888    0.1099
     0.2253         0    0.1569    0.6278         0

   */
  @Test
  public void testTfidf() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Connector conn = tester.getConnector();
    final String tEDeg, tET, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tET = names[0];
      tEDeg = names[1];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Double> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    Map<Key, Value> input = new HashMap<>(), indeg = new HashMap<>();
    {
      input.put(new Key("e1", "", "v1"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e1", "", "v2"), new Value("6".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e2", "", "v1"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e2", "", "v3"), new Value("6".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e3", "", "v1"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e3", "", "v5"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e4", "", "v2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e5", "", "v4"), new Value("9".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e5", "", "v5"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e6", "", "v1"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e6", "", "v3"), new Value("1".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("e6", "", "v4"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v22"));
      TestUtil.createTestTable(conn, tET, splits, GraphuloUtil.transposeMap(input));

      indeg.put(new Key("e1", "", "Degree"), new Value("13".getBytes(StandardCharsets.UTF_8)));
      indeg.put(new Key("e2", "", "Degree"), new Value("8".getBytes(StandardCharsets.UTF_8)));
      indeg.put(new Key("e3", "", "Degree"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      indeg.put(new Key("e4", "", "Degree"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      indeg.put(new Key("e5", "", "Degree"), new Value("10".getBytes(StandardCharsets.UTF_8)));
      indeg.put(new Key("e6", "", "Degree"), new Value("7".getBytes(StandardCharsets.UTF_8)));
      TestUtil.createTestTable(conn, tEDeg, null, indeg);

      expect.put(new Key("e1", "", "v1"), 0.4246);
      expect.put(new Key("e1", "", "v2"), 0.5071);
      expect.put(new Key("e2", "", "v1"), 0.1971);
      expect.put(new Key("e2", "", "v3"), 0.8240);
      expect.put(new Key("e3", "", "v1"), 0.5913);
      expect.put(new Key("e3", "", "v5"), 0.2747);
      expect.put(new Key("e4", "", "v2"), 1.0986);
      expect.put(new Key("e5", "", "v4"), 0.9888);
      expect.put(new Key("e5", "", "v5"), 0.1099);
      expect.put(new Key("e6", "", "v1"), 0.2253);
      expect.put(new Key("e6", "", "v3"), 0.1569);
      expect.put(new Key("e6", "", "v4"), 0.6278);
    }

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numEntriesResultTable = graphulo.doTfidf(tET, tEDeg, -1, tR, tRT);


    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), Double.valueOf(entry.getValue().toString()));
    }
    scanner.close();
    System.out.println("Tfidf test:");
    TestUtil.printExpectActual(expect, actual);
    Assert.assertEquals(input.size(), numEntriesResultTable);
    // need to be careful about comparing doubles
    for (Map.Entry<Key, Double> actualEntry : actual.entrySet()) {
      double actualValue = actualEntry.getValue();
      Assert.assertTrue(expect.containsKey(actualEntry.getKey()));
      double expectValue = expect.get(actualEntry.getKey());
      Assert.assertEquals(expectValue, actualValue, 0.001);
    }

    conn.tableOperations().delete(tET);
    conn.tableOperations().delete(tEDeg);
    conn.tableOperations().delete(tRT);
    conn.tableOperations().delete(tR);
  }

  // for debugging
//  @Test
//  public void temp() {
//    Connector conn = tester.getConnector();
//    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
////    DebugUtil.printTable("DH_pg10_20160331_TgraphAdjUU", conn, "DH_pg10_20160331_TgraphAdjUU");
//    long numpp = graphulo.Jaccard("DH_pg10_20160331_TgraphAdjUU", "DH_pg10_20160331_TgraphAdjUUDeg", "j10", null, null, null);
//    System.out.println(numpp);
//  }

}
