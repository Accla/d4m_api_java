package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.simplemult.ConstantTwoScalar;
import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.skvi.TwoTableIterator;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Test TableMult in {@link Graphulo}.
 */
public class TableMultTest extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(TableMultTest.class);

  /**
   * <pre>
   *      C1 C2        C1 C2 C3          B1  B2
   * A1 [ 5  2 ] * B1 [   3  3  ] = A1 [ 6   15+6 ]
   * A2 [ 4    ]   B2 [3  3     ]   A2 [     12   ]
   * </pre>
   */
  @Test
//  @Ignore("New version only works with BatchWriter. KnownBug: ACCUMULO-3645")
  public void test1() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tC, tAT, tB, tCT;
    {
      String[] names = getUniqueNames(4);
      tAT = names[0];
      tB = names[1];
      tC = names[2];
      tCT = names[3];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tB, null, input);
    }
    SortedMap<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes(StandardCharsets.UTF_8)));
    expect.put(new Key("A1", "", "B2"), new Value("21".getBytes(StandardCharsets.UTF_8)));
    expect.put(new Key("A2", "", "B2"), new Value("12".getBytes(StandardCharsets.UTF_8)));
    SortedMap<Key, Value> expectT = GraphuloUtil.transposeMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, tCT, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.BIGDECIMAL, "", false),
        Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        null, null, null, false, false, 1);

    Assert.assertEquals(4, numpp);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
    }

    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actualT = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actualT.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectT, actualT);
    } 

    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
    conn.tableOperations().delete(tCT);
  }

  /**
   * <pre>
   *      C1 C2        C1 C2 C3          B1  B2
   * A1 [ 5  2 ] * B1 [   3  3  ] = A1 [ 6   15+6 ]
   * A2 [ 4    ]   B2 [3  3     ]   A2 [     12   ]
   * </pre>
   */
  @Test
//  @Ignore("New version only works with BatchWriter. KnownBug: ACCUMULO-3645")
  public void testLongLex() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tC, tAT, tB, tCT;
    {
      String[] names = getUniqueNames(4);
      tAT = names[0];
      tB = names[1];
      tC = names[2];
      tCT = names[3];
    }
    Lexicoder<Long> lex = new LongLexicoder();
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value(lex.encode(5L)));
      input.put(new Key("A1", "", "C2"), new Value(lex.encode(2L)));
      input.put(new Key("A2", "", "C1"), new Value(lex.encode(4L)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value(lex.encode(3L)));
      input.put(new Key("B1", "", "C3"), new Value(lex.encode(3L)));
      input.put(new Key("B2", "", "C1"), new Value(lex.encode(3L)));
      input.put(new Key("B2", "", "C2"), new Value(lex.encode(3L)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tB, null, input);
    }
    SortedMap<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("A1", "", "B1"), new Value(lex.encode(6L)));
    expect.put(new Key("A1", "", "B2"), new Value(lex.encode(21L)));
    expect.put(new Key("A2", "", "B2"), new Value(lex.encode(12L)));
    SortedMap<Key, Value> expectT = GraphuloUtil.transposeMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, tCT, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LEX_LONG, "", false),
        MathTwoScalar.combinerSetting(6, null, MathTwoScalar.ScalarOp.PLUS, MathTwoScalar.ScalarType.LEX_LONG, false),
        null, null, null, false, false, 1);

    Assert.assertEquals(4, numpp);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
    }

    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actualT = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actualT.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectT, actualT);
    }

    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
    conn.tableOperations().delete(tCT);
  }

  /** @return null if user already has these authorizations; otherwise previous authorizations of the user. */
  private Authorizations grantAuthorizations(String... auths) throws AccumuloSecurityException, AccumuloException {
    Connector conn = tester.getConnector();
    String user = conn.whoami();

    Authorizations beforeAuthorizations = conn.securityOperations().getUserAuthorizations(user);
    boolean hasAllAuths = true;
    for (String auth : auths) {
      if (!beforeAuthorizations.contains(auth)) {
        hasAllAuths = false;
        break;
      }
    }
    if (hasAllAuths) {
      log.info("User " + user + " already has authorizations for " + Arrays.toString(auths) + "; no need to modify user permissions");
      return null;
    }
    else {
      Assume.assumeTrue("Cannot test authorizations because user " + user + " does not have ALTER_USER permission",
              conn.securityOperations().hasSystemPermission(user, SystemPermission.ALTER_USER));
      Authorizations tempAuthorizations;
      {
        List<byte[]> beforeAuthorizationsList = beforeAuthorizations.getAuthorizations();
        List<byte[]> tempAuthorizationsList = new ArrayList<>(beforeAuthorizationsList);
        for (String auth : auths) {
          if (!beforeAuthorizations.contains(auth))
            tempAuthorizationsList.add(auth.getBytes(StandardCharsets.UTF_8));
        }
        tempAuthorizations = new Authorizations(tempAuthorizationsList);
      }
      conn.securityOperations().changeUserAuthorizations(user, tempAuthorizations);
      log.info("Changing user "+user+" authorizations to: " + tempAuthorizations);
      return beforeAuthorizations;
    }
  }


  /**
   * <pre>
   *      C1 C2        C1 C2 C3          B1  B2
   * A1 [ 5  2 ] * B1 [   3  3  ] = A1 [ 6   15+6 ]
   * A2 [ 4    ]   B2 [3  3     ]   A2 [     12   ]
   * </pre>
   */
  @Test
//  @Ignore("New version only works with BatchWriter. KnownBug: ACCUMULO-3645")
  public void test1WithAuthoriations() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    String authA = "testvisA", authB = "testvisB", authC = "testvisC";
    String user = conn.whoami();
    Authorizations beforeAuthorizations = grantAuthorizations(authA, authB, authC);

    try {
      final String tC, tAT, tB, tCT;
      {
        String[] names = getUniqueNames(4);
        tAT = names[0];
        tB = names[1];
        tC = names[2];
        tCT = names[3];
      }
      {
        Map<Key, Value> input = new HashMap<>();
        input.put(new Key("A1", "", "C1", authA), new Value("5".getBytes(StandardCharsets.UTF_8)));
        input.put(new Key("A1", "", "C2", authA), new Value("2".getBytes(StandardCharsets.UTF_8)));
        input.put(new Key("A2", "", "C1", authA), new Value("4".getBytes(StandardCharsets.UTF_8)));
        input = GraphuloUtil.transposeMap(input);
        TestUtil.createTestTable(conn, tAT, null, input);
      }
      {
        Map<Key, Value> input = new HashMap<>();
        input.put(new Key("B1", "", "C2", authB), new Value("3".getBytes(StandardCharsets.UTF_8)));
        input.put(new Key("B1", "", "C3", authB), new Value("3".getBytes(StandardCharsets.UTF_8)));
        input.put(new Key("B2", "", "C1", authB), new Value("3".getBytes(StandardCharsets.UTF_8)));
        input.put(new Key("B2", "", "C2", authB), new Value("3".getBytes(StandardCharsets.UTF_8)));
        input = GraphuloUtil.transposeMap(input);
        TestUtil.createTestTable(conn, tB, null, input);
      }
      SortedMap<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
      expect.put(new Key("A1", "", "B1"), new Value("6".getBytes(StandardCharsets.UTF_8)));
      expect.put(new Key("A1", "", "B2"), new Value("21".getBytes(StandardCharsets.UTF_8)));
      expect.put(new Key("A2", "", "B2"), new Value("12".getBytes(StandardCharsets.UTF_8)));
      SortedMap<Key, Value> expectT = GraphuloUtil.transposeMap(expect);

      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long numpp = graphulo.TableMult(tAT, tB, tC, tCT, -1,
          MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.BIGDECIMAL, authC, false),
          Graphulo.PLUS_ITERATOR_BIGDECIMAL,
          null, null, null, false, false, null, null, null, null, null, 1,
          new Authorizations(authA), new Authorizations(authB));

      Assert.assertEquals(4, numpp);

      Scanner scanner = conn.createScanner(tC, new Authorizations(authC));
      {
        SortedMap<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
        for (Map.Entry<Key, Value> entry : scanner) {
          actual.put(entry.getKey(), entry.getValue());
        }
        scanner.close();
        Assert.assertEquals(expect, actual);
      }

      scanner = conn.createScanner(tCT, new Authorizations(authC));
      {
        SortedMap<Key, Value> actualT = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
        for (Map.Entry<Key, Value> entry : scanner) {
          actualT.put(entry.getKey(), entry.getValue());
        }
        scanner.close();
        Assert.assertEquals(expectT, actualT);
      }

      conn.tableOperations().delete(tAT);
      conn.tableOperations().delete(tB);
      conn.tableOperations().delete(tC);
      conn.tableOperations().delete(tCT);
    } finally {
      if (beforeAuthorizations != null) {
        conn.securityOperations().changeUserAuthorizations(user, beforeAuthorizations);
        log.info("Reverting user " + user + " authorizations to: " + beforeAuthorizations);
      }
    }
  }

  /**
   * <pre>
   *      C1 C2        C1 C2 C3          B1  B2
   * A1 [ x  2 ] * B1 [   3  3  ] = A1 [ 6   6 ]
   * A2 [ x    ]   B2 [x  3     ]   A2 [       ]
   * </pre>
   */
  @Test
  public void testRowFilter() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tAT, tB, tC;
    {
      String[] names = getUniqueNames(3);
      tAT = names[0];
      tB = names[1];
      tC = names[2];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("C15"));
      TestUtil.createTestTable(conn, tB, splits, input);
    }

    TestUtil.createTestTable(conn, tC);

    Map<Key, Value> expect = new HashMap<>();
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes(StandardCharsets.UTF_8)));
    expect.put(new Key("A1", "", "B2"), new Value("6".getBytes(StandardCharsets.UTF_8)));
    expect = Collections.unmodifiableMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG, "", false),
        Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        "C2,:,", null, null, false, false, 1);

    Assert.assertEquals(2, numpp);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);

    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
  }

  /**
   * <pre>
   *      C1 C2        C1 C2 C3          B1  B2
   * A1 [ x  2 ] * B1 [   3  3  ] = A1 [ 6     ]
   * A2 [ x    ]   B2 [x  xx    ]   A2 [       ]
   * </pre>
   */
  @Test
  public void testRowFilterColFilterATB() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tAT, tB, tC, tCT;
    {
      String[] names = getUniqueNames(4);
      tAT = names[0];
      tB = names[1];
      tC = names[2];
      tCT = names[3];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("C15"));
      TestUtil.createTestTable(conn, tB, splits, input);
    }
    TestUtil.createTestTable(conn, tC);

    Map<Key, Value> expect = new HashMap<>();
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes(StandardCharsets.UTF_8)));

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG, "", false),
        Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        "C2,:,",
        "A1,", "B1,", false, false, 1);

    Assert.assertEquals(1, numpp);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);
    scanner.close();

    // now check more advanced column filter, write to transpose
    expect = GraphuloUtil.transposeMap(expect);
    graphulo.TableMult(tAT, tB, null, tCT, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG, "", false),
        Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        "C2,:,",
        "A1,:,A15,", "B1,:,B15,F,", false, false, 1);
    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);
    scanner.close();

    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
    conn.tableOperations().delete(tCT);
  }


  /**
   * Adds in A*A at the same time as A*B.
   * <pre>
   *      C1 C2        C1 C2 C3          B1  B2
   * A1 [ x  2 ] * B1 [   3  3  ] = A1 [ 6     ]
   * A2 [ x    ]   B2 [x  xx    ]   A2 [       ]
   * </pre>
   */
  @Test
  public void testAlsoDoAA() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tAT, tB, tC, tCT;
    {
      String[] names = getUniqueNames(4);
      tAT = names[0];
      tB = names[1];
      tC = names[2];
      tCT = names[3];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("C15"));
      TestUtil.createTestTable(conn, tB, splits, input);
    }
    TestUtil.createTestTable(conn, tC);

    Map<Key, Value> expect = new HashMap<>();
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes(StandardCharsets.UTF_8)));
    expect.put(new Key("A1", "", "A1"), new Value("4".getBytes(StandardCharsets.UTF_8)));

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG, "", false),
        Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        "C2,:,",
        "A1,", "B1,", true, false, 1);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);
    Assert.assertEquals(2, numpp);
    scanner.close();

    // now check more advanced column filter, write to transpose
    expect = GraphuloUtil.transposeMap(expect);
    graphulo.TableMult(tAT, tB, null, tCT, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG, "", false),
        Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        "C2,:,",
        "A1,:,A15,", "B1,:,B15,F,", true, false, 1);
    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);
    scanner.close();

    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
    conn.tableOperations().delete(tCT);
  }

  /**
   * Adds in A*A at the same time as A*B.
   * <pre>
   *      C1 C2 T      C1 C2 C3          B1  A1  A2
   * A1 [ x  2 ] * B1 [   3  3  ] = A1 [ 6          ]
   * A2 [ x    ]   B2 [x  xx    ]   C1 [            ]
   *                                C2 [     2      ]
   *                                A1 [     4      ]
   * </pre>
   */
  @Test
  public void testAlsoDoAAAlsoEmitA() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tAT, tB, tC, tCT;
    {
      String[] names = getUniqueNames(4);
      tAT = names[0];
      tB = names[1];
      tC = names[2];
      tCT = names[3];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("C15"));
      TestUtil.createTestTable(conn, tB, splits, input);
    }
    TestUtil.createTestTable(conn, tC);

    Map<Key, Value> expect = new HashMap<>();
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes(StandardCharsets.UTF_8)));
    expect.put(new Key("A1", "", "A1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
    expect.put(new Key("C2", "", "A1"), new Value("2".getBytes(StandardCharsets.UTF_8)));

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG, "", false),
        Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        "C2,:,",
        "A1,", "B1,", true, false, true, false,
        null, null, null, null, null, 1, null, null);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);
    Assert.assertEquals(3, numpp);
    scanner.close();

    // now check more advanced column filter, write to transpose
    expect = GraphuloUtil.transposeMap(expect);
    graphulo.TableMult(tAT, tB, null, tCT, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG, "", false),
        Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        "C2,:,",
        "A1,:,A15,", "B1,:,B15,F,", true, false, true, false,
        null, null, null, null, null, 1, null, null);
    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);
    scanner.close();

    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
    conn.tableOperations().delete(tCT);
  }


  /**
   * <pre>
   *      C1 C2
   * A1 [ 5  2 ]
   * A2 [ 4    ]
   * </pre>
   */
  @Test
  public void testCloneMultiply() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tC, tAT, tB, tCT;
    {
      String[] names = getUniqueNames(3);
      tAT = TwoTableIterator.CLONESOURCE_TABLENAME;
      tB = names[0];
      tC = names[1];
      tCT = names[2];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tB, null, input);
    }
    SortedMap<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("A1", "", "A1"), new Value("29".getBytes(StandardCharsets.UTF_8))); // 5*5 + 2*2
    expect.put(new Key("A1", "", "A2"), new Value("20".getBytes(StandardCharsets.UTF_8))); // 5*4
    expect.put(new Key("A2", "", "A1"), new Value("20".getBytes(StandardCharsets.UTF_8))); // 5*4
    expect.put(new Key("A2", "", "A2"), new Value("16".getBytes(StandardCharsets.UTF_8))); // 4*4
    SortedMap<Key, Value> expectT = GraphuloUtil.transposeMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, tCT, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.BIGDECIMAL, "", false),
        Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        null, null, null, false, false, 1);

    Assert.assertEquals(5, numpp);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
//      DebugUtil.printMapFull(actual.entrySet().iterator(), 5);
      Assert.assertEquals(expect, actual);
    }

    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actualT = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actualT.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectT, actualT);
    }

    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
    conn.tableOperations().delete(tCT);
  }

  /**
   * <pre>
   *      C1 C2
   * A1 [ 5  2 ]
   * A2 [ 4    ]
   * </pre>
   */
  @Test
  public void testCloneMultiplyAndEmitA() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tC, tAT, tB, tCT;
    {
      String[] names = getUniqueNames(3);
      tAT = TwoTableIterator.CLONESOURCE_TABLENAME;
      tB = names[0];
      tC = names[1];
      tCT = names[2];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tB, null, input);
    }
    SortedMap<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("A1", "", "A1"), new Value("29".getBytes(StandardCharsets.UTF_8))); // 5*5 + 2*2
    expect.put(new Key("A1", "", "A2"), new Value("20".getBytes(StandardCharsets.UTF_8))); // 5*4
    expect.put(new Key("A2", "", "A1"), new Value("20".getBytes(StandardCharsets.UTF_8))); // 5*4
    expect.put(new Key("A2", "", "A2"), new Value("16".getBytes(StandardCharsets.UTF_8))); // 4*4
    expect.put(new Key("C1", "", "A1"), new Value("5".getBytes(StandardCharsets.UTF_8))); // A
    expect.put(new Key("C2", "", "A1"), new Value("2".getBytes(StandardCharsets.UTF_8))); // A
    expect.put(new Key("C1", "", "A2"), new Value("4".getBytes(StandardCharsets.UTF_8))); // A
    SortedMap<Key, Value> expectT = GraphuloUtil.transposeMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, tCT, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.BIGDECIMAL, "", false),
        Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        null, null, null, false, false,
        true, false, null, null, null, null, null, 1, null, null);

    Assert.assertEquals(8, numpp);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
//      DebugUtil.printMapFull(actual.entrySet().iterator(), 5);
      Assert.assertEquals(expect, actual);
    }

    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actualT = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actualT.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectT, actualT);
    }

    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
    conn.tableOperations().delete(tCT);
  }

  /**
   * <pre>
   *      C1 C2
   * A1 [ 5  2 ]
   * A2 [ 4    ]
   * </pre>
   */
  @Test
  public void testCloneMultiplyAndEmitAAndUseIterators() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tC, tAT, tB, tCT;
    {
      String[] names = getUniqueNames(3);
      tAT = TwoTableIterator.CLONESOURCE_TABLENAME;
      tB = names[0];
      tC = names[1];
      tCT = names[2];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tB, null, input);
    }
    SortedMap<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("A1", "", "A1"), new Value("7".getBytes(StandardCharsets.UTF_8))); // 1*5 + 1*2
    expect.put(new Key("A1", "", "A2"), new Value("4".getBytes(StandardCharsets.UTF_8))); // 1*4
    expect.put(new Key("A2", "", "A1"), new Value("5".getBytes(StandardCharsets.UTF_8))); // 1*5
    expect.put(new Key("A2", "", "A2"), new Value("4".getBytes(StandardCharsets.UTF_8))); // 1*4
    expect.put(new Key("C1", "", "A1"), new Value("1".getBytes(StandardCharsets.UTF_8))); // A
    expect.put(new Key("C2", "", "A1"), new Value("1".getBytes(StandardCharsets.UTF_8))); // A
    expect.put(new Key("C1", "", "A2"), new Value("1".getBytes(StandardCharsets.UTF_8))); // A
    SortedMap<Key, Value> expectT = GraphuloUtil.transposeMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, tCT, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.BIGDECIMAL, "", false),
        Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        null, null, null, false, false,
        true, false, Collections.singletonList(ConstantTwoScalar.iteratorSetting(1, new Value("1".getBytes(StandardCharsets.UTF_8)))),
        null, null, null, null, 1, null, null);

    Assert.assertEquals(8, numpp);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
//      DebugUtil.printMapFull(actual.entrySet().iterator(), 5);
      Assert.assertEquals(expect, actual);
    }

    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actualT = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actualT.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectT, actualT);
    }

    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
    conn.tableOperations().delete(tCT);
  }

  /**
   * <pre>
   *      C1 C2
   * A1 [ 5  2 ]
   * A2 [ 4    ]
   * </pre>
   */
  @Test
  public void testCloneMultiplyAndEmitAAndUseIterators2() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tC, tAT, tB, tCT;
    {
      String[] names = getUniqueNames(3);
      tAT = TwoTableIterator.CLONESOURCE_TABLENAME;
      tB = names[0];
      tC = names[1];
      tCT = names[2];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tB, null, input);
    }
    SortedMap<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("A1", "", "A1"), new Value("13".getBytes(StandardCharsets.UTF_8))); // 1*8 + 1*5
    expect.put(new Key("A1", "", "A2"), new Value("7".getBytes(StandardCharsets.UTF_8))); // 1*7
    expect.put(new Key("A2", "", "A1"), new Value("8".getBytes(StandardCharsets.UTF_8))); // 1*8
    expect.put(new Key("A2", "", "A2"), new Value("7".getBytes(StandardCharsets.UTF_8))); // 1*7
    expect.put(new Key("C1", "", "A1"), new Value("1".getBytes(StandardCharsets.UTF_8))); // A
    expect.put(new Key("C2", "", "A1"), new Value("1".getBytes(StandardCharsets.UTF_8))); // A
    expect.put(new Key("C1", "", "A2"), new Value("1".getBytes(StandardCharsets.UTF_8))); // A
    SortedMap<Key, Value> expectT = GraphuloUtil.transposeMap(expect);

    IteratorSetting add3 =
        MathTwoScalar.applyOpLong(1, true, MathTwoScalar.ScalarOp.PLUS, 3, false);
    GraphuloUtil.applyIteratorSoft(add3, conn.tableOperations(), tB);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, tCT, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.BIGDECIMAL, "", false),
        Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        null, null, null, false, false,
        true, false, Collections.singletonList(ConstantTwoScalar.iteratorSetting(1, new Value("1".getBytes(StandardCharsets.UTF_8)))),
        null, null, null, null, 1, null, null);

    Assert.assertEquals(8, numpp);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
//      DebugUtil.printMapFull(actual.entrySet().iterator(), 5);
      Assert.assertEquals(expect, actual);
    }

    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actualT = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actualT.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectT, actualT);
    }

    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
    conn.tableOperations().delete(tCT);
  }

  /**
   * <pre>
   *      C1 C2
   * A1 [ 5  2 ]
   * A2 [ 4    ]
   * </pre>
   */
  @Test
  public void testCloneMultiplyAndEmitAAndUseIterators3() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tC, tAT, tB, tCT;
    {
      String[] names = getUniqueNames(3);
      tAT = TwoTableIterator.CLONESOURCE_TABLENAME;
      tB = names[0];
      tC = names[1];
      tCT = names[2];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes(StandardCharsets.UTF_8)));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tB, null, input);
    }
    SortedMap<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("A1", "", "A1"), new Value("13".getBytes(StandardCharsets.UTF_8))); // 1*8 + 1*5
    expect.put(new Key("A1", "", "A2"), new Value("7".getBytes(StandardCharsets.UTF_8))); // 1*7
    expect.put(new Key("A2", "", "A1"), new Value("8".getBytes(StandardCharsets.UTF_8))); // 1*8
    expect.put(new Key("A2", "", "A2"), new Value("7".getBytes(StandardCharsets.UTF_8))); // 1*7
    expect.put(new Key("C1", "", "A1"), new Value("1".getBytes(StandardCharsets.UTF_8))); // A
    expect.put(new Key("C2", "", "A1"), new Value("1".getBytes(StandardCharsets.UTF_8))); // A
    expect.put(new Key("C1", "", "A2"), new Value("1".getBytes(StandardCharsets.UTF_8))); // A
    SortedMap<Key, Value> expectT = GraphuloUtil.transposeMap(expect);

    IteratorSetting add3 =
        new DynamicIteratorSetting(1, null)
        .append(MathTwoScalar.applyOpLong(1, true, MathTwoScalar.ScalarOp.PLUS, 3, false))
        .toIteratorSetting();
    GraphuloUtil.applyIteratorSoft(add3, conn.tableOperations(), tB);

    IteratorSetting only1 =
        new DynamicIteratorSetting(1, null)
            .append(ConstantTwoScalar.iteratorSetting(1, new Value("1".getBytes(StandardCharsets.UTF_8))))
            .toIteratorSetting();

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, tCT, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.BIGDECIMAL, "", false),
        Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        null, null, null, false, false,
        true, false, Collections.singletonList(only1),
        null, null, null, null, 1, null, null);

    Assert.assertEquals(8, numpp);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
//      DebugUtil.printMapFull(actual.entrySet().iterator(), 5);
      Assert.assertEquals(expect, actual);
    }

    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actualT = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actualT.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectT, actualT);
    }

    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
    conn.tableOperations().delete(tCT);
  }

}
