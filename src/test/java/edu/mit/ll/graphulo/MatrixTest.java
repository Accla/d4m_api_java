package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.MemMatrixUtil;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Test MemMatrixUtil
 */
public class MatrixTest  {
  private static final Logger log = LoggerFactory.getLogger(MatrixTest.class);

  @Test
  public void testInverseIdentity() {
    double tol = 0.00001;
    Map<Key, Value> input = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    input.put(new Key("1", "", "1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
//    input.put(new Key("1", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
//    input.put(new Key("2", "", "1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
    input.put(new Key("2", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));

    RealMatrix matrix = MemMatrixUtil.buildMatrix(input.entrySet().iterator(), 2);
    Assert.assertEquals(2, matrix.getRowDimension());
    Assert.assertEquals(2, matrix.getColumnDimension());
    Assert.assertEquals(1, matrix.getEntry(0, 0), tol);
    Assert.assertEquals(0, matrix.getEntry(0, 1), tol);
    Assert.assertEquals(0, matrix.getEntry(1, 0), tol);
    Assert.assertEquals(1, matrix.getEntry(1, 1), tol);

    matrix = MemMatrixUtil.doInverse(matrix, -1);
    Assert.assertEquals(2, matrix.getRowDimension());
    Assert.assertEquals(2, matrix.getColumnDimension());
    Assert.assertEquals(1, matrix.getEntry(0, 0), tol);
    Assert.assertEquals(0, matrix.getEntry(0, 1), tol);
    Assert.assertEquals(0, matrix.getEntry(1, 0), tol);
    Assert.assertEquals(1, matrix.getEntry(1, 1), tol);

    SortedMap<Key, Value> back = MemMatrixUtil.matrixToMap(new TreeMap<Key, Value>(TestUtil.COMPARE_KEY_TO_COLQ), matrix);
    TestUtil.assertEqualDoubleMap(input, back);
//    Assert.assertEquals(1, Double.parseDouble(new String(back.get(new Key("1", "", "1")).get())), tol);
//    Assert.assertEquals(1, Double.parseDouble(new String(back.get(new Key("2", "", "2")).get())), tol);
  }

  @Test
  public void testInverse2x2() {
    double tol = 0.001;
    Map<Key, Value> input = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    input.put(new Key("1", "", "1"), new Value("4".getBytes(StandardCharsets.UTF_8)));
    input.put(new Key("1", "", "2"), new Value("3".getBytes(StandardCharsets.UTF_8)));
    input.put(new Key("2", "", "1"), new Value("1".getBytes(StandardCharsets.UTF_8)));
    input.put(new Key("2", "", "2"), new Value("1".getBytes(StandardCharsets.UTF_8)));
    Map<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("1", "", "1"), new Value("1 ".getBytes(StandardCharsets.UTF_8)));
    expect.put(new Key("1", "", "2"), new Value("-3".getBytes(StandardCharsets.UTF_8)));
    expect.put(new Key("2", "", "1"), new Value("-1".getBytes(StandardCharsets.UTF_8)));
    expect.put(new Key("2", "", "2"), new Value("4 ".getBytes(StandardCharsets.UTF_8)));

    RealMatrix matrix = MemMatrixUtil.buildMatrix(input.entrySet().iterator(), 2);
    Assert.assertEquals(2, matrix.getRowDimension());
    Assert.assertEquals(2, matrix.getColumnDimension());
    Assert.assertEquals(4, matrix.getEntry(0, 0), tol);
    Assert.assertEquals(3, matrix.getEntry(0, 1), tol);
    Assert.assertEquals(1, matrix.getEntry(1, 0), tol);
    Assert.assertEquals(1, matrix.getEntry(1, 1), tol);

    matrix = MemMatrixUtil.doInverse(matrix, -1);
    Assert.assertEquals(2, matrix.getRowDimension());
    Assert.assertEquals(2, matrix.getColumnDimension());
    Assert.assertEquals(1 , matrix.getEntry(0, 0), tol);
    Assert.assertEquals(-3, matrix.getEntry(0, 1), tol);
    Assert.assertEquals(-1, matrix.getEntry(1, 0), tol);
    Assert.assertEquals(4, matrix.getEntry(1, 1), tol);

    SortedMap<Key, Value> back = MemMatrixUtil.matrixToMap(new TreeMap<Key, Value>(TestUtil.COMPARE_KEY_TO_COLQ), matrix);
    TestUtil.assertEqualDoubleMap(expect, back);
  }

  @Test
  public void testSubtractMatrixNorm() {
    RealMatrix m, mp;
    m = MatrixUtils.createRealMatrix(3,5);
    mp = MatrixUtils.createRealMatrix(3,5);
    m.setEntry(1, 2, 0.1613351);
    mp.setEntry(1, 2, 0.486433333333);
    Assert.assertEquals(0.0, Graphulo.nmfError_Client(m, mp), 0.000001);
    m.setEntry(2, 2, 0.76);
    mp.setEntry(2, 2, 0.25);
    Assert.assertEquals(0.0, Graphulo.nmfError_Client(m, mp), 0.000001);
    m.setEntry(2, 1, 0.4);
    mp.setEntry(2,0,0.9);
    Assert.assertEquals(2.0/3.0, Graphulo.nmfError_Client(m, mp), 0.000001);
  }

}
