package edu.mit.ll.graphulo.util;

import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.MatrixEntry;
import no.uib.cipr.matrix.UpperSymmDenseMatrix;
import no.uib.cipr.matrix.sparse.LinkedSparseMatrix;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_TEXT;

/**
 * For working with the MTJ matrix library.
 * Used in client-side version of kTrussAdj.
 */
public class MTJUtil {
  private static final Logger log = LoggerFactory.getLogger(MTJUtil.class);

  private MTJUtil() {
  }

  /** Replace row and col labels with integer indexes; create map from indexes to original labels */
  public static Matrix indexMapAndMatrix(final Map<Key,Value> orig,
                                         final SortedMap<Integer,String> rowMap, final SortedMap<Integer,String> colMap,
                                         final double zeroTolerance) {
    // use Map<Integer,Map<Integer,Double>> to build
    SortedMap<Integer,SortedMap<Integer,Double>> rowcolvalmap = new TreeMap<>();
    Map<String,Integer> rowMapRev = new HashMap<>(), colMapRev = new HashMap<>();

    Text rowText = new Text(), colText = new Text();
    for (Iterator<Map.Entry<Key, Value>> iterator = orig.entrySet().iterator(); iterator.hasNext(); ) {
      Map.Entry<Key, Value> entry = iterator.next();
      double val = Double.valueOf(new String(entry.getValue().get(), StandardCharsets.UTF_8));
      if (val >= -zeroTolerance && val <= zeroTolerance) {
        iterator.remove();
        continue;
      }
      Key k = entry.getKey();
      String row = k.getRow(rowText).toString(), col = k.getColumnQualifier(colText).toString();
      Integer rowInt = rowMapRev.get(row);
      SortedMap<Integer, Double> colvalmap;
      if (rowInt == null) {
        rowInt = rowMapRev.size() + 1;
        rowMapRev.put(row, rowInt);
        rowMap.put(rowInt, row);
        colvalmap = new TreeMap<>();
        rowcolvalmap.put(rowInt, colvalmap);
      } else
        colvalmap = rowcolvalmap.get(rowInt);

      Integer colInt = colMapRev.get(col);
      if (colInt == null) {
        colInt = colMapRev.size() + 1;
        colMapRev.put(col, colInt);
        colMap.put(colInt, col);
      }
      colvalmap.put(colInt, val);
    }

    // build matrix
    int N = rowMap.size();
    int M = colMap.size();
    Matrix m = new LinkedSparseMatrix(N, M);

    for (Map.Entry<Integer, SortedMap<Integer, Double>> rcentry : rowcolvalmap.entrySet()) {
      int rowInt = rcentry.getKey();
      for (Map.Entry<Integer, Double> centry : rcentry.getValue().entrySet()) {
        int colInt = centry.getKey();
        double val = centry.getValue();
        m.set(rowInt-1, colInt-1, val);
      }
    }
    return m;
  }

  /** Replace row and col labels with integer indexes; create map from indexes to original labels */
  public static Matrix indexMapAndMatrix_SameRowCol(final Map<Key, Value> orig,
                                                    final SortedMap<Integer, String> rowColMap,
                                                    final double zeroTolerance, boolean useSparse, boolean upperOnly) {
    // use Map<Integer,Map<Integer,Double>> to build
    SortedMap<Integer,SortedMap<Integer,Double>> rowcolvalmap = new TreeMap<>();
    Map<String,Integer> rowColMapRev = new HashMap<>();
    Text rowText = new Text(), colText = new Text();

    // first collect unique rows and columns
    SortedSet<String> rowColSet = new TreeSet<>();
    for (Iterator<Map.Entry<Key, Value>> iterator = orig.entrySet().iterator(); iterator.hasNext(); ) {
      Map.Entry<Key, Value> entry = iterator.next();
      double val = Double.valueOf(new String(entry.getValue().get(), StandardCharsets.UTF_8));
      if (val >= -zeroTolerance && val <= zeroTolerance) {
        iterator.remove();
        continue;
      }
      Key k = entry.getKey();
      String row = k.getRow(rowText).toString(), col = k.getColumnQualifier(colText).toString();
      rowColSet.add(row);
      rowColSet.add(col);
    }

    // iterate in sorted order
    int i = 1;
    for (String s : rowColSet) {
      rowColMap.put(i, s);
      rowColMapRev.put(s, i);
      i++;
    }
    i--; // i is now the dimension N

//    int[][] nzCols = new int[i][];


    Matrix m = useSparse ? new LinkedSparseMatrix(i, i) : new DenseMatrix(i,i); //(upperOnly ? new UpperSymmDenseMatrix(i) : new DenseMatrix(i,i));

    for (Map.Entry<Key, Value> entry : orig.entrySet()) {
      Key k = entry.getKey();
      String row = k.getRow(rowText).toString(), col = k.getColumnQualifier(colText).toString();
      Integer rowInt = rowColMapRev.get(row);
      Integer colInt = rowColMapRev.get(col);
      assert rowInt != null && colInt != null;

      if (!useSparse && upperOnly && rowInt > colInt) // let diagonal through
        continue;
      m.set(rowInt-1, colInt-1, Double.valueOf(new String(entry.getValue().get(), StandardCharsets.UTF_8)));
    }
    return m;
  }

  public static Map<Key, Value> matrixToMapWithLabels(Matrix orig,
                                                      final SortedMap<Integer, String> rowLabelMap,
                                                      final SortedMap<Integer, String> colLabelMap,
                                                      final double zeroTolerance,
                                                      String RNewVisibility,
                                                      boolean coerceToLong, boolean onlyUpper) {
    final Map<Key,Value> ret = new TreeMap<>();
    if (RNewVisibility == null) RNewVisibility = "";
    Text cv = new Text(RNewVisibility);

    for (MatrixEntry matrixEntry : orig) {
      int row = matrixEntry.row();
      int column = matrixEntry.column();
      if (onlyUpper && row >= column)
        continue;
      double value = matrixEntry.get();
      if (value >= -zeroTolerance && value <= zeroTolerance)
        continue;
      row++; column++;
      Text rowText, cqText;

      rowText = new Text(rowLabelMap.get(row));
      cqText = new Text(colLabelMap.get(column));

      Key k = new Key(rowText, EMPTY_TEXT, cqText, cv);
      String s = coerceToLong ? Long.toString((long)value) : Double.toString(value);
      Value v = new Value(s.getBytes(StandardCharsets.UTF_8));
      ret.put(k,v);
    }

    return ret;
  }

}
