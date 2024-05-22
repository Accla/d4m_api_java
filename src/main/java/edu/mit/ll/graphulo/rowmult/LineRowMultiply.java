package edu.mit.ll.graphulo.rowmult;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.skvi.Watch;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.SKVIRowIterator;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.hadoop.io.WritableComparator;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * Expects AT as Atable, A as Btable.
 */
public class LineRowMultiply implements RowMultiplyOp {
  private static final Logger log = LoggerFactory.getLogger(LineRowMultiply.class);

  public static final String SEPARATOR = "separator",
      ISDIRECTED = "isDirected",
      INCLUDE_EXTRA_CYCLES = "includeExtraCycles",
      NEW_VISIBILITY = "newVisibility";
  public static final String USE_NEW_VISIBILITY = "useNewVisibility";

  private boolean isDirected = true;
  /** Whether to include the AAT term. */
  private boolean includeExtraCycles = false;
//  private char separator = '|';
  private MultiplyOp multiplyOpAA, multiplyOpAAT;
  private boolean useNewVisibility = false;
  private byte[] newVisibility = null;

  private void parseOptions(Map<String, String> options) {
    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
        switch (optionKey) {
          case ISDIRECTED:
            isDirected = Boolean.parseBoolean(optionValue);
            break;
          case SEPARATOR:
//            if (optionValue.length() != 1)
//              throw new IllegalArgumentException("bad "+ "separator" +": "+optionValue);
            separator = optionValue.getBytes(StandardCharsets.UTF_8);
            break;
          case INCLUDE_EXTRA_CYCLES:
            includeExtraCycles = Boolean.parseBoolean(optionValue);
            break;
          case USE_NEW_VISIBILITY:
            useNewVisibility = Boolean.parseBoolean(optionValue);
            break;
          case NEW_VISIBILITY:
            newVisibility = optionValue.getBytes(StandardCharsets.UTF_8);
            break;
          default:
            log.warn("Unrecognized option: " + optionEntry);
            break;
        }
    }
  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    parseOptions(options);
    if (!isDirected)
      multiplyOpAA = this.new LineMultiply(LINEMODE.UNDIR);
    else {
      multiplyOpAA = this.new LineMultiply(LINEMODE.DIRAA);
      if (includeExtraCycles)
        multiplyOpAAT = this.new LineMultiply(LINEMODE.DIRAAT);
    }
  }

  @Override
  public Iterator<Map.Entry<Key, Value>> multiplyRow(SortedKeyValueIterator<Key, Value> skviAT, SortedKeyValueIterator<Key, Value> skviA) throws IOException {
    Watch<Watch.PerfSpan> watch = null; //Watch.getInstance();

    if (isDirected) { // TWOROW
      SortedMap<Key, Value> mapAT = CartesianRowMultiply.readRow(skviAT, watch, Watch.PerfSpan.ATnext);
      SortedMap<Key, Value> mapA = CartesianRowMultiply.readRow(skviA, watch, Watch.PerfSpan.Bnext);
      if (!startRow(mapAT, mapA)) {
        return Collections.emptyIterator();
      }
      Iterator<Map.Entry<Key, Value>> it1 = new CartesianIterator(mapAT.entrySet().iterator(), mapA, multiplyOpAA, false);
      if (!includeExtraCycles)
        return it1;
      else {
        Iterator<Map.Entry<Key, Value>> it2 = new CartesianIterator(mapAT.entrySet().iterator(), mapAT, multiplyOpAAT, false);
        return Iterators.concat(it1, it2);
      }
    } else {
      SortedMap<Key, Value> mapAT = CartesianRowMultiply.readRow(skviAT, watch, Watch.PerfSpan.ATnext);
      Iterator<Map.Entry<Key, Value>> itA = new SKVIRowIterator(skviA);
      if (!startRow(mapAT, null)) {
        mapAT = null;
        while (itA.hasNext())
          itA.next();
        return Collections.emptyIterator();
      }
      return new CartesianIterator(itA, mapAT, multiplyOpAA, true);
    }
  }




  private boolean startRow(SortedMap<Key, Value> mapAT, SortedMap<Key, Value> mapA) {
//    if (mapA == null && mapAT == null) {
//      assert linemode == LINEMODE.DIRAA;
//      linemode = LINEMODE.DIRAAT;
//      return true;
//    } else if (linemode == LINEMODE.DIRAAT)
//      linemode = LINEMODE.DIRAA;

    assert mapAT != null && (!isDirected || mapA != null);
    din = mapAT.size();      // undirected: always pass mapAT
    //noinspection ConstantConditions
    dout = !isDirected ? din : mapA.size();
    assert din >= 1 && dout >= 1;
    win = sumValues(mapAT);
    if (Double.doubleToRawLongBits(win) == 0)
      return false; // case where weights sum to zero; very rare.

    nume = !isDirected ? din * (din-1) / 2 : // undirected: (din choose 2)
        (!includeExtraCycles ? din * dout :  // directed without AAT term: din*dout
            din * dout + din * (din-1));     // directed with AAT term: din*dout + 2*(din choose 2)
    if (Double.doubleToRawLongBits(nume) == 0)
      return false; // no edges emit
    winPerEdge = !isDirected ? win / nume / 2 : win / nume;
    winPerEdgeValue = new Value(Double.toString(winPerEdge).getBytes(StandardCharsets.UTF_8));
    return true;
  }

  private static final TypedValueCombiner.Encoder<BigDecimal> DECIMAL_ENCODER = new BigDecimalCombiner.BigDecimalEncoder();
  private static double sumValues(SortedMap<Key,Value> map) {
    BigDecimal d = new BigDecimal(0);
    for (Value value : map.values()) {
      d = d.add(DECIMAL_ENCODER.decode(value.get()));
    }
    return d.doubleValue();
  }

  enum CATMODE { ROWCOL, COLROW, LEXICO }
  enum LINEMODE { UNDIR, DIRAA, DIRAAT }

  private byte[] doCat(ByteSequence mrow, ByteSequence col, CATMODE catmode) {
    switch (catmode) {
      case LEXICO:
        int cmp = WritableComparator.compareBytes(mrow.toArray(), mrow.offset(), mrow.length(),
            col.toArray(), col.offset(), col.length());
        return cmp < 0 ? doCat2(mrow, col) : doCat2(col, mrow);
      case ROWCOL:
        return doCat2(mrow, col);
      case COLROW:
        return doCat2(col, mrow);
    }
    throw new AssertionError("unknown catmode: "+catmode);
  }

  private byte[] doCat2(ByteSequence b1, ByteSequence b2) {
    byte[] ret = new byte[b1.length()+separator.length+b2.length()];
    System.arraycopy(b1.toArray(), b1.offset(), ret, 0, b1.length());
    System.arraycopy(separator,0,ret,b1.length(),separator.length);
    System.arraycopy(b2.toArray(), b2.offset(), ret, b1.length() + separator.length, b2.length());
    return ret;
  }

  private byte[] separator = "|".getBytes(StandardCharsets.UTF_8);
  double din, dout, win, nume, winPerEdge;
  Value winPerEdgeValue;

  class LineMultiply implements MultiplyOp {
    private LINEMODE linemode;

    public LineMultiply(LINEMODE linemode) {
      this.linemode = linemode;
    }

    @Override
    public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    }

    /** For {@link MultiplyOp}. Uses {@link System#currentTimeMillis()} for the timestamp. */
    @Override
    public Iterator<? extends Map.Entry<Key, Value>> multiply(
        ByteSequence Mrow,
        ByteSequence ATcolF, ByteSequence ATcolQ, ByteSequence ATcolVis, long ATtime,
        ByteSequence BcolF, ByteSequence BcolQ, ByteSequence BcolVis, long Btime,
        Value ATval, Value Bval) {
      if ((linemode == LINEMODE.UNDIR || linemode == LINEMODE.DIRAAT) &&
          Arrays.equals(BcolQ.toArray(), ATcolQ.toArray()))
        return Collections.emptyIterator(); // no diagonal
      Key k;
      CATMODE newrow=null, newcol=null;
      switch (linemode) {
        case UNDIR:
          newrow = CATMODE.LEXICO;
          newcol = CATMODE.LEXICO;
          break;
        case DIRAA:
          newrow = CATMODE.COLROW;
          newcol = CATMODE.ROWCOL;
          break;
        case DIRAAT:
          newrow = CATMODE.COLROW;
          newcol = CATMODE.COLROW;
          break;
      }
      k = new Key(doCat(Mrow, ATcolQ, newrow),
          ATcolF.toArray(),
          doCat(Mrow, BcolQ, newcol),
          useNewVisibility ? newVisibility : GraphuloUtil.EMPTY_BYTES);
      // reuse object instead of new one each time?
      return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, winPerEdgeValue));
    }
  }
}
