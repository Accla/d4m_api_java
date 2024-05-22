package edu.mit.ll.graphulo.rowmult;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.skvi.Watch;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.SKVIRowIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;


public class CartesianRowMultiply implements RowMultiplyOp {
  private static final Logger log = LoggerFactory.getLogger(CartesianRowMultiply.class);

  /**
   * Fill a SortedMap with all the entries in the same row as skvi.getTopKey().getRow()
   * when first called.
   * Postcondition: !skvi.hasTop() || skvi.getTopKey().getRow() has changed.
   *
   * @return Sorted map of the entries.
   * Todo P2: replace SortedMap with a list of entries, since sorted order is guaranteed
   */
  public static SortedMap<Key, Value> readRow(SortedKeyValueIterator<Key, Value> skvi,
                                       Watch<Watch.PerfSpan> watch, Watch.PerfSpan watchtype) throws IOException {
    if (!skvi.hasTop())
      throw new IllegalStateException(skvi + " should hasTop()");
    Text thisRow = skvi.getTopKey().getRow();
    Text curRow = new Text(thisRow);
    SortedMap<Key, Value> map = new TreeMap<>();
    do {
      map.put(new Key(skvi.getTopKey()), new Value(skvi.getTopValue()));
//      watch.start(watchtype);
//      try {
        skvi.next();
//      } finally {
//        watch.stop(watchtype);
//      }
    } while (skvi.hasTop() && skvi.getTopKey().getRow(curRow).equals(thisRow));
    return map;
  }


  /**
   * Fill a SortedMap with all the entries in the same row as skvi.getTopKey().getRow()
   * when first called.
   * Postcondition: !skvi.hasTop() || skvi.getTopKey().getRow() has changed.
   *
   * Similar to {@link #readRow(SortedKeyValueIterator, Watch, Watch.PerfSpan)},
   * but only puts the column qualifier into the map.
   */
  public static SortedMap<Text, Value> readRowColumns(SortedKeyValueIterator<Key, Value> skvi,
                                              Watch<Watch.PerfSpan> watch, Watch.PerfSpan watchtype) throws IOException {
    if (!skvi.hasTop())
      throw new IllegalStateException(skvi + " should hasTop()");
    Text thisRow = skvi.getTopKey().getRow();
    Text curRow = new Text(thisRow);
    SortedMap<Text, Value> map = new TreeMap<>();
    do {
      map.put(skvi.getTopKey().getColumnQualifier(), new Value(skvi.getTopValue()));
//      watch.start(watchtype);
//      try {
      skvi.next();
//      } finally {
//        watch.stop(watchtype);
//      }
    } while (skvi.hasTop() && skvi.getTopKey().getRow(curRow).equals(thisRow));
    return map;
  }

  public static SortedSet<Text> readRowColumnsNoValues(SortedKeyValueIterator<Key, Value> skvi) throws IOException {
    if (!skvi.hasTop())
      throw new IllegalStateException(skvi + " should hasTop()");
    final Text thisRow = skvi.getTopKey().getRow();
    Text curRow = new Text(thisRow);
    SortedSet<Text> map = new TreeSet<>();
    do {
      map.add(skvi.getTopKey().getColumnQualifier());
//      watch.start(watchtype);
//      try {
      skvi.next();
//      } finally {
//        watch.stop(watchtype);
//      }
    } while (skvi.hasTop() && skvi.getTopKey().getRow(curRow).equals(thisRow));
    return map;
  }


  public enum ROWMODE {
    /** Read both rows into memory. */
    TWOROW,
    /** Read a row from A into memory and stream/iterate through columns in the row from B. */
    ONEROWA,
    /** Read a row from B into memory and stream/iterate through columns in the row from A. */
    ONEROWB
  }

  public static final String ALSODOAA="alsoDoAA", ALSODOBB="alsoDoBB",
      ALSOEMITA="alsoEmitA", ALSOEMITB="alsoEmitB";

  private MultiplyOp multiplyOp;
  private Map<String, String> multiplyOpOptions = new HashMap<>();
  private ROWMODE rowmode = ROWMODE.ONEROWB;
  private boolean isRowStartMultiplyOp = false;
  private boolean alsoDoAA = false, alsoDoBB = false,
      alsoEmitA = false, alsoEmitB = false;


  private void parseOptions(Map<String, String> options) {
    log.debug("rowMultiplyOp options: "+options);
    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
      if (optionKey.startsWith("multiplyOp.opt.")) {
        String keyAfterPrefix = optionKey.substring("multiplyOp.opt.".length());
        multiplyOpOptions.put(keyAfterPrefix, optionValue);
      } else {
        switch (optionKey) {
          case "rowmode":
            rowmode = ROWMODE.valueOf(optionValue);
            break;
          case "multiplyOp":
            multiplyOp = GraphuloUtil.subclassNewInstance(optionValue, MultiplyOp.class);
            isRowStartMultiplyOp = multiplyOp instanceof RowStartMultiplyOp;
            break;
          case ALSODOBB:
            alsoDoBB = Boolean.parseBoolean(optionValue);
            break;
          case ALSODOAA:
            alsoDoAA = Boolean.parseBoolean(optionValue);
            break;
          case ALSOEMITA:
            alsoEmitA = Boolean.parseBoolean(optionValue);
            break;
          case ALSOEMITB:
            alsoEmitB = Boolean.parseBoolean(optionValue);
            break;
          default:
            log.warn("Unrecognized option: " + optionEntry);
            break;
        }
      }
    }
    if (multiplyOp == null) {
      multiplyOp = new MathTwoScalar(); // default
      multiplyOpOptions.putAll(MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.BIGDECIMAL, "", false));
    }
    if ((alsoDoAA || alsoEmitA) && (alsoEmitB || alsoDoBB) && rowmode != ROWMODE.TWOROW) {
      log.warn("Because alsoEmitA/alsoDoAA and alsoEmitB/alsoDoBB are true, forcing rowmode to " + ROWMODE.TWOROW);
      rowmode = ROWMODE.TWOROW;
    } else if ((alsoDoAA || alsoEmitA) && (rowmode == ROWMODE.ONEROWB)) {
      log.warn("Because alsoEmitA/alsoDoAA is true, forcing rowmode from "+ROWMODE.ONEROWB +" to " + ROWMODE.TWOROW);
      rowmode = ROWMODE.TWOROW;
    } else if ((alsoDoBB || alsoEmitB) && (rowmode == ROWMODE.ONEROWA)) {
      log.warn("Because alsoEmitB/alsoDoBB is true, forcing rowmode from "+ROWMODE.ONEROWA+" to " + ROWMODE.TWOROW);
      rowmode = ROWMODE.TWOROW;
    }
  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    parseOptions(options);
    multiplyOp.init(multiplyOpOptions,env);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<Map.Entry<Key,Value>> multiplyRow(SortedKeyValueIterator<Key, Value> skviA, SortedKeyValueIterator<Key, Value> skviB) throws IOException {
    assert skviA != null || skviB != null;
    Watch<Watch.PerfSpan> watch = null;//Watch.getInstance();

    if (skviB == null) { // have row A, no matching row B
      if (alsoEmitA && !alsoDoAA) {
        return new SKVIRowIterator(skviA);
      } else if (!alsoEmitA && alsoDoAA) {
        SortedMap<Key, Value> ArowMap = readRow(skviA, watch, Watch.PerfSpan.ATnext);
        if (isRowStartMultiplyOp)
          if (!((RowStartMultiplyOp)multiplyOp).startRow(ArowMap, null, false))
            return Collections.emptyIterator();
        return new CartesianIterator(ArowMap.entrySet().iterator(), ArowMap, multiplyOp, false);
      } else if (alsoEmitA && alsoDoAA) {
        SortedMap<Key, Value> ArowMap = readRow(skviA, watch, Watch.PerfSpan.ATnext);
        if (isRowStartMultiplyOp)
          if (!((RowStartMultiplyOp)multiplyOp).startRow(ArowMap, null, false))
            return Collections.emptyIterator();
        return Iterators.concat(ArowMap.entrySet().iterator(),
            new CartesianIterator(ArowMap.entrySet().iterator(), ArowMap, multiplyOp, false));
      } else {
        SKVIRowIterator.dumpRow(skviA);
        return Collections.emptyIterator();
      }
    }

    if (skviA == null) {
      if (alsoEmitB && !alsoDoBB) {
        return new SKVIRowIterator(skviB);
      } else if (!alsoEmitB && alsoDoBB) {
        SortedMap<Key, Value> BrowMap = readRow(skviB, watch, Watch.PerfSpan.Bnext);
        if (isRowStartMultiplyOp)
          if (!((RowStartMultiplyOp) multiplyOp).startRow(BrowMap, null, false))
            return Collections.emptyIterator();
        return new CartesianIterator(BrowMap.entrySet().iterator(), BrowMap, multiplyOp, false);
      } else if (alsoEmitB && alsoDoBB) {
        SortedMap<Key, Value> BrowMap = readRow(skviB, watch, Watch.PerfSpan.Bnext);
        if (isRowStartMultiplyOp)
          if (!((RowStartMultiplyOp) multiplyOp).startRow(BrowMap, null, false))
            return Collections.emptyIterator();
        return Iterators.concat(BrowMap.entrySet().iterator(),
            new CartesianIterator(BrowMap.entrySet().iterator(), BrowMap, multiplyOp, false));
      } else {
        SKVIRowIterator.dumpRow(skviB);
        return Collections.emptyIterator();
      }
    }

    assert skviA.hasTop() && skviB.hasTop() && skviA.getTopKey().equals(skviB.getTopKey(), PartialKey.ROW);
    
    switch (rowmode) {
      case TWOROW: {
        SortedMap<Key, Value> ArowMap = readRow(skviA, watch, Watch.PerfSpan.ATnext);
        SortedMap<Key, Value> BrowMap = readRow(skviB, watch, Watch.PerfSpan.Bnext);
        if (isRowStartMultiplyOp)
          if (!((RowStartMultiplyOp)multiplyOp).startRow(ArowMap, BrowMap, true))
            return Collections.emptyIterator();

        List<Iterator<Map.Entry<Key,Value>>> all = new LinkedList<>();
        if (alsoEmitA)
          all.add(ArowMap.entrySet().iterator());
        if (alsoEmitB)
          all.add(BrowMap.entrySet().iterator());
        if (alsoDoAA)
          all.add(new CartesianIterator(ArowMap.entrySet().iterator(), ArowMap, multiplyOp, false));
        if (alsoDoBB)
          all.add(new CartesianIterator(BrowMap.entrySet().iterator(), BrowMap, multiplyOp, false));
        all.add(new CartesianIterator(ArowMap.entrySet().iterator(), BrowMap, multiplyOp, false));
        return Iterators.concat(all.iterator());
      }

      case ONEROWA: {
        assert !alsoDoBB && !alsoEmitB;
        SortedMap<Key, Value> ArowMap = readRow(skviA, watch, Watch.PerfSpan.ATnext);
        Iterator<Map.Entry<Key, Value>> itBonce = new SKVIRowIterator(skviB);
        if (isRowStartMultiplyOp)
          if (!((RowStartMultiplyOp)multiplyOp).startRow(ArowMap, null, true)) {
            // skip rest of row at skviB
            ArowMap = null;
            while (itBonce.hasNext())
              itBonce.next();
            return Collections.emptyIterator();
          }

        List<Iterator<Map.Entry<Key,Value>>> all = new LinkedList<>();
        if (alsoEmitA)
          all.add(ArowMap.entrySet().iterator());
        if (alsoDoAA)
          all.add(new CartesianIterator(ArowMap.entrySet().iterator(), ArowMap, multiplyOp, false));
        all.add(new CartesianIterator(itBonce, ArowMap, multiplyOp, true));
        return Iterators.concat(all.iterator());
      }

      case ONEROWB: {
        assert !alsoDoAA;
        Iterator<Map.Entry<Key, Value>> itAonce = new SKVIRowIterator(skviA);
        SortedMap<Key, Value> BrowMap = readRow(skviB, watch, Watch.PerfSpan.Bnext);
        if (isRowStartMultiplyOp)
          if (!((RowStartMultiplyOp)multiplyOp).startRow(null, BrowMap, true)) {
            // skip rest of row at skviA
            BrowMap = null;
            while (itAonce.hasNext())
              itAonce.next();
            return Collections.emptyIterator();
          }

        List<Iterator<Map.Entry<Key,Value>>> all = new LinkedList<>();
        if (alsoEmitB)
          all.add(BrowMap.entrySet().iterator());
        if (alsoDoBB)
          all.add(new CartesianIterator(BrowMap.entrySet().iterator(), BrowMap, multiplyOp, false));
        all.add(new CartesianIterator(itAonce, BrowMap, multiplyOp, false));
        return Iterators.concat(all.iterator());
      }

      default:
        throw new AssertionError("unknown rowmode: "+rowmode);
    }
    
  }

}