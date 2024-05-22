package edu.mit.ll.graphulo.rowmult;

import edu.mit.ll.graphulo.util.PeekingIterator1;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * Emits Cartesian product of provided iterators, passing each pair of entries to multiply function.
 * Default is to stream through A once and iterate through B many times.
 * Pass <tt>switched</tt> as true if the two are switched.
 */
public class CartesianIterator implements Iterator<Map.Entry<Key, Value>> {
  private static final Logger log = LoggerFactory.getLogger(CartesianIterator.class);

  public interface MCondition {
    boolean shouldMultiply(Map.Entry<Key, Value> eA, Map.Entry<Key, Value> eB);
  }

  private final SortedMap<Key, Value> BrowMap;
  private final boolean switched;
  private final PeekingIterator1<Map.Entry<Key, Value>> itAonce;
  private Iterator<Map.Entry<Key, Value>> itBreset;
  private final MultiplyOp multiplyOp;
  private Iterator<? extends Map.Entry<Key, Value>> multiplyOpIterator;
  private MCondition mcondition;

  public CartesianIterator(Iterator<Map.Entry<Key, Value>> itAonce, SortedMap<Key, Value> mapBreset,
                           MultiplyOp multiplyOp, boolean switched) {
    this(itAonce, mapBreset, multiplyOp, switched, null);
  }

  public CartesianIterator(Iterator<Map.Entry<Key, Value>> itAonce, SortedMap<Key, Value> mapBreset,
                           MultiplyOp multiplyOp, boolean switched, MCondition mcondition) {
    BrowMap = mapBreset;
    this.switched = switched;
    this.itAonce = new PeekingIterator1<>(itAonce);
    this.itBreset = BrowMap.entrySet().iterator();
    this.multiplyOp = multiplyOp;
    this.mcondition = mcondition;
    if (itBreset.hasNext())
      prepNext();
    else
      multiplyOpIterator = Collections.emptyIterator();
  }

  @Override
  public boolean hasNext() {
    return multiplyOpIterator.hasNext();
  }

  @Override
  public Map.Entry<Key, Value> next() {
    Map.Entry<Key, Value> ret = multiplyOpIterator.next();
    if (!multiplyOpIterator.hasNext() && itBreset.hasNext())
      prepNext();
    return ret;
  }

  private void prepNext() {
    do {
      Map.Entry<Key, Value> eA, eB = itBreset.next();
      if (!itBreset.hasNext()) {
        eA = itAonce.next();    // advance itA
        if (itAonce.hasNext())  // STOP if no more itA
          itBreset = BrowMap.entrySet().iterator();
      } else
        eA = itAonce.peek();
      if (mcondition == null || (switched ? mcondition.shouldMultiply(eB, eA) : mcondition.shouldMultiply(eA, eB)))
        multiplyOpIterator = switched ? multiplyEntry(eB, eA) : multiplyEntry(eA, eB);
    } while (!multiplyOpIterator.hasNext() && itBreset.hasNext());
  }

  private Iterator<? extends Map.Entry<Key, Value>> multiplyEntry(Map.Entry<Key, Value> e1, Map.Entry<Key, Value> e2) {
    assert e1.getKey().getRowData().compareTo(e2.getKey().getRowData()) == 0;
    Key k1 = e1.getKey(), k2 = e2.getKey();
    return multiplyOp.multiply(k1.getRowData(), k1.getColumnFamilyData(), k1.getColumnQualifierData(),
        k1.getColumnVisibilityData(), k1.getTimestamp(),
        k2.getColumnFamilyData(), k2.getColumnQualifierData(), k2.getColumnVisibilityData(), k2.getTimestamp(),
        e1.getValue(), e2.getValue());
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
