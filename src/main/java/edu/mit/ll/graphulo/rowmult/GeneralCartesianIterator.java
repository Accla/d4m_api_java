package edu.mit.ll.graphulo.rowmult;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.mit.ll.graphulo.util.PeekingIterator1;

/**
 * Emits Cartesian product of provided iterators, passing each pair of entries to multiply function.
 * Default is to stream through A once and iterate through B many times.
 * Pass <tt>switched</tt> as true if the two are switched.
 */
public class GeneralCartesianIterator<K,V,RK,RV> implements Iterator<Map.Entry<RK, RV>> {
  private static final Logger log = LoggerFactory.getLogger(GeneralCartesianIterator.class);

  public interface MCondition<K,V> {
    boolean shouldMultiply(Map.Entry<K, V> eA, Map.Entry<K, V> eB);
  }

  public interface Multiply<K,V,RK,RV> {
    Iterator<? extends Map.Entry<RK,RV>> multiply(Map.Entry<K,V> eA, Map.Entry<K, V> eB);
  }

  private final SortedMap<K, V> BrowMap;
  private final boolean switched;
  private final PeekingIterator<Map.Entry<K, V>> itAonce;
  private Iterator<Map.Entry<K, V>> itBreset;
  private final Multiply<K,V,RK,RV> multiplyOp;
  private Iterator<? extends Map.Entry<RK, RV>> multiplyOpIterator = Collections.emptyIterator();
  private final MCondition<K,V> mcondition;

  public GeneralCartesianIterator(Iterator<Map.Entry<K, V>> itAonce, SortedMap<K, V> mapBreset,
                                  Multiply<K,V,RK,RV> multiplyOp, boolean switched) {
    this(itAonce, mapBreset, multiplyOp, switched, null);
  }

  public GeneralCartesianIterator(Iterator<Map.Entry<K, V>> itAonce, SortedMap<K, V> mapBreset,
                                  Multiply<K,V,RK,RV> multiplyOp, boolean switched, MCondition<K,V> mcondition) {
    BrowMap = mapBreset;
    this.switched = switched;
    this.itAonce = Iterators.peekingIterator(itAonce);
    this.itBreset = BrowMap.entrySet().iterator();
    this.multiplyOp = multiplyOp;
    this.mcondition = mcondition;
    if (itAonce.hasNext() && itBreset.hasNext())
      prepNext();
  }

  @Override
  public boolean hasNext() {
    return multiplyOpIterator.hasNext();
  }

  @Override
  public Map.Entry<RK, RV> next() {
    Map.Entry<RK, RV> ret = multiplyOpIterator.next();
    if (!multiplyOpIterator.hasNext() && itBreset.hasNext())
      prepNext();
    return ret;
  }

  private void prepNext() {
    do {
      Map.Entry<K, V> eA, eB = itBreset.next();
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

  private Iterator<? extends Map.Entry<RK, RV>> multiplyEntry(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {
    return multiplyOp.multiply(e1, e2);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
