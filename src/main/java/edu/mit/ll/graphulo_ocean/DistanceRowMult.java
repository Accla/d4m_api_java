package edu.mit.ll.graphulo_ocean;

import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.rowmult.RowMultiplyOp;
import edu.mit.ll.graphulo.skvi.DoubleLexicoderTemp;
import edu.mit.ll.graphulo.skvi.RemoteSourceIterator;
import edu.mit.ll.graphulo.skvi.Watch;
import edu.mit.ll.graphulo.util.PeekingIterator1;
import edu.mit.ll.graphulo.util.SKVIRowIterator;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import static edu.mit.ll.graphulo.rowmult.CartesianRowMultiply.readRowColumns;
import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_TEXT;

/**
 * Bray-Curtis index.
 * Don't emit non-matching entries.
 * For matching entries (kmer, sample1, sample2, cnt1, cnt2):
 * Get from degree map deg1, deg2;
 * emit (sample1, sample2, min(cnt1 / deg1, cnt2 / deg2)).
 *
 * Rows aligned at kmer. Only emit when sample2 > sample1. Hold row2 in memory.
 * Holds degrees in memory.
 */
public class DistanceRowMult implements RowMultiplyOp {
  private static final Logger log = LoggerFactory.getLogger(DistanceRowMult.class);

  private void scanDegreeTable() throws IOException {
    remoteDegTable.seek(new Range(), Collections.<ByteSequence>emptySet(), false);
    while (remoteDegTable.hasTop()) {
      degMap.put(remoteDegTable.getTopKey().getRow(),
          Long.valueOf(remoteDegTable.getTopValue().toString()));
      remoteDegTable.next();
    }
  }

  /** Setup with {@link edu.mit.ll.graphulo.Graphulo#basicRemoteOpts(String, String, String, Authorizations)}
   * basicRemoteOpts(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX, ADeg, null, Aauthorizations)
   * options for RemoteSourceIterator. */
  public static IteratorSetting iteratorSetting(int priority, Map<String,String> remoteOpts) {
    IteratorSetting is = new IteratorSetting(priority, ApplyIterator.class, remoteOpts);
    is.addOption(ApplyIterator.APPLYOP, DistanceApply.class.getName());
    return is;
  }

  private RemoteSourceIterator remoteDegTable;
  private Map<Text,Long> degMap;

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    remoteDegTable = new RemoteSourceIterator();
    remoteDegTable.init(null, options, env);
    degMap = new HashMap<>();
    scanDegreeTable();
  }

  private static final TypedValueCombiner.Encoder<Long> LEX = new LongLexicoder();
  private static final TypedValueCombiner.Encoder<Double> LEXDOUBLE = new DoubleLexicoderTemp(); // attempt 1.6 compat
  private static final byte[] ZERO_BYTE = new byte[] { 0x00 };

  @Override
  public Iterator<Map.Entry<Key, Value>> multiplyRow(
      SortedKeyValueIterator<Key, Value> skviA, SortedKeyValueIterator<Key, Value> skviB) throws IOException {
    assert skviA != null && skviB != null;

//    Text rowKmer = skviA.getTopKey().getRow();
    final PeekingIterator1<Map.Entry<Key, Value>> itAonce = new PeekingIterator1<>(new SKVIRowIterator(skviA));
    final SortedMap<Text, Value> BrowMap = readRowColumns(skviB, null, Watch.PerfSpan.Bnext);

    return new DistanceRowMultIterator(itAonce, BrowMap);
  }

  private class DistanceRowMultIterator implements Iterator<Map.Entry<Key, Value>> {
//    private static final Logger log = LogManager.getLogger(DistanceRowMultIterator.class);

    private final SortedMap<Text, Value> BrowMap;
    private final PeekingIterator1<Map.Entry<Key, Value>> itAonce;
    private Iterator<Map.Entry<Text, Value>> itBreset;

    private Map.Entry<Key, Value> nextEntry;

    private Text textAfterColumn = new Text();

    private Text getTextAfterColumn() {
      byte[] obs = itAonce.peek().getKey().getColumnQualifierData().toArray();
      textAfterColumn.set(obs);
      textAfterColumn.append(ZERO_BYTE, 0, 1);
      return textAfterColumn;
    }

    public DistanceRowMultIterator(PeekingIterator1<Map.Entry<Key, Value>> itAonce, SortedMap<Text, Value> mapBreset) {
      BrowMap = mapBreset;
      this.itAonce = itAonce;
      if (!itAonce.hasNext()) {
        nextEntry = null;
        return;
      }
      this.itBreset = BrowMap.tailMap(getTextAfterColumn()).entrySet().iterator();
      if (itBreset.hasNext())
        prepNext();
      else {
        nextEntry = null;
      }
    }

    @Override
    public boolean hasNext() {
      return nextEntry != null;
    }

    @Override
    public Map.Entry<Key, Value> next() {
      Map.Entry<Key, Value> ret = nextEntry;
      nextEntry = null;
      if (itBreset.hasNext())
        prepNext();
      return ret;
    }

    private void prepNext() {
      Map.Entry<Key, Value> eA;
      Map.Entry<Text, Value> eB = itBreset.next();
      if (!itBreset.hasNext()) {
        eA = itAonce.next();    // advance itA
        if (itAonce.hasNext())  // STOP if no more itA
          itBreset = BrowMap.tailMap(getTextAfterColumn()).entrySet().iterator();
        else
          return;
      } else
        eA = itAonce.peek();

      Text cola = eA.getKey().getColumnQualifier();
      Key nk = new Key(cola, EMPTY_TEXT, eB.getKey());
      long a = LEX.decode(eA.getValue().get());
      long b = LEX.decode(eB.getValue().get());

      long da = degMap.get(cola);
      long db = degMap.get(eB.getKey());

      double nd = Math.min(((double)a)/da, ((double)b)/db); /// (da * db); // full calc is 1 - 2*
      Value nv = new Value(LEXDOUBLE.encode(nd)); //new Value(Double.toString(nd).getBytes(UTF_8));
      nextEntry = new AbstractMap.SimpleImmutableEntry<>(nk, nv);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }




}
