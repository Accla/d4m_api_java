package edu.mit.ll.graphulo_ocean;

import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.apply.ApplyOp;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Generate entries for all the kmer-mers of the sequence in the column qualifier.
 */
public class KMerColQApply implements ApplyOp {
  private static final Logger log = LoggerFactory.getLogger(KMerColQApply.class);

  public static final String KMER = "KMER";

  public static IteratorSetting iteratorSetting(int priority, int k) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, KMerColQApply.class.getName());
    if (k > 0)
      itset.addOption(ApplyIterator.APPLYOP+GraphuloUtil.OPT_SUFFIX+ KMER, Integer.toString(k));
    return itset;
  }

  private int kmer = 11;

  private void parseOptions(Map<String,String> options) {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String v = entry.getValue();
      switch (entry.getKey()) {
        case KMER:
            kmer = Integer.valueOf(v);
          break;
        default:
          log.warn("Unrecognized option: " + entry);
          break;
      }
    }
  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    parseOptions(options);
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) {
    String origSeq = k.getColumnQualifier().toString();
    if (origSeq.length() < kmer)
      return Collections.emptyIterator();
    SortedMap<String,Integer> map = new TreeMap<>();

    for (int i = 0; i < origSeq.length()-kmer; i++) {
      String mer = origSeq.substring(i,i+kmer);
      Integer curval = map.get(mer);
      int newval = curval == null ? 1 : curval+1;
      map.put(mer, newval);
    }
    return new KMerIterator(k, map.entrySet().iterator());

//    return new KMerIterator(origSeq, kmer);
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {}


  private static class KMerIterator implements Iterator<Map.Entry<Key, Value>> {
    private final Iterator<Map.Entry<String, Integer>> olditer;
    private final byte[] row, colf, colvis;
    private final long ts;

    public KMerIterator(Key k, Iterator<Map.Entry<String, Integer>> olditer) {
      row=k.getRowData().toArray();
      colf = k.getColumnFamilyData().toArray();
      colvis = k.getColumnVisibilityData().toArray();
      ts = k.getTimestamp();
      this.olditer = olditer;
    }

    @Override
    public boolean hasNext() {
      return olditer.hasNext();
    }

    @Override
    public Map.Entry<Key, Value> next() {
      Map.Entry<String, Integer> next = olditer.next();
      return new AbstractMap.SimpleImmutableEntry<>(
          new Key(row, colf, next.getKey().getBytes(UTF_8), colvis, ts),
          new Value(next.getValue().toString().getBytes(UTF_8)));
    }

    @Override
    public void remove() {
      olditer.remove();
    }
  }



//  private static final Value VALUE_ONE = new Value("1".getBytes(UTF_8));

//  private static class KMerIterator implements Iterator<Map.Entry<Key, Value>> {
//    private final int kmer;
//    private final String origSeq;
//    private final Key baseKey;
//    private int i = 0;
//
//    public KMerIterator(Key k, Value v, int kmer) {
//      this.baseKey = new Key(k);
//      this.origSeq = k.getColumnQualifier().toString();
//      this.kmer = kmer;
//    }
//
//    @Override
//    public boolean hasNext() {
//      return false;
//    }
//
//    @Override
//    public Map.Entry<Key,Value> next() {
//      return null;
//    }
//
//    @Override
//    public void remove() {
//      throw new UnsupportedOperationException();
//    }
//  }
}
