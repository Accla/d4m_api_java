package edu.mit.ll.graphulo.skvi;

import edu.mit.ll.graphulo.util.PeekingIterator1;
import edu.mit.ll.graphulo.util.SKVIRowIterator;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Emit only the top KMER Values per row, each decoded as a Double.
 */
public class TopColPerRowIterator implements SortedKeyValueIterator<Key,Value> {
  private static final Logger log = LoggerFactory.getLogger(TopColPerRowIterator.class);

  public static final String K = "k";//, //SCALAR_TYPE = MathTwoScalar.SCALAR_TYPE; //"encoderType";

  public static IteratorSetting combinerSetting(int priority, int k/*, ScalarType scalarType*/) {
    IteratorSetting itset = new IteratorSetting(priority, TopColPerRowIterator.class);
    itset.addOption(K, Integer.toString(k));
    //itset.addOption(SCALAR_TYPE, scalarType.name());
    return itset;
  }


  private Map<String,String> initOptions;
  private SortedKeyValueIterator<Key,Value> source;
  private SKVIRowIterator sourceRows;
  //private MathTwoScalar.ScalarType scalarType = MathTwoScalar.ScalarType.BIGDECIMAL;
  private int k = 10;
  // for returning in sorted Key order
  private PeekingIterator1<Map.Entry<Key,Value>> retIter;

  private static class NumberKeyValue implements Comparable<NumberKeyValue> {
    Double n;
    Key k;
    Value v;
//    public NumberKeyValue(Key k, Value v) {
//      this.k = k; this.v = v;
//      this.n = Double.valueOf(v.toString());
//    }
    public NumberKeyValue(Double n, Key k, Value v) {
      this.k = new Key(k); this.v = new Value(v);
      this.n = n;
    }
    public NumberKeyValue(Map.Entry<Key,Value> entry) {
      this.k = new Key(entry.getKey()); this.v = new Value(entry.getValue());
      this.n = Double.valueOf(v.toString());
    }
    @Override
    public int compareTo(NumberKeyValue o) {
      return Double.compare(n, o.n);
    }
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NumberKeyValue that = (NumberKeyValue) o;
      return n.equals(that.n);
    }

    @Override
    public int hashCode() {
      return n.hashCode();
    }

    public NumberKeyValue setToCopy(double d, Key k, Value v) {
      this.n = d; this.k = new Key(k); this.v = new Value(v);
      return this;
    }
  }

  private void parseOptions(Map<String, String> options) {
    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
      switch (optionKey) {
        case K:
          k = Integer.parseInt(optionValue);
          break;
//        case SCALAR_TYPE:
//          scalarType = ScalarType.valueOf(optionValue);
//          break;
        default:
          log.warn("Unrecognized option: " + optionEntry);
      }
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
    this.initOptions = new HashMap<>(options);
    parseOptions(options);
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    TopColPerRowIterator copy = new TopColPerRowIterator();
    try {
      copy.init(source.deepCopy(env), initOptions, env);
    } catch (IOException e) {
      log.error("problem creating new instance of TopColPerRowIterator from options "+initOptions, e);
      throw new RuntimeException(e);
    }
    return copy;
  }


  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
    sourceRows = new SKVIRowIterator(source);
    prepareNext();
  }

  private void prepareNext() throws IOException {
    PriorityQueue<NumberKeyValue> pq;
    pq = new PriorityQueue<>(k);
    while (pq.size() < k && sourceRows.hasNext()) {
      Map.Entry<Key, Value> next = sourceRows.next();
//      System.out.printf("ADD     %.1f          \n", Double.parseDouble(next.getValue().toString()));
      pq.add(new NumberKeyValue(next));
    }
    while (sourceRows.hasNext()) {
      Map.Entry<Key, Value> entry = sourceRows.next();
      Key k = entry.getKey();
      Value v = entry.getValue();
      double d = Double.parseDouble(v.toString());
      if (d > pq.peek().n) {  // if greater than the smallest of the top KMER
//        System.out.printf("REPLACE %.1f with %.1f\n", pq.peek().n, d);
        pq.add(pq.remove().setToCopy(d, k, v));
      }
//      else
//        System.out.printf("KEEP    %.1f over %.1f\n", pq.peek().n, d);
    }
    SortedMap<Key,Value> retMap = new TreeMap<>();
    for (NumberKeyValue nkv : pq) {
      retMap.put(nkv.k, nkv.v);
    }
//    System.out.println("FINAL "+retMap);
    retIter = new PeekingIterator1<>(retMap.entrySet().iterator());
  }


  @Override
  public Key getTopKey() {
    return retIter.peek().getKey();
  }

  @Override
  public Value getTopValue() {
    return retIter.peek().getValue();
  }

  @Override
  public boolean hasTop() {
    return retIter.hasNext();
  }

  @Override
  public void next() throws IOException {
    retIter.next();
    if (!retIter.hasNext() && sourceRows.reuseNextRow()) 
      prepareNext();
  }

}
