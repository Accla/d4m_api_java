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
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * Emit only the top KMER Values per row, each decoded as a Double.
 */
public class NoConsecutiveDuplicateRowsIterator implements SortedKeyValueIterator<Key,Value> {
  private static final Logger log = LoggerFactory.getLogger(NoConsecutiveDuplicateRowsIterator.class);

  public static final String K = "k";//, //SCALAR_TYPE = MathTwoScalar.SCALAR_TYPE; //"encoderType";

  /** Pass columns as null or empty to combine on all columns. */
  public static IteratorSetting combinerSetting(int priority) {
    IteratorSetting itset = new IteratorSetting(priority, NoConsecutiveDuplicateRowsIterator.class);
    return itset;
  }


//  private Map<String,String> initOptions;
  private SortedKeyValueIterator<Key,Value> source;
  private SortedMap<Key,Value> storedRow;
  private PeekingIterator1<Map.Entry<Key,Value>> retIter;

//  private void parseOptions(Map<String, String> options) {
//    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
//      String optionKey = optionEntry.getKey();
//      String optionValue = optionEntry.getValue();
//      switch (optionKey) {
//
//        default:
//          log.warn("Unrecognized option: " + optionEntry);
//      }
//    }
//  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
//    this.initOptions = new HashMap<>(options);
//    parseOptions(options);
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    NoConsecutiveDuplicateRowsIterator copy = new NoConsecutiveDuplicateRowsIterator();
//    try {
//      copy.init(source.deepCopy(env), initOptions, env);
//    } catch (IOException e) {
//      log.error("problem creating new instance of TopColPerRowIterator from options "+initOptions, e);
//      throw new RuntimeException(e);
//    }
    return copy;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
    storedRow = SKVIRowIterator.readRowIntoMap(source);
    prepareNext();
  }

  private void prepareNext() throws IOException {
//    if (storedRow == null) {
//      if (!source.hasTop())
//        return;
//      storedRow = SKVIRowIterator.readRowIntoMap(source);
//    }
    retIter = new PeekingIterator1<>(storedRow.entrySet().iterator());
    if (!retIter.hasNext())
      return;

    SortedMap<Key, Value> map2;
    do {
      map2 = SKVIRowIterator.readRowIntoMap(source);
    } while (customEquals(storedRow, map2));
    storedRow = map2;
  }

  private boolean customEquals(SortedMap<Key, Value> map1, SortedMap<Key, Value> map2) {
    Iterator<Map.Entry<Key, Value>> it1 = map1.entrySet().iterator(), it2 = map2.entrySet().iterator();
    while (it1.hasNext() && it2.hasNext()) {
      Map.Entry<Key, Value> next1 = it1.next(), next2 = it2.next();
      if (!keyEqualsNotRow(next1.getKey(), next2.getKey()) || !next1.getValue().equals(next2.getValue()))
        return false;
    }
    return !(it1.hasNext() || it2.hasNext());
  }

  private boolean keyEqualsNotRow(Key key1, Key key2) {
    return
        key1.getColumnFamilyData().equals(key2.getColumnFamilyData()) &&
        key1.getColumnQualifierData().equals(key2.getColumnQualifierData()) &&
        key1.getColumnFamilyData().equals(key2.getColumnVisibilityData());
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
    if (!retIter.hasNext())
      prepareNext();
  }

}
