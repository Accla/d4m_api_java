package edu.mit.ll.graphulo.skvi;

import edu.mit.ll.graphulo.util.PeekingIterator1;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
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
import java.util.Map;
import java.util.SortedMap;

/**
 * For testing; an iterator that emits entries from a list of hardcoded data.
 */
public class MapIterator implements SortedKeyValueIterator<Key, Value> {
  private static final Logger log = LoggerFactory.getLogger(MapIterator.class);

  private SortedMap<Key, Value> allEntriesToInject;
  private PeekingIterator1<Map.Entry<Key, Value>> inner;
  private Range seekRng;

  public MapIterator(SortedMap<Key,Value> map) {
    allEntriesToInject = map;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    if (source != null)
      log.warn("MapIterator ignores/replaces parent source passed in init(): " + source);
    // define behavior before seek as seek to start at negative infinity
    inner = new PeekingIterator1<>(allEntriesToInject.entrySet().iterator());
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    MapIterator newInstance;
    try {
      newInstance = MapIterator.class.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    newInstance.inner = new PeekingIterator1<>(allEntriesToInject.tailMap(inner.peek().getKey()).entrySet().iterator());
    return newInstance;
  }

  @Override
  public boolean hasTop() {
    if (!inner.hasNext())
      return false;
    Key k = inner.peek().getKey();
    return seekRng.contains(k);
  }

  @Override
  public void next() throws IOException {
    inner.next();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    seekRng = range;
    // seek to first entry inside range
    if (range.isInfiniteStartKey())
      inner = new PeekingIterator1<>(allEntriesToInject.entrySet().iterator());
    else if (range.isStartKeyInclusive())
      inner = new PeekingIterator1<>(allEntriesToInject.tailMap(range.getStartKey()).entrySet().iterator());
    else
      inner = new PeekingIterator1<>(allEntriesToInject.tailMap(range.getStartKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME)).entrySet().iterator());
  }

  @Override
  public Key getTopKey() {
    return hasTop() ? inner.peek().getKey() : null;
  }

  @Override
  public Value getTopValue() {
    return hasTop() ? inner.peek().getValue() : null;
  }
}
