package edu.mit.ll.graphulo.skvi;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.util.IteratorAdapter;
import edu.mit.ll.graphulo.util.MemMatrixUtil;
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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Matrix inverse. Values interpreted as doubles, encoded as Strings.
 * Designed to be used at full major compaction so that it sees all the entries.
 */
public class InverseMatrixIterator implements SortedKeyValueIterator<Key,Value> {
  private static final Logger log = LoggerFactory.getLogger(MapIterator.class);

  public static final String MATRIX_SIZE = "matrixSize", NUMITERATIONS = "numIterations";

  public static IteratorSetting iteratorSetting(int priority, int matrixSize, int numIterations) {
    IteratorSetting itset = new IteratorSetting(priority, InverseMatrixIterator.class);
    itset.addOption(MATRIX_SIZE, Integer.toString(matrixSize));
    itset.addOption(NUMITERATIONS, Integer.toString(numIterations));
    return itset;
  }

  private SortedKeyValueIterator<Key,Value> source, mapIterator;
  private int matrixSize;
  private int numIterations;

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
    Preconditions.checkArgument(options.containsKey(MATRIX_SIZE));
    matrixSize = Integer.parseInt(options.get(MATRIX_SIZE));
    Preconditions.checkArgument(options.containsKey(NUMITERATIONS));
    numIterations = Integer.parseInt(options.get(NUMITERATIONS));
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    return new InverseMatrixIterator();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    if (!range.isInfiniteStartKey() || !range.isInfiniteStopKey())
      log.warn("Range is not infinite: "+range);
    source.seek(range, columnFamilies, inclusive);

    IteratorAdapter ia = new IteratorAdapter(source);
    SortedMap<Key, Value> map = MemMatrixUtil.matrixToMap(new TreeMap<Key, Value>(),
        MemMatrixUtil.doInverse(MemMatrixUtil.buildMatrix(ia, matrixSize), numIterations));


//    DebugUtil.printMapFull(map.entrySet().iterator());
    mapIterator = new MapIterator(map);
    mapIterator.init(null, null, null);
    mapIterator.seek(range, columnFamilies, inclusive);
  }

  @Override
  public Key getTopKey() {
    return mapIterator.getTopKey();
  }

  @Override
  public Value getTopValue() {
    return mapIterator.getTopValue();
  }

  @Override
  public boolean hasTop() {
    return mapIterator.hasTop();
  }

  @Override
  public void next() throws IOException {
    mapIterator.next();
  }




}
