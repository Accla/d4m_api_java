package edu.mit.ll.graphulo.skvi;

import edu.mit.ll.graphulo.util.PeekingIterator1;
import edu.mit.ll.graphulo.util.RangeSet;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Limits the Ranges seeked to to a certain subset.
 * This functionality is built into the RemoteWriteIterator.
 */
public class SeekFilterIterator implements SortedKeyValueIterator<Key, Value>, OptionDescriber {
  private static final Logger log = LoggerFactory.getLogger(SeekFilterIterator.class);

  SortedKeyValueIterator<Key, Value> source;
  private RangeSet rowRanges = new RangeSet();
  /**
   * Holds the current range we are scanning.
   * Goes through the part of ranges after seeking to the beginning of the seek() clip.
   */
  private PeekingIterator1<Range> rowRangeIterator;
  private Range seekRange;
  private Collection<ByteSequence> columnFamilies;
  private boolean inclusive;

  /**
   * Call init() after construction.
   */
  public SeekFilterIterator() {
  }

  SeekFilterIterator(SeekFilterIterator other) {
    other.rowRanges = rowRanges;
  }

  static final IteratorOptions iteratorOptions;

  static {
    Map<String, String> optDesc = new LinkedHashMap<>();
    optDesc.put(RemoteSourceIterator.ROWRANGES, "Row ranges to scan for remote Accumulo table, Matlab syntax. (default ':,' all)");
    iteratorOptions = new IteratorOptions("SeekFilterIterator",
        "Intersects seek ranges with the option given, passing the reduced range to the parent.",
        Collections.unmodifiableMap(optDesc), null);
  }

  @Override
  public IteratorOptions describeOptions() {
    return iteratorOptions;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    return validateOptionsStatic(options);
  }

  public static boolean validateOptionsStatic(Map<String, String> options) {
    new SeekFilterIterator().parseOptions(options);
    return true;
  }

  private void parseOptions(Map<String, String> map) {
    for (Map.Entry<String, String> entry : map.entrySet()) {
      if (entry.getValue().isEmpty())
        continue;
      switch (entry.getKey()) {
        case RemoteSourceIterator.ROWRANGES:
          rowRanges.setTargetRanges(RemoteWriteIterator.parseRanges(entry.getValue()));
          break;
        default:
          log.warn("Unrecognized option: " + entry);
          continue;
      }
      log.trace("Option OK: " + entry);
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> map, IteratorEnvironment iteratorEnvironment) throws IOException {
    this.source = source;
    parseOptions(map);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    noSeekTopFlag = false;
    this.seekRange = range;
    this.columnFamilies = columnFamilies;
    this.inclusive = inclusive;
    rowRangeIterator = rowRanges.iteratorWithRangeMask(seekRange);
    if (!rowRangeIterator.hasNext())
      noSeekTopFlag = true;
    else
      next(true);
  }

  boolean noSeekTopFlag = false;

  @Override
  public boolean hasTop() {
    return !noSeekTopFlag && source.hasTop();
  }

  @Override
  public void next() throws IOException {
    next(false);
  }

  private void next(boolean initialSeek) throws IOException {
    if (!initialSeek)
      source.next();
    while (initialSeek || (!source.hasTop() && rowRangeIterator.hasNext())) {
      Range targetRange = rowRangeIterator.next();
      assert targetRange.clip(seekRange, true) != null;
      source.seek(targetRange, columnFamilies, inclusive);
      initialSeek = false;
    }
  }

  @Override
  public Key getTopKey() {
    return source.getTopKey();
  }

  @Override
  public Value getTopValue() {
    return source.getTopValue();
  }

  @Override
  public SeekFilterIterator deepCopy(IteratorEnvironment iteratorEnvironment) {
    SeekFilterIterator copy = new SeekFilterIterator(this);
    copy.source = source.deepCopy(iteratorEnvironment);
    return copy;
  }
}
