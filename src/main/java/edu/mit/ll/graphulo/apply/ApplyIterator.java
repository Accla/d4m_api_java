package edu.mit.ll.graphulo.apply;

import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.PeekingIterator1;
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

/**
 * Apply an ApplyOp to every entry from the source iterator.
 */
public class ApplyIterator implements SortedKeyValueIterator<Key, Value> {
  //private static final Logger log = LogManager.getLogger(ApplyIterator.class);
  private static final Logger log = LoggerFactory.getLogger(ApplyIterator.class);

  public static final String APPLYOP = "applyOp";
  public static final String IGNORE_SEEK_EXCLUSIVE_START = "ignoreSeekExclusiveStart";


  private SortedKeyValueIterator<Key, Value> source;
  private ApplyOp applyOp;
  private boolean ignoreSeekExclusiveStart = false;
  private Map<String,String> applyOpOptions = new HashMap<>();

  private PeekingIterator1<? extends Map.Entry<Key,Value>> topIterator;

  private void parseOptions(Map<String, String> options) {
    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
      String optionKey = optionEntry.getKey(), optionValue = optionEntry.getValue();
      if (optionKey.startsWith(APPLYOP+GraphuloUtil.OPT_SUFFIX)) {
        String keyAfterPrefix = optionKey.substring((APPLYOP+GraphuloUtil.OPT_SUFFIX).length());
        applyOpOptions.put(keyAfterPrefix, optionValue);
      } else {
        switch (optionKey) {
          case APPLYOP:
            applyOp = GraphuloUtil.subclassNewInstance(optionValue, ApplyOp.class);
            break;
          case IGNORE_SEEK_EXCLUSIVE_START:
            ignoreSeekExclusiveStart = Boolean.parseBoolean(optionValue);
            break;
          default:
            log.warn("Unrecognized option: " + optionEntry);
        }
      }
    }
    if (applyOp == null)
      throw new IllegalArgumentException("Must specify ApplyOp in options. Given: "+options);
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
    parseOptions(options);
    applyOp.init(applyOpOptions, env);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
//    log.info("apply seek: "+range);
    if( ignoreSeekExclusiveStart && !range.isStartKeyInclusive() ) {
      topIterator = PeekingIterator1.emptyIterator();
      return;
    }
    source.seek(range, columnFamilies, inclusive);
    applyOp.seekApplyOp(range, columnFamilies, inclusive);
    if (source.hasTop()) {
      topIterator = new PeekingIterator1<>(applyOp.apply(source.getTopKey(), source.getTopValue()));
      prepNext(false);
    } else {
      topIterator = PeekingIterator1.emptyIterator();
    }
  }

  private void prepNext(boolean doNext) throws IOException {
    if (doNext)
      topIterator.next();

    while (!topIterator.hasNext() /*&& source.hasTop()*/) {
      source.next();
      if (!source.hasTop())
        return;
      topIterator = new PeekingIterator1<>(applyOp.apply(source.getTopKey(), source.getTopValue()));
    }
  }

  @Override
  public void next() throws IOException {
    prepNext(true);
  }

  @Override
  public boolean hasTop() {
    return topIterator.hasNext();
  }

  @Override
  public Key getTopKey() {
    return topIterator.peek().getKey();
  }

  @Override
  public Value getTopValue() {
    return topIterator.peek().getValue();
  }

  @Override
  public ApplyIterator deepCopy(IteratorEnvironment env) {
    ApplyIterator copy = new ApplyIterator();
    try {
      copy.applyOp = applyOp.getClass().newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      log.error("", e);
      throw new RuntimeException("",e);
    }
    try {
      copy.applyOp.init(applyOpOptions, env);
    } catch (IOException e) {
      log.error("", e);
      throw new RuntimeException("",e);
    }
    copy.applyOpOptions = applyOpOptions;
    copy.source = source.deepCopy(env);
    return copy;
  }
}
