package edu.mit.ll.graphulo.apply;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
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
import java.util.Iterator;
import java.util.Map;

import static java.lang.Boolean.TRUE;

/**
 * Only retain the part of the Key given as a PartialKey option.
 * If null, then reduces the Key to the seek start Key (which is the all empty fields Key if seek range starts at -inf).
 */
public class KeyRetainOnlyApply implements ApplyOp {
  private static final Logger log = LoggerFactory.getLogger(KeyRetainOnlyApply.class);

  public static final String PARTIAL_KEY = "PartialKey";

  /** Create an IteratorSetting that prunes every Key it sees.
   * A null <tt>pk</tt> means reduce the Key to the seek start Key (which is the all empty fields Key if seek range starts at -inf). */
  public static IteratorSetting iteratorSetting(int priority, PartialKey pk) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, KeyRetainOnlyApply.class.getName());
    if( pk == null )
      itset.addOption(ApplyIterator.IGNORE_SEEK_EXCLUSIVE_START, TRUE.toString());
    else
      itset.addOption(ApplyIterator.APPLYOP+GraphuloUtil.OPT_SUFFIX+PARTIAL_KEY, pk.name());
    return itset;
  }

  private PartialKey pk = null;
  private Key seekStartKey = null;

  private void parseOptions(Map<String,String> options) {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String v = entry.getValue();
      switch (entry.getKey()) {
        case PARTIAL_KEY:
          if (v.isEmpty())
            pk = null;
          else
            pk = PartialKey.valueOf(v);
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
//  private final static IntegerLexicoder INTEGER_LEXICODER = new IntegerLexicoder();
  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) {
//    log.info("keyretainonlyapply see "+k.toStringNoTime()+" -> "+INTEGER_LEXICODER.decode(v.get()));
    Key knew;
    if (pk == null)
      knew = seekStartKey;
    else {
      knew = GraphuloUtil.keyCopy(k, pk);
      if (knew.compareTo(seekStartKey) < 0)
        return null;
    }
    return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(knew, v));
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    if (range.isInfiniteStartKey())
      seekStartKey = new Key();
    else
      seekStartKey = range.isStartKeyInclusive() ? range.getStartKey() : range.getStartKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL); // problem
  }
}
