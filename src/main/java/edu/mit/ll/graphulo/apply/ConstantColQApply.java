package edu.mit.ll.graphulo.apply;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.util.GraphuloUtil;

/**
 * Only retain the part of the Key given as a PartialKey option.
 * If null, then reduces the Key to the seek start Key (which is the all empty fields Key if seek range starts at -inf).
 */
public class ConstantColQApply implements ApplyOp {
//  private static final Logger log = LogManager.getLogger(ConstantColQApply.class);
  private static final Logger log = LoggerFactory.getLogger(ConstantColQApply.class);

  public static final String COLQ = "COLQ";

  /** Create an IteratorSetting that prunes every Key it sees.
   * A null <tt>pk</tt> means reduce the Key to the seek start Key (which is the all empty fields Key if seek range starts at -inf). */
  public static IteratorSetting iteratorSetting(int priority, String colq) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, ConstantColQApply.class.getName());
    if (colq == null)
      colq = "";
    itset.addOption(ApplyIterator.APPLYOP+GraphuloUtil.OPT_SUFFIX+ COLQ, colq);
    return itset;
  }

  private byte[] colq = new byte[0];

  private void parseOptions(Map<String,String> options) {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String v = entry.getValue();
      switch (entry.getKey()) {
        case COLQ:
          colq = v.getBytes(StandardCharsets.UTF_8);
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
    Key knew;
    knew = new Key(k.getRowData().toArray(), k.getColumnFamilyData().toArray(), colq, k.getColumnVisibilityData().toArray(), k.getTimestamp(), k.isDeleted(), true);
    return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(knew, v));
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {}
}
