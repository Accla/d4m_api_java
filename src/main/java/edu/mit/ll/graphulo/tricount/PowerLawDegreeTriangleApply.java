package edu.mit.ll.graphulo.tricount;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.apply.ApplyOp;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.UIntegerLexicoder;
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
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class PowerLawDegreeTriangleApply implements ApplyOp {
  private static final Logger log = LoggerFactory.getLogger(PowerLawDegreeTriangleApply.class);

  /**
   */
  public static IteratorSetting iteratorSetting(final int priority) {
    final IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, PowerLawDegreeTriangleApply.class.getName());
    return itset;
  }

//  private void parseOptions(Map<String,String> options) {
//    for (Map.Entry<String, String> entry : options.entrySet()) {
//      String v = entry.getValue();
//      switch (entry.getKey()) {
//        case PARTIAL_KEY:
//          if (v.isEmpty())
//            pk = null;
//          else
//            pk = PartialKey.valueOf(v);
//          break;
//        default:
//          log.warn("Unrecognized option: " + entry);
//          break;
//      }
//    }
//  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    //parseOptions(options);
  }
  private final static Lexicoder<Integer> INTEGER_LEXICODER = new UIntegerLexicoder();
//  private final static Lexicoder<Long> LONG_LEXICODER = new ULongLexicoder();

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) {
//    log.info("keyretainonlyapply see "+k.toStringNoTime()+" -> "+INTEGER_LEXICODER.decode(v.get()));
    final int i = INTEGER_LEXICODER.decode(v.get());
    final int inew = i % 2 == 0 ? (i/2)*(i-1) : ((i-1)/2)*i; // try to avoi overflow
    final Value vnew = new Value(INTEGER_LEXICODER.encode(inew));
    return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, vnew));
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
  }
}
