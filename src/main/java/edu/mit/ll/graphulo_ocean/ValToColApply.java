package edu.mit.ll.graphulo_ocean;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.apply.ApplyOp;
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
import java.util.Iterator;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Put the Value in the Column Qualifier.
 */
public class ValToColApply implements ApplyOp {
  private static final Logger log = LoggerFactory.getLogger(ValToColApply.class);

  public static IteratorSetting iteratorSetting(int priority) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, ValToColApply.class.getName());
    return itset;
  }

  private static final Value VALUE_ONE = new Value("1".getBytes(UTF_8));

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) {
    Key knew = new Key(k.getRowData().toArray(), k.getColumnFamilyData().toArray(), v.get(), k.getColumnVisibilityData().toArray(), k.getTimestamp());
    return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(knew, VALUE_ONE));
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
  }
}
