package edu.mit.ll.graphulo.apply;

import com.google.common.collect.Iterators;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_TEXT;

/**
 * For every entry passed in, emits an entry with the same row,
 * empty column family, and column qualifier set to the row.
 */
public class RowToDiagonalApply implements ApplyOp {
  private static final Logger log = LoggerFactory.getLogger(RowToDiagonalApply.class);

  public static IteratorSetting iteratorSetting(int priority) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, RowToDiagonalApply.class.getName());
    return itset;
  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) {
    Text row = k.getRow();
    Key knew = new Key(row, EMPTY_TEXT, row);
    return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(knew, v));
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

  }

}
