package edu.mit.ll.graphulo.apply;

import com.google.common.collect.Iterators;
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

/**
 * Adds two 0 bytes to the end of the column qualifier.
 */
public class ColQSpecialByteApply implements ApplyOp {
  private static final Logger log = LoggerFactory.getLogger(ColQSpecialByteApply.class);

  public static IteratorSetting iteratorSetting(int priority) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, ColQSpecialByteApply.class.getName());
    return itset;
  }

  static final byte SPECIAL_BYTE = 0;

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
  }

  static Key addSpecialBytes(final Key k) {
    byte[] colQBytes = k.getColumnQualifierData().toArray();
    byte[] newColQBytes = new byte[colQBytes.length+2];
    System.arraycopy(colQBytes,0,newColQBytes,0,colQBytes.length);
    newColQBytes[newColQBytes.length-2] = newColQBytes[newColQBytes.length-1] = SPECIAL_BYTE;
    return new Key(k.getRowData().toArray(), k.getColumnFamilyData().toArray(),
        newColQBytes, k.getColumnVisibilityData().toArray(), k.getTimestamp(), k.isDeleted());
  }

  static Key removeSpecialBytes(final Key k) {
    byte[] colQBytes = k.getColumnQualifierData().toArray();
    if (colQBytes.length < 2 || colQBytes[colQBytes.length-1] != SPECIAL_BYTE
        || colQBytes[colQBytes.length-2] != SPECIAL_BYTE)
      return null;
    byte[] newColQBytes = new byte[colQBytes.length-2];
    System.arraycopy(colQBytes,0,newColQBytes,0,newColQBytes.length);
    return new Key(k.getRowData().toArray(), k.getColumnFamilyData().toArray(),
        newColQBytes, k.getColumnVisibilityData().toArray(), k.getTimestamp(), k.isDeleted());
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(final Key k, Value v) {
    Key newKey = addSpecialBytes(k);
    return Iterators.singletonIterator( new AbstractMap.SimpleImmutableEntry<>(newKey,v));
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
  }
}