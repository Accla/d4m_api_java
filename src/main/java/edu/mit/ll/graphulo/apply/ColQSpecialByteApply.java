package edu.mit.ll.graphulo.apply;

import com.google.common.collect.Iterators;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Adds two 0 bytes to the end of the column qualifier.
 */
public class ColQSpecialByteApply implements ApplyOp {
  private static final Logger log = LogManager.getLogger(ColQSpecialByteApply.class);

  static final byte SPECIAL_BYTE = 0;

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
  }

  static Key addSpecialBytes(final Key k) {
    byte[] colQBytes = k.getColumnQualifierData().getBackingArray();
    byte[] newColQBytes = new byte[colQBytes.length+2];
    System.arraycopy(colQBytes,0,newColQBytes,0,colQBytes.length);
    newColQBytes[newColQBytes.length-2] = newColQBytes[newColQBytes.length-1] = SPECIAL_BYTE;
    return new Key(k.getRowData().getBackingArray(), k.getColumnFamilyData().getBackingArray(),
        newColQBytes, k.getColumnVisibilityData().getBackingArray(), k.getTimestamp(), k.isDeleted());
  }

  static Key removeSpecialBytes(final Key k) {
    byte[] colQBytes = k.getColumnQualifierData().getBackingArray();
    if (colQBytes.length < 2 || colQBytes[colQBytes.length-1] != SPECIAL_BYTE
        || colQBytes[colQBytes.length-2] != SPECIAL_BYTE)
      return null;
    byte[] newColQBytes = new byte[colQBytes.length-2];
    System.arraycopy(colQBytes,0,newColQBytes,0,newColQBytes.length);
    return new Key(k.getRowData().getBackingArray(), k.getColumnFamilyData().getBackingArray(),
        newColQBytes, k.getColumnVisibilityData().getBackingArray(), k.getTimestamp(), k.isDeleted());
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(final Key k, Value v) {
    Key newKey = addSpecialBytes(k);
    return Iterators.singletonIterator( new AbstractMap.SimpleImmutableEntry<>(newKey,v));
  }
}