package edu.mit.ll.graphulo.rowmult;

import com.google.common.primitives.Ints;
import edu.mit.ll.graphulo.util.PeekingIterator1;
import edu.mit.ll.graphulo.util.SKVIRowIteratorNoValues;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.client.lexicoder.UIntegerLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;

import static edu.mit.ll.graphulo.rowmult.CartesianRowMultiply.readRowColumnsNoValues;
import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_BYTES;
import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_TEXT;

/**
 * TableMult: (k,cq,v) * (k,cq',v') = (cq,cq',2)
 * only when cq < cq'.
 */
public class UpperTriTwoJoin implements RowMultiplyOp {
//  private static final Logger log = LogManager.getLogger(UpperTriTwoJoin.class);
  public static final String MAGIC = "magic";
  private boolean magic = false;

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    if( options.containsKey(MAGIC) )
      magic = Boolean.parseBoolean(options.get(MAGIC));
  }


  private static final TypedValueCombiner.Encoder<Integer> LEX = new UIntegerLexicoder();
  private static final byte[] ZERO_BYTE = new byte[] { 0x00 };
  private static final Value VALUE_TWO = new Value(LEX.encode(2));
  private static final Value VALUE_TWO_MAGIC = new Value(Ints.toByteArray(2));

  @Override
  public Iterator<Map.Entry<Key, Value>> multiplyRow(
      SortedKeyValueIterator<Key, Value> skviA, SortedKeyValueIterator<Key, Value> skviB) throws IOException {
    assert skviA != null && skviB != null;

//    Text rowKmer = skviA.getTopKey().getRow();
    final PeekingIterator1<Key> itAonce = new PeekingIterator1<>(new SKVIRowIteratorNoValues(skviA));
    final SortedSet<Text> BrowMap = readRowColumnsNoValues(skviB);

    return new UpperTriTwoIterator(itAonce, BrowMap);
  }

  private class UpperTriTwoIterator implements Iterator<Map.Entry<Key, Value>> {
//    private static final Logger log = LogManager.getLogger(UpperTriTwoIterator.class);

    private final SortedSet<Text> BrowMap;
    private final PeekingIterator1<Key> itAonce;
    private Iterator<Text> itBreset;

    private Map.Entry<Key, Value> nextEntry;

    private final Text textAfterColumn = new Text();

    private Text getTextAfterColumn() {
      byte[] obs = itAonce.peek().getColumnQualifierData().toArray();
      textAfterColumn.set(obs);
      textAfterColumn.append(ZERO_BYTE, 0, 1);
      return textAfterColumn;
    }

    public UpperTriTwoIterator(PeekingIterator1<Key> itAonce, SortedSet<Text> mapBreset) {
      BrowMap = mapBreset;
      this.itAonce = itAonce;
      if (!itAonce.hasNext()) {
        nextEntry = null;
        return;
      }
      this.itBreset = BrowMap.tailSet(getTextAfterColumn()).iterator();
      if (itBreset.hasNext())
        prepNext();
      else {
        nextEntry = null;
      }
    }

    @Override
    public boolean hasNext() {
      return nextEntry != null;
    }

    @Override
    public Map.Entry<Key, Value> next() {
      final Map.Entry<Key, Value> ret = nextEntry;
      nextEntry = null;
      if (itBreset.hasNext())
        prepNext();
      return ret;
    }

    private final Text cola = new Text();
    private final byte[] newRow = new byte[5];

    private void prepNext() {
      final Key eA;
      final Text eB = itBreset.next();
      if (!itBreset.hasNext()) {
        eA = itAonce.next();    // advance itA
        if (itAonce.hasNext())  // STOP if no more itA
          itBreset = BrowMap.tailSet(getTextAfterColumn()).iterator();
        else
          return;
      } else
        eA = itAonce.peek();

      final Key nk;

      if( !magic ) {
        eA.getColumnQualifier(cola);
        nk = new Key(cola, EMPTY_TEXT, eB);
      } else {
        final byte[] bcol = eA.getColumnQualifierData().toArray();
        if( bcol.length != 4 )
          throw new RuntimeException("expected an int with array length 4 but got col qualifier length "+bcol.length+" in "+ Arrays.toString(bcol));
        newRow[0] = ReverseByteTable[bcol[3] & 0xFF];
        System.arraycopy(bcol, 0, newRow, 1, 4);
        nk = new Key(newRow, EMPTY_BYTES, eB.getBytes());
      }

//      long a = LEX.decode(eA.getValue().get());
//      long b = LEX.decode(eB.getValue().get());

//      double nd = Math.min(((double)a)/da, ((double)b)/db); /// (da * db); // full calc is 1 - 2*
//      Value nv = new Value(LEXDOUBLE.encode(nd)); //new Value(Double.toString(nd).getBytes(UTF_8));

//      log.info("LowerTiJoin emits "+nk.toStringNoTime()+" value "+VALUE_TWO);

//      System.out.println(Arrays.toString(LEX.decode(eA.getRowData().toArray()).toString().getBytes())+": "+Arrays.toString(LEX.decode(cola.getBytes()).toString().getBytes())+" xx "+Arrays.toString(LEX.decode(eB.getBytes()).toString().getBytes()));

      nextEntry = new AbstractMap.SimpleImmutableEntry<>(nk, magic ? VALUE_TWO_MAGIC : VALUE_TWO); // need to copy?
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }


  private static final byte[] ReverseByteTable = new byte[] {
      (byte)0x00, (byte)0x80, (byte)0x40, (byte)0xc0, (byte)0x20, (byte)0xa0, (byte)0x60, (byte)0xe0,
      (byte)0x10, (byte)0x90, (byte)0x50, (byte)0xd0, (byte)0x30, (byte)0xb0, (byte)0x70, (byte)0xf0,
      (byte)0x08, (byte)0x88, (byte)0x48, (byte)0xc8, (byte)0x28, (byte)0xa8, (byte)0x68, (byte)0xe8,
      (byte)0x18, (byte)0x98, (byte)0x58, (byte)0xd8, (byte)0x38, (byte)0xb8, (byte)0x78, (byte)0xf8,
      (byte)0x04, (byte)0x84, (byte)0x44, (byte)0xc4, (byte)0x24, (byte)0xa4, (byte)0x64, (byte)0xe4,
      (byte)0x14, (byte)0x94, (byte)0x54, (byte)0xd4, (byte)0x34, (byte)0xb4, (byte)0x74, (byte)0xf4,
      (byte)0x0c, (byte)0x8c, (byte)0x4c, (byte)0xcc, (byte)0x2c, (byte)0xac, (byte)0x6c, (byte)0xec,
      (byte)0x1c, (byte)0x9c, (byte)0x5c, (byte)0xdc, (byte)0x3c, (byte)0xbc, (byte)0x7c, (byte)0xfc,
      (byte)0x02, (byte)0x82, (byte)0x42, (byte)0xc2, (byte)0x22, (byte)0xa2, (byte)0x62, (byte)0xe2,
      (byte)0x12, (byte)0x92, (byte)0x52, (byte)0xd2, (byte)0x32, (byte)0xb2, (byte)0x72, (byte)0xf2,
      (byte)0x0a, (byte)0x8a, (byte)0x4a, (byte)0xca, (byte)0x2a, (byte)0xaa, (byte)0x6a, (byte)0xea,
      (byte)0x1a, (byte)0x9a, (byte)0x5a, (byte)0xda, (byte)0x3a, (byte)0xba, (byte)0x7a, (byte)0xfa,
      (byte)0x06, (byte)0x86, (byte)0x46, (byte)0xc6, (byte)0x26, (byte)0xa6, (byte)0x66, (byte)0xe6,
      (byte)0x16, (byte)0x96, (byte)0x56, (byte)0xd6, (byte)0x36, (byte)0xb6, (byte)0x76, (byte)0xf6,
      (byte)0x0e, (byte)0x8e, (byte)0x4e, (byte)0xce, (byte)0x2e, (byte)0xae, (byte)0x6e, (byte)0xee,
      (byte)0x1e, (byte)0x9e, (byte)0x5e, (byte)0xde, (byte)0x3e, (byte)0xbe, (byte)0x7e, (byte)0xfe,
      (byte)0x01, (byte)0x81, (byte)0x41, (byte)0xc1, (byte)0x21, (byte)0xa1, (byte)0x61, (byte)0xe1,
      (byte)0x11, (byte)0x91, (byte)0x51, (byte)0xd1, (byte)0x31, (byte)0xb1, (byte)0x71, (byte)0xf1,
      (byte)0x09, (byte)0x89, (byte)0x49, (byte)0xc9, (byte)0x29, (byte)0xa9, (byte)0x69, (byte)0xe9,
      (byte)0x19, (byte)0x99, (byte)0x59, (byte)0xd9, (byte)0x39, (byte)0xb9, (byte)0x79, (byte)0xf9,
      (byte)0x05, (byte)0x85, (byte)0x45, (byte)0xc5, (byte)0x25, (byte)0xa5, (byte)0x65, (byte)0xe5,
      (byte)0x15, (byte)0x95, (byte)0x55, (byte)0xd5, (byte)0x35, (byte)0xb5, (byte)0x75, (byte)0xf5,
      (byte)0x0d, (byte)0x8d, (byte)0x4d, (byte)0xcd, (byte)0x2d, (byte)0xad, (byte)0x6d, (byte)0xed,
      (byte)0x1d, (byte)0x9d, (byte)0x5d, (byte)0xdd, (byte)0x3d, (byte)0xbd, (byte)0x7d, (byte)0xfd,
      (byte)0x03, (byte)0x83, (byte)0x43, (byte)0xc3, (byte)0x23, (byte)0xa3, (byte)0x63, (byte)0xe3,
      (byte)0x13, (byte)0x93, (byte)0x53, (byte)0xd3, (byte)0x33, (byte)0xb3, (byte)0x73, (byte)0xf3,
      (byte)0x0b, (byte)0x8b, (byte)0x4b, (byte)0xcb, (byte)0x2b, (byte)0xab, (byte)0x6b, (byte)0xeb,
      (byte)0x1b, (byte)0x9b, (byte)0x5b, (byte)0xdb, (byte)0x3b, (byte)0xbb, (byte)0x7b, (byte)0xfb,
      (byte)0x07, (byte)0x87, (byte)0x47, (byte)0xc7, (byte)0x27, (byte)0xa7, (byte)0x67, (byte)0xe7,
      (byte)0x17, (byte)0x97, (byte)0x57, (byte)0xd7, (byte)0x37, (byte)0xb7, (byte)0x77, (byte)0xf7,
      (byte)0x0f, (byte)0x8f, (byte)0x4f, (byte)0xcf, (byte)0x2f, (byte)0xaf, (byte)0x6f, (byte)0xef,
      (byte)0x1f, (byte)0x9f, (byte)0x5f, (byte)0xdf, (byte)0x3f, (byte)0xbf, (byte)0x7f, (byte)0xff
  };

}
