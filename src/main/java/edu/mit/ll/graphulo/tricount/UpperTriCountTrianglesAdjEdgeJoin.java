package edu.mit.ll.graphulo.tricount;

import edu.mit.ll.graphulo.rowmult.RowMultiplyOp;
import edu.mit.ll.graphulo.util.PeekingIterator1;
import edu.mit.ll.graphulo.util.SKVIRowIteratorNoValues;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.WritableComparator;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_BYTES;
import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_VALUE;

/**
 * TableMult: (k,cq,"") * (k,cq',"") = (cq,cq',"")
 * only when cq < cq' in the first four bytes.
 */
public final class UpperTriCountTrianglesAdjEdgeJoin implements RowMultiplyOp {

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
  }

  private static final class FourByteComparator implements Comparator<byte[]> {
    @Override
    public int compare(byte[] o1, byte[] o2) {
      return WritableComparator.compareBytes(o1, 0, 4, o2, 0, 4);
    }
  }
  private static final Comparator<byte[]> FOUR_COMPARE = new FourByteComparator();

  private static List<byte[]> readRowColumnsNoValuesToBytes(SortedKeyValueIterator<Key, Value> skvi) throws IOException {
    if (!skvi.hasTop())
      throw new IllegalStateException(skvi + " should hasTop()");
    final Key thisRow = skvi.getTopKey();
    final List<byte[]> map = new ArrayList<>();
    do {
      map.add(skvi.getTopKey().getColumnQualifierData().toArray());
      skvi.next();
    } while (skvi.hasTop() && skvi.getTopKey().equals(thisRow, PartialKey.ROW));

    Collections.sort(map, FOUR_COMPARE);
    return map;
  }

  @Override
  public Iterator<Map.Entry<Key, Value>> multiplyRow(
      SortedKeyValueIterator<Key, Value> skviA, SortedKeyValueIterator<Key, Value> skviB) throws IOException {
    assert skviA != null && skviB != null;

    final PeekingIterator1<Key> itAonce = new PeekingIterator1<>(new SKVIRowIteratorNoValues(skviA));
    final List<byte[]> BrowMap = readRowColumnsNoValuesToBytes(skviB);

//    final List<Key> list = new ArrayList<>();
//    final List<String> sl = new ArrayList<>(), sl2 = new ArrayList<>();
//    Key k = null;
//    while (itAonce.hasNext()) {
//      k = itAonce.next();
//      list.add(k);
////      sl.add(Arrays.toString(k.getColumnQualifierData().toArray()));
//      sl.add(""+FixedIntegerLexicoder.INSTANCE.decode(k.getColumnQualifierData().toArray(), 0, 4));
//    }
//    for (byte[] b : BrowMap) {
//      sl2.add(FixedIntegerLexicoder.INSTANCE.decode(b,0,4)+"-"+FixedIntegerLexicoder.INSTANCE.decode(b,4,4));
//    }
//    if( k != null ) {
//      System.out.println( FixedIntegerLexicoder.INSTANCE.decode(k.getRowData().toArray(), 0, 4)+" "+sl+"  "+ sl2);
//    }

//    return new UpperTriTwoIterator(new PeekingIterator1<>(list.iterator()), BrowMap);
    return new UpperTriTwoIterator(itAonce, BrowMap);
  }



  private final class UpperTriTwoIterator implements Iterator<Map.Entry<Key, Value>> {

    private final List<byte[]> BrowMap;
    private final PeekingIterator1<Key> itAonce;
    private Iterator<byte[]> itBreset;

    private Map.Entry<Key, Value> nextEntry;

    private Iterator<byte[]> getItBreset() {
      byte[] afterThis = itAonce.peek().getColumnQualifierData().toArray();
      int idx = Collections.binarySearch(BrowMap, afterThis, FOUR_COMPARE);

//      {
//        final List<String> sl2 = new ArrayList<>();
//        for (byte[] b : BrowMap) {
//          sl2.add(FixedIntegerLexicoder.INSTANCE.decode(b, 0, 4) + "-" + FixedIntegerLexicoder.INSTANCE.decode(b, 4, 4));
//        }
//        System.out.println("pre: " + FixedIntegerLexicoder.INSTANCE.decode(afterThis, 0, 4) + sl2 + "idx = "+idx);
//      }

      if( idx >= 0 ) {
        do {
          idx++;
        } while ( idx < BrowMap.size() && FOUR_COMPARE.compare(afterThis, BrowMap.get(idx)) == 0 );

        final List<byte[]> l = BrowMap.subList(idx, BrowMap.size());
//        {
//          final List<String> sl2 = new ArrayList<>();
//          for (byte[] b : l) {
//            sl2.add(FixedIntegerLexicoder.INSTANCE.decode(b, 0, 4) + "-" + FixedIntegerLexicoder.INSTANCE.decode(b, 4, 4));
//          }
//          System.out.println("hit: " + sl2);
//        }
        return l.iterator();
      }
      else {
        final List<byte[]> l = BrowMap.subList(-(idx+1), BrowMap.size());
//        {
//          final List<String> sl2 = new ArrayList<>();
//          for (byte[] b : l) {
//            sl2.add(FixedIntegerLexicoder.INSTANCE.decode(b, 0, 4) + "-" + FixedIntegerLexicoder.INSTANCE.decode(b, 4, 4));
//          }
//          System.out.println("nop: " + sl2);
//        }
        return l.iterator();
      }
    }

    UpperTriTwoIterator(final PeekingIterator1<Key> itAonce, final List<byte[]> mapBreset) {
      BrowMap = mapBreset;
      this.itAonce = itAonce;
      if (!itAonce.hasNext()) {
        nextEntry = null;
        return;
      }
      this.itBreset = getItBreset();//  .tailSet(getTextAfterColumn()).iterator();
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


    private void prepNext() {
      final Key eA;
      final byte[] eB = itBreset.next();
      if (!itBreset.hasNext()) {
        eA = itAonce.next();    // advance itA
        if (itAonce.hasNext())  // STOP if no more itA
          this.itBreset = getItBreset();
      } else
        eA = itAonce.peek();

      final byte[] newRow = eA.getColumnQualifierData().toArray();
      final Key nk = new Key(newRow, EMPTY_BYTES, eB);

//      {
//        System.out.println("GOT: " + FixedIntegerLexicoder.INSTANCE.decode(newRow, 0, 4) + " , " + FixedIntegerLexicoder.INSTANCE.decode(eB, 0, 4) + "-" + FixedIntegerLexicoder.INSTANCE.decode(eB, 4, 4));
//      }

      nextEntry = new AbstractMap.SimpleImmutableEntry<>(nk, EMPTY_VALUE); // need to copy?
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
