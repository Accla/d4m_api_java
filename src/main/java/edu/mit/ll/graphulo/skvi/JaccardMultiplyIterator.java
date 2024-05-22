package edu.mit.ll.graphulo.skvi;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.rowmult.GeneralCartesianIterator;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.mit.ll.graphulo.util.PeekingIterator1;
import edu.mit.ll.graphulo.util.SKVIRowIterator;

import javax.annotation.Nullable;

/**
 * Core Jaccard Multiply Iterator: LTL, LTU, UTU @ no diagonal.
 * This version works within a OneTable. No TwoTable required.
 */
public class JaccardMultiplyIterator implements SortedKeyValueIterator<Key,Value> {
  private static final Logger log = LoggerFactory.getLogger(JaccardMultiplyIterator.class);

  public static IteratorSetting iteratorSetting(int priority) {
    return new IteratorSetting(priority, JaccardMultiplyIterator.class);
  }


//  private Map<String,String> initOptions;
  private SortedKeyValueIterator<Key,Value> source;
  private SKVIRowIterator sourceRows;
  private PeekingIterator1<Map.Entry<Key,Value>> retIter = PeekingIterator1.emptyIterator();

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    JaccardMultiplyIterator copy = new JaccardMultiplyIterator();
    try {
      copy.init(source.deepCopy(env), new HashMap<String, String>(), env);
    } catch (IOException e) {
//      log.error("problem creating new instance of TopColPerRowIterator from options "+initOptions, e);
      throw new RuntimeException(e);
    }
    return copy;
  }


  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
    sourceRows = new SKVIRowIterator(source);
    prepareNext();
  }

  private static final GeneralCartesianIterator.MCondition<Text,Long> lessCondition = new GeneralCartesianIterator.MCondition<Text, Long>() {
    @Override
    public boolean shouldMultiply(Map.Entry<Text, Long> eA, Map.Entry<Text, Long> eB) {
      return eA.getKey().compareTo(eB.getKey()) < 0;
    }
  };

  private static final Text EMPTY = new Text();
  private static final Value ONE = new Value("1".getBytes(StandardCharsets.UTF_8));

  private static final GeneralCartesianIterator.Multiply<Text,Long,Key,Value> multiply = new GeneralCartesianIterator.Multiply<Text, Long, Key, Value>() {
    @Override
    public Iterator<? extends Map.Entry<Key, Value>> multiply(Map.Entry<Text, Long> eA, Map.Entry<Text, Long> eB) {
      Key k = new Key(eA.getKey(), EMPTY, eB.getKey());
      Value v = ONE; //new Value(Long.toString(eA.getValue()*eB.getValue()).getBytes(StandardCharsets.UTF_8));
      return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, v));
    }
  };

//  private static final GeneralCartesianIterator.Multiply<Text,Long,Key,Value> multiplyPrint = new GeneralCartesianIterator.Multiply<Text, Long, Key, Value>() {
//    @Override
//    public Iterator<? extends Map.Entry<Key, Value>> multiply(Map.Entry<Text, Long> eA, Map.Entry<Text, Long> eB) {
//      Key k = new Key(eA.getKey(), EMPTY, eB.getKey());
//      Value v = ONE; //new Value(Long.toString(eA.getValue()*eB.getValue()).getBytes(StandardCharsets.UTF_8));
//      return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, v));
//    }
//  };

  private void prepareNext() throws IOException {
    while (!retIter.hasNext() && sourceRows.hasNext()) {
      ImmutableSortedMap<Text, Long> less, great;
      {
        Text row = null;
        ImmutableSortedMap.Builder<Text, Long> bless = ImmutableSortedMap.naturalOrder();
        boolean nowEqual = false, nowGreat = false;
        ImmutableSortedMap.Builder<Text, Long> bgreat = ImmutableSortedMap.naturalOrder();

        while (sourceRows.hasNext()) {
          Map.Entry<Key, Value> next = sourceRows.next();
          if (row == null)
            row = next.getKey().getRow();

//        int c = next.getKey().compareColumnQualifier(row);
//        if (c < 0) {
//          bless.put(next.getKey().getColumnQualifier(), 1L);
//        } else if (c > 0) {
//          bgreat.put(next.getKey().getColumnQualifier(), 1L);
//        }

          if (!nowEqual && next.getKey().compareColumnQualifier(row) < 0) {
            bless.put(next.getKey().getColumnQualifier(), 1L);//Long.parseLong(next.getValue().toString()));
            continue;
          } else
            nowEqual = true;

          if (!nowGreat && next.getKey().compareColumnQualifier(row) == 0)
            continue;
          else
            nowGreat = true;

          bgreat.put(next.getKey().getColumnQualifier(), 1L); //Long.parseLong(next.getValue().toString()));
        }
        less = bless.build();
        great = bgreat.build();
//        if (log.isInfoEnabled())
//          log.info("row " + row + " less " + less + " great " + great);
      }

      retIter = new PeekingIterator1<Map.Entry<Key, Value>>(
          Iterators.concat(
              // todo - these could be further optimized, since we know the inputs are always 1. Multiply just returns value 1. Don't need to track value from input.
              new GeneralCartesianIterator<Text, Long, Key, Value>(less.entrySet().iterator(), less, multiply, false, lessCondition),
              new GeneralCartesianIterator<Text, Long, Key, Value>(less.entrySet().iterator(), great, multiply, false, null),
              new GeneralCartesianIterator<Text, Long, Key, Value>(great.entrySet().iterator(), great, multiply, false, lessCondition)
          )
      );
      sourceRows.reuseNextRow();
    }
  }


  @Override
  public Key getTopKey() {
    return retIter.peek().getKey();
  }

  @Override
  public Value getTopValue() {
    return retIter.peek().getValue();
  }

  @Override
  public boolean hasTop() {
    return retIter.hasNext();
  }

  @Override
  public void next() throws IOException {
    retIter.next();
    if (!retIter.hasNext())
      prepareNext();
  }

}
