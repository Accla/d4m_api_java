package edu.mit.ll.graphulo.skvi;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.simplemult.KeyTwoScalar;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.collections4.MapIterator;
import org.apache.commons.collections4.map.AbstractHashedMap;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.collections4.map.LRUMap;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used to facilitate opportunistic pre-summing.
 * Partial products in a TableMult emit out of order.  It would be ideal if we could grab the partial products
 * with the same Key and, instead of sending them individually through a BatchWriter, pre-sum them
 * so that we only send a single Value for each unique Key.
 * <p>
 * Do not use when results stream from the source iterator <b>in order</b>.
 * Better to instantiate the combiner directly in that case.
 * Do use when results stream from the source iterator <b>out of order</b>.
 * This provides a chance at doing pre-summing that depends on the cache capacity,
 * sparsity patterns and the number of entries the source iterator emits.
 * <p>
 * There is an option to use a regular map in place of an LRU map, which may improve performance.
 */
public class LruCacheIterator implements SortedKeyValueIterator<Key,Value> {
  private static final Logger log = LoggerFactory.getLogger(LruCacheIterator.class);


  public static final String CAPACITY = "capacity", COMBINER="combiner",
    COMBINER_OPT_PREFIX = COMBINER+".opt.",
    USE_REGULAR_MAP = "useRegularMap";


  /** Pass columns as null or empty to combine on all columns. */
  public static IteratorSetting combinerSetting(int priority, List<IteratorSetting.Column> columns,
                                                int capacity, Class<? extends Combiner> combiner,
                                                Map<String, String> combinerOpts) {
    return combinerSetting(priority, columns, capacity, combiner, combinerOpts, false);
  }

  /** Pass columns as null or empty to combine on all columns. */
  public static IteratorSetting combinerSetting(int priority, List<IteratorSetting.Column> columns,
                                                int capacity, Class<? extends Combiner> combiner,
                                                Map<String,String> combinerOpts,
                                                boolean useRegularMap) {
    IteratorSetting itset = new IteratorSetting(priority, LruCacheIterator.class);
    if (columns == null || columns.isEmpty())
      Combiner.setCombineAllColumns(itset, true);
    else
      Combiner.setColumns(itset, columns);
    itset.addOption(COMBINER, combiner.getName());
    Preconditions.checkArgument(capacity > 0, "To use the LruCacheIterator, specify a positive capacity instead of "+capacity);
    itset.addOption(CAPACITY, Integer.toString(capacity));
    if (combinerOpts != null)
      for (Map.Entry<String, String> entry : combinerOpts.entrySet())
        itset.addOption(COMBINER_OPT_PREFIX + entry.getKey(), entry.getValue());
    if (useRegularMap)
      itset.addOption(USE_REGULAR_MAP, Boolean.TRUE.toString());
    return itset;
  }


  private Map<String,String> initOptions;
  private SortedKeyValueIterator<Key,Value> source;
  private KeyTwoScalar combiner;
//  private int size = 0, capacity = 1000;
  private AbstractHashedMap<Key, Value> cache;
  private Key emitKey = null;
  private Value emitValue = null;

  private int capacity = 1000;
  private long ppIn = 0, ppOut = 0;
  private boolean hitFullCache = false;
  private boolean urm = false;

  private Map<String,String> parseOptions(Map<String, String> options) {

    Map<String,String> combinerOpts = new HashMap<>();
    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
      if (optionKey.startsWith(COMBINER_OPT_PREFIX)) {
        combinerOpts.put(optionKey.substring(COMBINER_OPT_PREFIX.length()), optionValue);
      } else {
        switch (optionKey) {
          case COMBINER:
            Combiner combiner = GraphuloUtil.subclassNewInstance(optionValue, Combiner.class);
            if (combiner instanceof KeyTwoScalar)
              this.combiner = (KeyTwoScalar) combiner;
            else
              this.combiner = KeyTwoScalar.toKeyTwoScalar(combiner);
            break;
          case CAPACITY:
            capacity = Integer.parseInt(optionValue);
            break;
          case USE_REGULAR_MAP:
            urm = Boolean.parseBoolean(optionValue);
            break;
          default:
            log.warn("Unrecognized option: " + optionEntry);
        }
      }
    }
    cache = urm ? new MapKV(capacity) : new LruMapKV(capacity);
    return combinerOpts;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
    this.initOptions = new HashMap<>(options);
    Map<String, String> combinerOpts = parseOptions(options);
    combiner.init(combinerOpts, env);
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    LruCacheIterator copy = new LruCacheIterator();
    try {
      copy.init(source.deepCopy(env), initOptions, env);
    } catch (IOException e) {
      log.error("problem creating new instance of " + combiner+" from options "+initOptions, e);
      throw new RuntimeException(e);
    }
    return copy;
  }


  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    log.debug("Resetting cache. cap="+capacity);
    cache.clear();
    ppIn = ppOut = 0;
    hitFullCache = false;
    source.seek(range, columnFamilies, inclusive);
    prepareNext();
  }

  private org.apache.commons.collections4.MapIterator<Key, Value> iterCache;

  private void prepareNext() throws IOException {
    while (cache.size() < capacity && source.hasTop()) {
      putCache(source.getTopKey(), source.getTopValue());
      source.next();
    }
    emitKey = null;
    emitValue = null;
    do {
      if (source.hasTop()) {
        hitFullCache = true;
        Key k = source.getTopKey();
        Value v = cache.get(k);
        boolean isNewVal = v == null;
        v = isNewVal ? source.getTopValue() : combiner.multiply(k, v, source.getTopValue());
        if (isNewVal)
          iterCache = removeFromMapToEmit(null);
        ppIn++;
        cache.put(k, v);
        source.next();
      } else {
        if (cache.isEmpty()) {
          log.info(String.format("Performance: cap=%5d hitFullCache=%b in=%6d out=%6d diff=%6d -> %2d%% less",
              capacity, hitFullCache, ppIn, ppOut, ppIn - ppOut, (int)(100*((float)(ppIn-ppOut))/ppIn)));
//          System.out.printf(": cap=%5d hitFullCache=%b in=%6d out=%6d diff=%6d -> %2d%% less%n",
//              cache.maxSize(), hitFullCache, ppIn, ppOut, ppIn - ppOut, (int)(100*((float)(ppIn-ppOut))/ppIn));
          return;
        }
        iterCache = removeFromMapToEmit(iterCache);
      }
    } while (emitValue == null);
    ppOut++;
  }

  private MapIterator<Key, Value> removeFromMapToEmit(MapIterator<Key, Value> iterCache) {
    if (urm) {
      if (iterCache == null)
        iterCache = cache.mapIterator();
      iterCache.next();
      emitKey = iterCache.getKey();
      emitValue = iterCache.getValue();
      iterCache.remove();
      return iterCache;
    } else {
      emitKey = ((LRUMap<Key,Value>)cache).firstKey();       // LRU
      emitValue = cache.remove(emitKey);
      return null;
    }
  }

  private void putCache(Key topKey, Value topValue) {
    Value valueInCache = cache.get(topKey);
    valueInCache = valueInCache == null ? topValue : combiner.multiply(topKey, valueInCache, topValue);
    ppIn++;
    cache.put(topKey, valueInCache);
  }


  @Override
  public Key getTopKey() {
    return emitKey;
  }

  @Override
  public Value getTopValue() {
    return emitValue;
  }

  @Override
  public boolean hasTop() {
    return emitKey != null;
  }

  @Override
  public void next() throws IOException {
    prepareNext();
  }


  private static class LruMapKV extends LRUMap<Key,Value> {
    private static final long serialVersionUID = 1;

    LruMapKV(int capacity) {
      super(capacity);
    }

//    /**
//     * Only store up to colvis internally.
//     */
//    @Override
//    protected Object convertKey(Object key) {
//      return key == null ? NULL : GraphuloUtil.keyCopy((Key)key, PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
//    }

    /**
     * Only compare up to colvis.
     */
    @Override
    protected boolean isEqualKey(Object key1, Object key2) {
      return key1 == key2 || ((Key)key1).equals((Key)key2, PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
    }
  }

  private static class MapKV extends HashedMap<Key,Value> {
    MapKV(int capacity) {
      super(capacity, 1);
    }

    /**
     * Only compare up to colvis.
     */
    @Override
    protected boolean isEqualKey(Object key1, Object key2) {
      return key1 == key2 || ((Key)key1).equals((Key)key2, PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
    }
  }
}
