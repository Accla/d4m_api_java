package edu.mit.ll.graphulo.simplemult;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.apply.ApplyOp;
import edu.mit.ll.graphulo.util.DoubletonIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A simple abstract class for an operation acting on two scalars that can also see the acting Key.
 * This includes Combiner-style sum and unary Apply.
 * <p>
 * The requirement is that this operation logic must return zero or one entry per multiply
 * and depend only on Values and Keys.  More advanced operations should not use this class.
 * Simpler operations that do not depend on the Key can use {@link SimpleTwoScalar}.
 * Certain methods are marked as final to prevent misuse/confusion.
 * Please take care that your operator is commutative for the Combiner usage.
 * Your operator should always be associative.
 * <p>
 *   Can be used for unary Apply if passed a fixed Value operand
 *   on the left or right of the multiplication inside iterator options.
 *   Defaults to the left side of the multiplication; pass {@value #REVERSE} if the right is desired.
 */
public abstract class KeyTwoScalar extends Combiner implements ApplyOp {
  private static final Logger log = LoggerFactory.getLogger(KeyTwoScalar.class);

  //////////////////////////////////////////////////////////////////////////////////
  /** Implements simple multiply logic with Key. Returning null means no entry is emitted. */
  public abstract Value multiply(Key key, Value v1, Value v2);
  //////////////////////////////////////////////////////////////////////////////////

  public static final String REVERSE = "simpleReverse", FIXED_VALUE = "simpleFixedValue";

  /** For use in one-element applies. Subclasses must complete this method,
   * because we don't know the name of the concrete class, because this method is static.
   * Defaults to setting constant on the left if reverse is false. */
  protected static IteratorSetting addOptionsToIteratorSetting(IteratorSetting itset, boolean reverse, Value fixedValue) {
//    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
//    itset.addOption(ApplyIterator.APPLYOP, this.getClass().getName());
    itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + FIXED_VALUE, new String(fixedValue.get(), StandardCharsets.UTF_8));
    itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + REVERSE, Boolean.toString(reverse));
    return itset;
  }


  protected boolean reverse = false;
  private Value fixedValue = null;

  /** Used in ApplyIterator.
   * This is different than {@link #init(SortedKeyValueIterator, Map, IteratorEnvironment)},
   * when this class is used as a Combiner. */
  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String k = entry.getKey(), v = entry.getValue();
      switch (k) {
        case FIXED_VALUE:
          fixedValue = new Value(v.getBytes(StandardCharsets.UTF_8));
          break;
        case REVERSE:
          reverse = Boolean.parseBoolean(v);
          break;
        default:
          log.warn("Unrecognized option: "+k+" -> "+v);
          break;
      }
    }
  }

  /** The Combiner SKVI init. Calls the ApplyOp/MultiplyOp/EWiseOp init from here.
   * Marked final for safety. */
  @Override
  public final void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    Map<String,String> notCombinerOpts = new HashMap<>(), combinerOpts = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String k = entry.getKey(), v = entry.getValue();
      switch (k) {
        case FIXED_VALUE:
          log.warn("KeyTwoScalar ignores fixed values when used as a Combiner");
          break;
        case ALL_OPTION:
        case COLUMNS_OPTION:
          combinerOpts.put(k,v);
          break;
        default:
          notCombinerOpts.put(k, v);
          break;
      }
    }
    init(notCombinerOpts, env);             // Call the ApplyOp init.
    super.init(source, combinerOpts, env);  // Setup Combiner parent.
  }

  /** Applies {@link #multiply(Key,Value, Value)} to every pair of consecutive Values with matching
   * row, column family, column qualifier, and column visibility. No hard guarantees on order of operations;
   * makes a best effort to pass the Value that sorts before on the left and the Value that sorts after on the right.
   * Flipped if reversed==true.
   * */
  @Override
  public final Value reduce(Key key, Iterator<Value> iter) {
    if (!iter.hasNext())
      return null;
    Value vPrev = iter.next();
    while (iter.hasNext())
      vPrev = vPrev == null ? iter.next() :
          (reverse ? multiply(key, iter.next(), vPrev) : multiply(key, vPrev, iter.next()));
    return vPrev;
  }

  /** Overridden to return false when the result of reduce is null. Marked final for safety. */
  @Override
  public final boolean hasTop() {
    return super.hasTop() && super.getTopValue() != null;
  }

  @Override
  public KeyTwoScalar deepCopy(IteratorEnvironment env) {
    KeyTwoScalar copy = (KeyTwoScalar) super.deepCopy(env);
    copy.reverse = reverse;
    copy.fixedValue = fixedValue == null ? null : new Value(fixedValue);
    return copy;
  }

  /** Marked final for safety. */
  @Override
  public final void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    seekApplyOp(range, columnFamilies, inclusive);
    super.seek(range, columnFamilies, inclusive);
  }

  /** Feel free to override this method to obtain additional information from seek calls, for the case of ApplyOps. */
  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
  }

  @Override
  public final Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v1) {
    Value v2 = reverse ? multiply(k, v1, fixedValue) : multiply(k, fixedValue, v1);
    return v2 == null ? Collections.<Map.Entry<Key,Value>>emptyIterator() : Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, v2));
  }

  /** Marked final for safety. */
  @Override
  public final Key getTopKey() {
    return super.getTopKey();
  }

  /** Marked final for safety. */
  @Override
  public final Value getTopValue() {
    return super.getTopValue();
  }

  /** Marked final for safety. */
  @Override
  public final void next() throws IOException {
    super.next();
  }

  /** Marked final for safety. */
  @Override
  protected final void setSource(SortedKeyValueIterator<Key, Value> source) {
    super.setSource(source);
  }

  /** Marked final for safety. */
  @Override
  protected final SortedKeyValueIterator<Key, Value> getSource() {
    return super.getSource();
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("KeyTwoScalar");
    io.setDescription("A Combiner that operates on every pair of entries matching row through column visibility");
    return io;
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public boolean validateOptions(Map<String, String> options) {
    if (options.containsKey(REVERSE))
      Boolean.parseBoolean(options.get(REVERSE));
    if (options.containsKey(FIXED_VALUE))
      log.warn("KeyTwoScalar ignores fixed values when used as a Combiner");
    return super.validateOptions(options);
  }




  /** Adapts a Combiner to a KeyTwoScalar operation. */
  public static KeyTwoScalar toKeyTwoScalar(final Combiner combiner) {
    return new KeyTwoScalar() {
      DoubletonIterator<Value> iter = new DoubletonIterator<>();

      Map<String, String> origOptions;

      @Override
      public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
        origOptions = options;
        super.init(options, env);
        combiner.init(null, options, env);
      }

      @Override
      public Value multiply(Key key, Value Aval, Value Bval) {
        iter.reuseAndReset(Aval, Bval);
        return combiner.reduce(key, iter);
      }

      @Override
      public KeyTwoScalar deepCopy(IteratorEnvironment env) {
        Combiner c = (Combiner) combiner.deepCopy(env);
        KeyTwoScalar k = KeyTwoScalar.toKeyTwoScalar(c);
        try {
          k.init(origOptions, env);
        } catch (IOException e) {
          throw new RuntimeException("",e);
        }
        return k;
      }
    };
  }

}
