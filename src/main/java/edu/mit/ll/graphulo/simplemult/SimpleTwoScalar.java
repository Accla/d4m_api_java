package edu.mit.ll.graphulo.simplemult;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.ewise.EWiseOp;
import edu.mit.ll.graphulo.reducer.Reducer;
import edu.mit.ll.graphulo.rowmult.MultiplyOp;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;


/**
 * A simple abstract class for an operation acting on two scalars.
 * This includes matrix multiplication, element-wise multiplication,
 * element-wise sum, Combiner-style sum, unary Apply, and Reductions.
 * <p>
 * The requirement is that this operation logic must return zero or one entry per multiply
 * and depend only on Values, not Keys.  More advanced operations should not use this class.
 * Certain methods are marked as final to prevent misuse/confusion.
 * Please take care that your operator is commutative for the Combiner and Reduce usages.
 * Your operator should always be associative.
 * Finally, in the case of EWiseOp, the operation logic must not need to act on non-matching Values.
 * <p>
 *   Can be used for unary Apply if passed a fixed Value operand
 *   on the left or right of the multiplication inside iterator options.
 *   Defaults to the left side of the multiplication; pass {@value #REVERSE} if the right is desired.
 * <p>
 * Has a newVisibility option for new Keys created with MultiplyOp or EWiseOp usage.
 */
public abstract class SimpleTwoScalar extends KeyTwoScalar implements MultiplyOp, EWiseOp, Reducer {
  private static final Logger log = LoggerFactory.getLogger(SimpleTwoScalar.class);

  public static final String NEW_VISIBILITY ="newVisibility", USE_NEW_VISIBILITY = "useNewVisibility";
  protected boolean useNewVisibility = false;
  protected byte[] newVisibility = null;

  //////////////////////////////////////////////////////////////////////////////////
  /** Implements simple multiply logic. Returning null means no entry is emitted. */
  public abstract Value multiply(Value Aval, Value Bval);
  //////////////////////////////////////////////////////////////////////////////////

//  protected static IteratorSetting addOptionsToIteratorSetting(IteratorSetting itset, boolean reverse, Value fixedValue,
//                                                               String newVisibility) {
//    KeyTwoScalar.addOptionsToIteratorSetting(itset, reverse, fixedValue);
//    if (newVisibility != null && !newVisibility.isEmpty())
//      itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + NEW_VISIBILITY, newVisibility);
//    return itset;
//  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    Map<String,String> parentOpts = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String k = entry.getKey(), v = entry.getValue();
      switch (k) {
        case USE_NEW_VISIBILITY: useNewVisibility = Boolean.parseBoolean(v); break;
        case NEW_VISIBILITY:
          newVisibility = v.getBytes(StandardCharsets.UTF_8);
          break;
        default:
          parentOpts.put(k, v);
          break;
      }
    }
    super.init(parentOpts, env);  // Setup parent.
  }

  @Override
  public SimpleTwoScalar deepCopy(IteratorEnvironment env) {
    SimpleTwoScalar copy = (SimpleTwoScalar) super.deepCopy(env);
    copy.useNewVisibility = useNewVisibility;
    copy.newVisibility = useNewVisibility ? Arrays.copyOf(newVisibility, newVisibility.length) : null ;
    return copy;
  }

  /** For {@link MultiplyOp}. Uses {@link System#currentTimeMillis()} for the timestamp. */
  @Override
  public final Iterator<? extends Entry<Key, Value>> multiply(
      ByteSequence Mrow, ByteSequence ATcolF, ByteSequence ATcolQ, ByteSequence ATcolVis, long ATtime,
      ByteSequence BcolF, ByteSequence BcolQ, ByteSequence BcolVis, long Btime,
      Value ATval, Value Bval) {
//    System.err.println("Mrow:"+Mrow+" ATcolQ:"+ATcolQ+" BcolQ:"+BcolQ+" ATval:"+ATval+" Bval:"+Bval);
    assert ATval != null || Bval != null;
    Key k = new Key(ATcolQ.toArray(), ATcolF.toArray(),
        BcolQ.toArray(), useNewVisibility ? newVisibility : GraphuloUtil.EMPTY_BYTES);
    Value v = reverse ? multiply(Bval, ATval) : multiply(ATval, Bval);
    return v == null ? Collections.<Entry<Key,Value>>emptyIterator() : Iterators.singletonIterator(new SimpleImmutableEntry<>(k, v));
  }

  /** For {@link EWiseOp}. Uses {@link System#currentTimeMillis()} for the timestamp. */
  @Override
  public final Iterator<? extends Entry<Key, Value>> multiply(
      ByteSequence Mrow, ByteSequence McolF, ByteSequence McolQ, ByteSequence McolVis,
      long Atime, long Btime,
      Value Aval, Value Bval) {
    // Important!  Aval xor Bval could be null, if emitNoMatchA or emitNoMatchB are true in TwoTableIterator.
    // Decision is to emit the non-matching entries untouched by the operation.  This is a *SIMPLETwoScalar* operator.
    assert Aval != null || Bval != null;
    final Key k = new Key(Mrow.toArray(), McolF.toArray(),
        McolQ.toArray(), useNewVisibility ? newVisibility : McolVis.toArray());
    if (Aval == null)
      return Iterators.singletonIterator(new SimpleImmutableEntry<>(k, Bval));
    if (Bval == null)
      return Iterators.singletonIterator(new SimpleImmutableEntry<>(k, Aval));

    Value v = reverse ? multiply(Bval, Aval) : multiply(Aval, Bval);
    return v == null ? Collections.<Entry<Key,Value>>emptyIterator() : Iterators.singletonIterator(new SimpleImmutableEntry<>(k, v));
  }

  @Override
  public final Value multiply(Key key, Value v1, Value v2) {
    return multiply(v1, v2);
  }


  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("SimpleTwoScalar");
    io.setDescription("A Combiner that operates on every pair of entries matching row through column visibility, that does not need to see the Key");
//    io.addNamedOption(NEW_VISIBILITY, "Authorizations to use for new Keys created with MultiplyOp or EWiseOp usage");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    if (options.containsKey(NEW_VISIBILITY))
//      new Authorizations(options.get(NEW_VISIBILITY));
      log.warn("Combiner usage does not use newVisibility: "+options.get(NEW_VISIBILITY));
    return super.validateOptions(options);
  }

  private Value reducerV;

  @Override
  public final void reset() {
    reducerV = null;
  }

  @Override
  public final void update(Key k, Value v) {
    if (reducerV == null)
      reducerV = v == null ? null : new Value(v);
    else
      reducerV = reverse ? multiply(reducerV, v) : multiply(v, reducerV);
  }

  @Override
  public final void combine(byte[] another) {
    Value v = new Value(another);
    if (reducerV == null)
      reducerV = new Value(v);
    else
      reducerV = reverse ? multiply(reducerV, v) : multiply(v, reducerV);
  }

  @Override
  public final boolean hasTopForClient() {
    return reducerV != null;
  }

  @Override
  public final byte[] getForClient() {
    return reducerV.get();
  }

}
