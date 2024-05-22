package edu.mit.ll.graphulo.simplemult;

import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder;
import org.apache.accumulo.core.iterators.ValueFormatException;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Math operations between two scalars.
 * Can be used as an ApplyOp by setting one of the sides to a constant scalar.
 * Will not emit zero entries unless keepZero is set to true.
 */
public class MathTwoScalar extends SimpleTwoScalar {
  private static final Logger log = LoggerFactory.getLogger(MathTwoScalar.class);

  public enum ScalarOp {
    PLUS, TIMES, SET_LEFT, MINUS,
    DIVIDE, POWER, MIN, MAX
  }
  public enum ScalarType /*implements Encoder*/ {
    LONG(/*new SummingCombiner.StringEncoder()*/),
    DOUBLE(/*new DoubleEncoder()*/),
    BIGDECIMAL(/*new BigDecimalCombiner.BigDecimalEncoder()*/),
    /** Parses as long if the input has a decimal point. Otherwise parses as double.
     * Returns as long if both inputs parsed as long. Otherwise returns as double. */
    LONG_OR_DOUBLE,
    LEX_LONG


    // Core Developer note: I tried to make the encoding and decoding generic,
    // but this did not play well with how Java handles enums. Someone can make this nicer.
//    private Encoder<? extends Number> encoder;
//
//    ScalarType(Encoder<? extends Number> encoder) {
//      this.encoder = encoder;
//    }
//
//    @Override
//    public byte[] encode(Object number) {
//      return encoder.encode((Number)number);
//    }
//
//    @Override
//    public Number decode(byte[] b) throws ValueFormatException {
//      return encoder.decode(b);
//    }
  }

  public static class DoubleEncoder implements Encoder<Double> {
    @Override
    public byte[] encode(Double v) {
      return v.toString().getBytes(UTF_8);
    }
    @Override
    public Double decode(byte[] b) throws ValueFormatException {
      try {
        return Double.parseDouble(new String(b, UTF_8));
      } catch (NumberFormatException nfe) {
        throw new ValueFormatException(nfe);
      }
    }
  }

//  private interface ScalarOpInterface<T> extends Encoder<T> {
//    Value doOp(Value Aval, ScalarOp op, Value Bval);
//  }

  public static final String
      SCALAR_OP = "ScalarOp",
      SCALAR_TYPE = "scalarType",
      KEEP_ZERO = "keepZero";

  /** For use as an ApplyOp.
   * Create an IteratorSetting that performs a ScalarOp on every Value it sees, parsing Values as Doubles. */
  public static IteratorSetting applyOpDouble(int priority, boolean constantOnRight, ScalarOp op, double scalar, boolean keepZero) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, MathTwoScalar.class.getName());
    for (Map.Entry<String, String> entry : optionMap(op, ScalarType.DOUBLE, null, keepZero).entrySet())
      itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + entry.getKey(), entry.getValue());
    itset = KeyTwoScalar.addOptionsToIteratorSetting(itset, constantOnRight, new Value(Double.toString(scalar).getBytes(StandardCharsets.UTF_8)));
    return itset;
  }

  /** For use as an ApplyOp.
   * Create an IteratorSetting that performs a ScalarOp on every Value it sees, parsing Values as Longs. */
  public static IteratorSetting applyOpLong(int priority, boolean constantOnRight, ScalarOp op, long scalar, boolean keepZero) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, MathTwoScalar.class.getName());
    for (Map.Entry<String, String> entry : optionMap(op, ScalarType.LONG, null, keepZero).entrySet())
      itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + entry.getKey(), entry.getValue());
    itset = KeyTwoScalar.addOptionsToIteratorSetting(itset, constantOnRight, new Value(Long.toString(scalar).getBytes(StandardCharsets.UTF_8)));
    return itset;
  }

  /** For use as an ApplyOp.
   * Create an IteratorSetting that performs a ScalarOp on every Value it sees, parsing Values as BigDecimal objects. */
  public static IteratorSetting applyOpBigDecimal(int priority, boolean constantOnRight, ScalarOp op, BigDecimal scalar, boolean keepZero) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, MathTwoScalar.class.getName());
    for (Map.Entry<String, String> entry : optionMap(op, ScalarType.BIGDECIMAL, null, keepZero).entrySet())
      itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + entry.getKey(), entry.getValue());
    itset = KeyTwoScalar.addOptionsToIteratorSetting(itset, constantOnRight, new Value(scalar.toString().getBytes(StandardCharsets.UTF_8))); // byte encoding UTF-8?
    return itset;
  }

  /** For use as an ApplyOp.
   * Create an IteratorSetting that performs a ScalarOp on every Value it sees, parsing Values as Longs. */
  public static IteratorSetting applyOpLexLong(int priority, boolean constantOnRight, ScalarOp op, long scalar, boolean keepZero) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, MathTwoScalar.class.getName());
    for (Map.Entry<String, String> entry : optionMap(op, ScalarType.LEX_LONG, null, keepZero).entrySet())
      itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + entry.getKey(), entry.getValue());
    itset = KeyTwoScalar.addOptionsToIteratorSetting(itset, constantOnRight, new Value(new LongLexicoder().encode(scalar)));
    return itset;
  }

  /** For use as a Combiner. Pass columns as null or empty to combine on all columns. */
  public static IteratorSetting combinerSetting(int priority, List<IteratorSetting.Column> columns, ScalarOp op, ScalarType type, boolean keepZero) {
    IteratorSetting itset = new IteratorSetting(priority, MathTwoScalar.class);
    if (columns == null || columns.isEmpty())
      Combiner.setCombineAllColumns(itset, true);
    else
      Combiner.setColumns(itset, columns);
    itset.addOptions(optionMap(op, type, null, keepZero)); // no newVisibility needed for Combiner usage
    return itset;
  }

  public static Map<String,String> optionMap(ScalarOp op, ScalarType type, String newVisibility, boolean keepZero) {
    Map<String,String> map = new HashMap<>();
    map.put(SCALAR_OP, op.name());
    map.put(SCALAR_TYPE, type.name());
    if (newVisibility != null && !newVisibility.isEmpty()) {
      map.put(USE_NEW_VISIBILITY, "true");
      map.put(NEW_VISIBILITY, newVisibility);
    }
    map.put(KEEP_ZERO, Boolean.toString(keepZero));
    return map;
  }


  private ScalarType scalarType = ScalarType.BIGDECIMAL; // default
  private ScalarOp scalarOp = ScalarOp.TIMES;  // default
  private boolean keepZero = false;

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env)  {
    Map<String,String> extraOpts = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String k = entry.getKey(), v = entry.getValue();
      switch (k) {
        case SCALAR_TYPE:
          scalarType = ScalarType.valueOf(options.get(SCALAR_TYPE));
          break;
        case SCALAR_OP: scalarOp = ScalarOp.valueOf(v); break;
        case KEEP_ZERO: keepZero = Boolean.parseBoolean(v); break;
        default:
          extraOpts.put(k, v);
          break;
      }
    }
    try {
      super.init(extraOpts, env);
    } catch (IOException e) {
      assert false;
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public Value multiply(Value Aval, Value Bval) {
    // local copy that may be re-assigned in the case of LONG_OR_DOUBLE
    ScalarType scalarType = this.scalarType;

    if (scalarOp == ScalarOp.SET_LEFT)
      return Aval;

    Number Anum, Bnum;
    Lexicoder<Long> lex = null;
    if (scalarType == ScalarType.LEX_LONG)
      lex = new LongLexicoder();

//    System.out.println("multiply("+Aval+","+Bval+")");
    String Astr=null, Bstr=null;
    if (scalarType != ScalarType.LEX_LONG) {
      Astr = new String(Aval.get(), StandardCharsets.UTF_8);
      Bstr = new String(Bval.get(), StandardCharsets.UTF_8);
    }

    switch(scalarType) {
      case LONG:
        Anum = Long.valueOf(Astr);
        Bnum = Long.valueOf(Bstr);
        break;
      case DOUBLE:
        Anum = Double.valueOf(Astr);
        Bnum = Double.valueOf(Bstr);
        break;
      case BIGDECIMAL:
        Anum = new BigDecimal(Astr);
        Bnum = new BigDecimal(Bstr);
        break;
      case LONG_OR_DOUBLE:
        boolean Along = Astr.indexOf('.') == -1, Blong = Bstr.indexOf('.') == -1;
        boolean ABlong = Along && Blong;
        scalarType = ABlong ? ScalarType.LONG : ScalarType.DOUBLE;
        Anum = ABlong
            ? Long.valueOf(Astr)
            : Double.valueOf(Astr);
        Bnum = ABlong
            ? Long.valueOf(Bstr)
            : Double.valueOf(Bstr);
        break;
      case LEX_LONG:
        Anum = lex.decode(Aval.get());
        Bnum = lex.decode(Bval.get());
        break;
      default: throw new AssertionError();
    }
    Number nnew;
    switch(scalarOp) {
      case PLUS:
        switch(scalarType) {
          case LONG: case LEX_LONG: nnew = Anum.longValue() + Bnum.longValue(); break;
          case DOUBLE: nnew = Anum.doubleValue() + Bnum.doubleValue(); break;
          case BIGDECIMAL: nnew = ((BigDecimal)Anum).add((BigDecimal)Bnum); break;
          default: throw new AssertionError();
        }
        break;
      case TIMES:
        switch(scalarType) {
          case LONG: case LEX_LONG: nnew = Anum.longValue() * Bnum.longValue(); break;
          case DOUBLE: nnew = Anum.doubleValue() * Bnum.doubleValue(); break;
          case BIGDECIMAL: nnew = ((BigDecimal)Anum).multiply((BigDecimal)Bnum); break;
          default: throw new AssertionError();
        }
        break;
      case MINUS:
        switch(scalarType) {
          case LONG: case LEX_LONG: nnew = Anum.longValue() - Bnum.longValue(); break;
          case DOUBLE: nnew = Anum.doubleValue() - Bnum.doubleValue(); break;
          case BIGDECIMAL: nnew = ((BigDecimal)Anum).subtract((BigDecimal) Bnum); break;
          default: throw new AssertionError();
        }
        break;
      case DIVIDE:
        switch(scalarType) {
          case LONG: case LEX_LONG: nnew = Anum.longValue() / Bnum.longValue(); break;
          case DOUBLE: nnew = Anum.doubleValue() / Bnum.doubleValue(); break;
          case BIGDECIMAL: nnew = ((BigDecimal)Anum).divide((BigDecimal) Bnum, BigDecimal.ROUND_HALF_UP); break;
          default: throw new AssertionError();
        }
        break;
      case POWER:
        switch(scalarType) {
          case LONG: case LEX_LONG: nnew = (long)Math.pow(Anum.longValue(), Bnum.longValue()); break;
          case DOUBLE: nnew = Math.pow(Anum.doubleValue(), Bnum.doubleValue()); break;
          case BIGDECIMAL: nnew = ((BigDecimal)Anum).pow(Bnum.intValue()); break;
          default: throw new AssertionError();
        }
        break;
      case MIN:
        switch(scalarType) {
          case LONG: case LEX_LONG: nnew = Math.min(Anum.longValue(), Bnum.longValue()); break;
          case DOUBLE: nnew = Math.min(Anum.doubleValue(), Bnum.doubleValue()); break;
          case BIGDECIMAL: nnew = ((BigDecimal)Anum).min((BigDecimal) Bnum); break;
          default: throw new AssertionError();
        }
        break;
      case MAX:
        switch(scalarType) {
          case LONG: case LEX_LONG: nnew = Math.max(Anum.longValue(), Bnum.longValue()); break;
          case DOUBLE: nnew = Math.max(Anum.doubleValue(), Bnum.doubleValue()); break;
          case BIGDECIMAL: nnew = ((BigDecimal)Anum).max((BigDecimal) Bnum); break;
          default: throw new AssertionError();
        }
        break;
      default:
        throw new AssertionError();
    }
    // check for zero
    if (!keepZero) {
      switch (scalarType) {
        case LONG: case LEX_LONG: if (nnew.longValue() == 0) return null; break;
        case DOUBLE: if (Double.doubleToRawLongBits(nnew.doubleValue()) == 0) return null; break;
        case BIGDECIMAL: if (nnew.equals(BigDecimal.ZERO)) return null; break;
      }
    }

    byte[] vnew;
    switch(scalarType) {
      case LONG: vnew = Long.toString(nnew.longValue()).getBytes(StandardCharsets.UTF_8); break;
      case DOUBLE: vnew = Double.toString(nnew.doubleValue()).getBytes(StandardCharsets.UTF_8); break;
      case BIGDECIMAL: vnew = nnew.toString().getBytes(StandardCharsets.UTF_8); break;
      case LEX_LONG: vnew = lex.encode(nnew.longValue()); break;
      default: throw new AssertionError();
    }
    return new Value(vnew);
  }

  @Override
  public MathTwoScalar deepCopy(IteratorEnvironment env) {
    MathTwoScalar copy = (MathTwoScalar) super.deepCopy(env);
    copy.scalarOp = scalarOp;
    copy.scalarType = scalarType;
    return copy;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("MathTwoScalar");
    io.setDescription("A Combiner that decodes and performs a math operation on every pair of entries matching row through column visibility");
    io.addNamedOption(SCALAR_OP, "Math operation; one of: " + Arrays.toString(ScalarOp.values()));
    io.addNamedOption(SCALAR_TYPE, "Decode/encode values as one of: "+Arrays.toString(ScalarType.values()));
    return io;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    if (options.containsKey(SCALAR_TYPE))
      ScalarType.valueOf(options.get(SCALAR_TYPE));
    if (options.containsKey(SCALAR_OP))
      ScalarOp.valueOf(options.get(SCALAR_OP));
    return super.validateOptions(options);
  }
}
