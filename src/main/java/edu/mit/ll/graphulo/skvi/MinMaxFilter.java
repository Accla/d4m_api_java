package edu.mit.ll.graphulo.skvi;

import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

/**
 * Filter based on minimum and maximum Value.
 * Interprets Values as one of the {@link edu.mit.ll.graphulo.simplemult.MathTwoScalar.ScalarType} types,
 * encoded as a String.
 */
public class MinMaxFilter extends Filter {
  private static final Logger log = LoggerFactory.getLogger(MinMaxFilter.class);

  public static final String MINVALUE = "minValue", MAXVALUE = "maxValue",
      USECOLQ = "useColQ", PREFIXCOLQ = "prefixColQ";

  public static IteratorSetting iteratorSetting(int priority, MathTwoScalar.ScalarType numberType, Number min, Number max) {
    IteratorSetting itset = new IteratorSetting(priority, MinMaxFilter.class);
    if (numberType != null)
      itset.addOption(MathTwoScalar.SCALAR_TYPE, numberType.name());
    if (min != null)
      itset.addOption(MINVALUE, min.toString());
    if (max != null)
      itset.addOption(MAXVALUE, max.toString());
    return itset;
  }

  public static IteratorSetting iteratorSetting(int priority, MathTwoScalar.ScalarType numberType, Number min, Number max,
                                                boolean useColQ, String prefixColQ) {
    IteratorSetting itset = iteratorSetting(priority, numberType, min, max);
    itset.addOption(USECOLQ, Boolean.toString(useColQ));
    if (useColQ && prefixColQ != null)
      itset.addOption(PREFIXCOLQ, prefixColQ);
    return itset;
  }

  private Number minValue = 0l, maxValue = Long.MAX_VALUE;
  private MathTwoScalar.ScalarType scalarType = MathTwoScalar.ScalarType.LONG;
  private boolean useColQ = false;
  private byte[] prefixColQ;


  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.containsKey(MathTwoScalar.SCALAR_TYPE))
      scalarType = MathTwoScalar.ScalarType.valueOf(options.get(MathTwoScalar.SCALAR_TYPE));
    if (scalarType == MathTwoScalar.ScalarType.LONG_OR_DOUBLE)
      scalarType = MathTwoScalar.ScalarType.DOUBLE;
    handleOptions(options);

    log.debug("minValue="+minValue+" maxValue="+maxValue+" useColQ="+useColQ+" prefixColQ="+
        (new String(prefixColQ, StandardCharsets.UTF_8)));
  }

  private Number parseGeneric(String val) {
    switch (scalarType) {
      case LONG: return Long.parseLong(val);
      case DOUBLE: return Double.parseDouble(val);
      case BIGDECIMAL: return new BigDecimal(val);
      default: throw new IllegalArgumentException("Unrecognized type: "+scalarType);
    }
  }

  private void handleOptions(Map<String,String> options) {
    if (options.containsKey(MINVALUE)) {
      minValue = parseGeneric(options.get(MINVALUE));
    } else {
      switch (scalarType) {
        case LONG:        minValue = 0L; break;
        case DOUBLE:      minValue = 0.0d; break;
        case BIGDECIMAL:  minValue = BigDecimal.ZERO; break;
      }
    }
    if (options.containsKey(MAXVALUE)) {
      maxValue = parseGeneric(options.get(MAXVALUE));
    } else {
      switch (scalarType) {
        case LONG:        maxValue = Long.MAX_VALUE; break;
        case DOUBLE:      maxValue = Double.MAX_VALUE; break;
        case BIGDECIMAL:  maxValue = BigDecimal.valueOf(Double.MAX_VALUE); break;
      }
    }
    boolean bad = false;
    switch (scalarType) {
      case LONG:        if (maxValue.longValue() < minValue.longValue()) bad = true; break;
      case DOUBLE:      if (maxValue.doubleValue() < minValue.doubleValue()) bad = true; break;
      case BIGDECIMAL:  if (((BigDecimal)maxValue).compareTo((BigDecimal)minValue) < 0) bad = true; break;
    }
    if (bad)
      throw new IllegalArgumentException("maxValue < minValue: "+maxValue+" < "+minValue);

    if (options.containsKey(USECOLQ) && Boolean.parseBoolean(options.get(USECOLQ))) {
      useColQ = true;
      if (options.containsKey(PREFIXCOLQ))
        prefixColQ = options.get(PREFIXCOLQ).getBytes(StandardCharsets.UTF_8);
      else
        prefixColQ = new byte[0];
    } else
      prefixColQ = new byte[0];
  }

  @Override
  public boolean accept(Key k, Value v) {
    byte[] num;
    if (useColQ) {
      String s = GraphuloUtil.stringAfter(prefixColQ, k.getColumnQualifierData().toArray());
      if (s == null) {
//        log.info("REJECT NULL: "+k+" -> "+v+" ("+s+")");
        return false; // reject entries that do not match the prefix pattern
      }
      num = s.getBytes(StandardCharsets.UTF_8);
    } else
      num = v.get();

    switch (scalarType) {
      case LONG:
        long l = Long.parseLong(new String(num, StandardCharsets.UTF_8));
//    if (l >= minValue.longValue() && l <= maxValue.longValue()) // DEBUG
//      log.info("accept: "+k+" -> "+v+" ("+l+")");
//    else {
//      log.info("REJECT: "+k+" -> "+v+" ("+l+")");
//    }
        return l >= minValue.longValue() && l <= maxValue.longValue();
      case DOUBLE:
        double d = Double.parseDouble(new String(num, StandardCharsets.UTF_8));
//        if (d >= minValue.doubleValue() && d <= maxValue.doubleValue()) // DEBUG
//          log.info("accept: "+k.toStringNoTime()+" -> "+v);
//        else {
//          log.info("REJECT: "+k.toStringNoTime()+" -> "+v);
//        }
        return d >= minValue.doubleValue() && d <= maxValue.doubleValue();
      case BIGDECIMAL:
        BigDecimal b = new BigDecimal(new String(num, StandardCharsets.UTF_8));
        return b.compareTo((BigDecimal)minValue) >= 0 && b.compareTo((BigDecimal)maxValue) <= 0;
      default:
        throw new UnsupportedOperationException("ScalarType not supported: "+scalarType);
    }
  }

  @Override
  public MinMaxFilter deepCopy(IteratorEnvironment env) {
    MinMaxFilter copy = (MinMaxFilter)super.deepCopy(env);
    copy.scalarType = scalarType;
    copy.minValue = minValue;
    copy.maxValue = maxValue;
    copy.useColQ = useColQ;
    copy.prefixColQ = new byte[prefixColQ.length];
    System.arraycopy(prefixColQ, 0, copy.prefixColQ, 0, prefixColQ.length);
    return copy;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName(MinMaxFilter.class.getCanonicalName());
    io.setDescription("Filter based on Value interpreted as a Long, encoded as String");
    io.addNamedOption(MathTwoScalar.SCALAR_TYPE, "How to interpret Value encoded as String: " + Arrays.toString(MathTwoScalar.ScalarType.values()));
    io.addNamedOption(MINVALUE, "Minimum Value, default " + minValue);
    io.addNamedOption(MAXVALUE, "Maximum Value, default "+maxValue);
    io.addNamedOption(USECOLQ,  "Use Column Qualifier instead of Value? [default false]");
    io.addNamedOption(PREFIXCOLQ, "[only if useColQ] Fixed prefix before the numeric part of the column qualifier.");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    new MinMaxFilter().handleOptions(options);
    return super.validateOptions(options);
  }
}
