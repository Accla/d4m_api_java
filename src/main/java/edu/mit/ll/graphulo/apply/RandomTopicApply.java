package edu.mit.ll.graphulo.apply;

import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_TEXT;

/**
 * For every entry passed in, emits <tt>knum</tt> entries,
 * each with the same row and with column qualifiers 1, 2, 3, ..., knum.
 * Values are random doubles between 0 and 1.
 */
public class RandomTopicApply implements ApplyOp {
  private static final Logger log = LoggerFactory.getLogger(RandomTopicApply.class);

  public static final String KNUM = "knum";

  public static IteratorSetting iteratorSetting(int priority, int knum) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, RandomTopicApply.class.getName());
    itset.addOption(ApplyIterator.APPLYOP+ GraphuloUtil.OPT_SUFFIX+KNUM, Integer.toString(knum));
    return itset;
  }

  private int knum;

  private void parseOptions(Map<String,String> options) {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String v = entry.getValue();
      switch (entry.getKey()) {
        case KNUM:
          knum = Integer.parseInt(v);
          break;
        default:
          log.warn("Unrecognized option: " + entry);
          break;
      }
    }
    if (knum <= 0)
      throw new IllegalArgumentException("Bad knum: "+knum);
  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    parseOptions(options);
  }

  private static final Random rand = new Random();

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) {
    Text row = k.getRow();
    SortedMap<Key,Value> map = new TreeMap<>();
    for (int i = 1; i <= knum; i++) {
      Key knew = new Key(row, EMPTY_TEXT, new Text(Integer.toString(i)), System.currentTimeMillis());

      Value vnew = new Value(Double.toString(
          Math.abs(rand.nextGaussian()))        // absolute value of random normal
          .getBytes(StandardCharsets.UTF_8));
      map.put(knew, vnew);
    }
    return map.entrySet().iterator();
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

  }

}
