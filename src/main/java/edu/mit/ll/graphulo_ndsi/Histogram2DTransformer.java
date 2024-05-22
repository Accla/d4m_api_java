package edu.mit.ll.graphulo_ndsi;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.apply.ApplyOp;
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
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Bin the row and column qualifier
 */
public class Histogram2DTransformer implements ApplyOp {
  private static final Logger log = LoggerFactory.getLogger(Histogram2DTransformer.class);

  public static final String
      MIN_X = "minX", MIN_Y = "minY", BINSIZE_X = "binsizeX", BINSIZE_Y = "binsizeY";

  public static IteratorSetting iteratorSetting(int priority, long minX, long minY, double binsizeX, double binsizeY) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, Histogram2DTransformer.class.getName());
    itset.addOption(ApplyIterator.APPLYOP+GraphuloUtil.OPT_SUFFIX+MIN_X, Long.toString(minX));
    itset.addOption(ApplyIterator.APPLYOP+GraphuloUtil.OPT_SUFFIX+MIN_Y, Long.toString(minY));
    itset.addOption(ApplyIterator.APPLYOP+GraphuloUtil.OPT_SUFFIX+BINSIZE_X, Double.toString(binsizeX));
    itset.addOption(ApplyIterator.APPLYOP+GraphuloUtil.OPT_SUFFIX+BINSIZE_Y, Double.toString(binsizeY));
    return itset;
  }

  private long minX = 0, minY = 0;
  private double binsizeX = 1, binsizeY = 1;


  @Override
  public void init(Map<String, String> options, IteratorEnvironment iteratorEnvironment) throws IOException {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String v = entry.getValue();
      switch (entry.getKey()) {
        case MIN_X:
          minX = Long.parseLong(v);
          break;
        case MIN_Y:
          minY = Long.parseLong(v);
          break;
        case BINSIZE_X:
          binsizeX = Double.parseDouble(v);
          break;
        case BINSIZE_Y:
          binsizeY = Double.parseDouble(v);
          break;
        default:
          log.warn("Unrecognized option: " + entry);
          break;
      }
    }
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(Key key, Value value) {
    long row = Long.parseLong(key.getRow().toString());
    long col = Long.parseLong(key.getColumnQualifier().toString());
    assert row > minX;
    assert col > minY;
    long newRow = (long)((row-minX)/binsizeX);
    long newCol = (long)((col-minY)/binsizeY);
    Text newRowText = new Text(Long.toString(newRow));
    Text newColText = new Text(Long.toString(newCol));
    Key newKey = new Key(newRowText, key.getColumnFamily(), newColText);
    return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(newKey, value));
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> collection, boolean b) throws IOException {
  }

}
