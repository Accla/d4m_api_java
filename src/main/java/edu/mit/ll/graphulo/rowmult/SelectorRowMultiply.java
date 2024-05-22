package edu.mit.ll.graphulo.rowmult;

import edu.mit.ll.graphulo.util.SKVIRowIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * A kind of intersection row multiply. Emits rows of B.
 */
public class SelectorRowMultiply implements RowMultiplyOp {
  private static final Logger log = LoggerFactory.getLogger(SelectorRowMultiply.class);

  public static final String ASELECTSBROW="ASelectsBRow";

  /** Set false to have B select an A row. */
  private boolean ASelectsBRow = true;


  private void parseOptions(Map<String, String> options) {
    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
        switch (optionKey) {
          case ASELECTSBROW:
            ASelectsBRow = Boolean.parseBoolean(optionValue);
            break;
          default:
            log.warn("Unrecognized option: " + optionEntry);
            break;
        }
      }
  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    parseOptions(options);
  }

  @Override
  public Iterator<Map.Entry<Key,Value>> multiplyRow(SortedKeyValueIterator<Key, Value> skviA, SortedKeyValueIterator<Key, Value> skviB) throws IOException {
    assert skviA != null || skviB != null;
    if (skviB == null || skviA == null)
      throw new IllegalStateException("No point in using " + SelectorRowMultiply.class.getSimpleName() + " if emitNoMatch is true for A or B");
    assert skviA.hasTop() && skviB.hasTop() && skviA.getTopKey().equals(skviB.getTopKey(), PartialKey.ROW);

    // advance selector skvi to next row
    Iterator<Map.Entry<Key,Value>> selectorRowIterator = new SKVIRowIterator(ASelectsBRow ? skviA : skviB);
    while (selectorRowIterator.hasNext())
      selectorRowIterator.next();

    // iterator over selected skvi's row
    return new SKVIRowIterator(ASelectsBRow ? skviB : skviA);
  }

}