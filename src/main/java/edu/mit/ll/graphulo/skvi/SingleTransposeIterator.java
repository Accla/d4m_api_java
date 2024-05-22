package edu.mit.ll.graphulo.skvi;

import edu.mit.ll.graphulo.util.GraphuloUtil;
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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * When passed an entry whose row contains the <tt>edgeSep</tt> string in the form of <tt>v1|v2</tt> for edgeSep <tt>|</tt>
 *  AND the <tt>v2</tt> part is NOT in the startNodes,
 *  then emit <tt>v1|v2</tt> with even timestamp and <tt>v2|v1</tt> with odd timestamp.
 * Otherwise emit the original entry except, if it contains the edgeSep, we change its timestamp to even.
 */
public class SingleTransposeIterator implements SortedKeyValueIterator<Key,Value> {
  private static final Logger log = LoggerFactory.getLogger(SingleTransposeIterator.class);

  public static final String EDGESEP = "edgeSep", STARTNODES = "startNodes",
      NEG_ONE_IN_DEG = "negOneInDeg",
      DEGCOL = "degCol";

  private char edgeSep = '|';
  private SortedSet<Range> startNodes = new TreeSet<>();
  private boolean negOneInDeg = false; // dangerous option
  private Text degCol = new Text("");

  private SortedKeyValueIterator<Key, Value> source;
  private Key topKey1, topKey2, topKey3;
  private Value topValue1, topValue2, topValue3;

  private void parseOptions(Map<String,String> options) {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String entryKey = entry.getKey(), entryValue = entry.getValue();
      switch (entryKey) {
        case EDGESEP:
          if (entryValue.length() != 1)
            throw new IllegalArgumentException("bad "+ EDGESEP +": "+entryValue);
          edgeSep = entryValue.charAt(0);
          break;
        case STARTNODES:
          startNodes = GraphuloUtil.d4mRowToRanges(entryValue);
          break;
        case NEG_ONE_IN_DEG:
          negOneInDeg = Boolean.parseBoolean(entryValue);
          break;
        case DEGCOL:
          degCol = new Text(entryValue);
          break;
        default:
          log.warn("Unrecognized option: " + entry);
      }
    }

  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
    parseOptions(options);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
    topKey1 = topKey2 = topKey3 = null;
    topValue1 = topValue2 = topValue3 = null;
    prepNext(false);
  }

  private void prepNext(boolean doNext) throws IOException {
    if (topKey3 != null) {
      topKey1 = topKey2;
      topKey2 = topKey3;
      topKey3 = null;
      topValue1 = topValue2;
      topValue2 = topValue3;
      topValue3 = null;
      return;
    }
    if (topKey2 != null) {
      topKey1 = topKey2;
      topKey2 = null;
      topValue1 = topValue2;
      topValue2 = null;
      return;
    }
    topKey1 = null;
    topValue1 = null;

    if (doNext)
      source.next();
    if (!source.hasTop())
      return;

    topKey1 = source.getTopKey();
    topValue1 = source.getTopValue();

    // begin analysis of topKey1 to see if we need to alter or make topKey2
    Text rowText = topKey1.getRow();
    String rStr = rowText.toString();
    int pos = rStr.indexOf(edgeSep);
    if (pos == -1)
      return;        // this is a degree row, not an edge row.
    // this is an edge row; ensure timestamp is even and check whether the toNode is in startNodes
    long ts = topKey1.getTimestamp();
    long tsEven = ts % 2 == 0 ? ts : ts-1;

    topKey1 = new Key(rowText, topKey1.getColumnFamily(), topKey1.getColumnQualifier(),
        topKey1.getColumnVisibility(), tsEven);

    String toNode = rStr.substring(pos+1);
    if (!isInStartNodes(toNode)) {
      long tsOdd = tsEven+1;
      String fromNode = rStr.substring(0,pos);
      topKey2 = new Key(new Text(toNode+edgeSep+fromNode), topKey1.getColumnFamily(), topKey1.getColumnQualifier(),
          topKey1.getColumnVisibility(), tsOdd);
      topValue2 = topValue1;
      // degree entry
      if (negOneInDeg) {
        topKey3 = new Key(new Text(toNode), topKey1.getColumnFamily(), degCol,
            topKey1.getColumnVisibility(), tsOdd);
        topValue3 = new Value("-1".getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  private boolean isInStartNodes(String toNode) {
    Key toKey = new Key(toNode);
    for (Range startNode : startNodes) {
      if (startNode.contains(toKey))
        return true;
    }
    return false;
  }


  @Override
  public void next() throws IOException {
    prepNext(true);
  }

  @Override
  public boolean hasTop() {
    return topKey1 != null;
  }

  @Override
  public Key getTopKey() {
    return topKey1;
  }

  @Override
  public Value getTopValue() {
    return topValue1;
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    SingleTransposeIterator copy = new SingleTransposeIterator();
    copy.edgeSep = edgeSep;
    copy.startNodes = startNodes;
    copy.source = source.deepCopy(env);
    copy.degCol = degCol;
    copy.negOneInDeg = negOneInDeg;
    return copy;
  }
}
