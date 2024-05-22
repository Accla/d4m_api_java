package edu.mit.ll.graphulo.reducer;

import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;

/**
 * Column "in|v3" ==> "v3".
 * Stores a set of the columns reached in one step of BFS on the incidence matrix.
 * Pass as an option a D4M string of all acceptable prefixes, e.g., "inA|,inB|,".
 */
public class EdgeBFSReducer extends ReducerSerializable<HashSet<String>> {
  private static final Logger log = LoggerFactory.getLogger(EdgeBFSReducer.class);

  public static final String IN_COLUMN_PREFIX = "inColumnPrefixes";
  private byte[][] inColumnPrefixes;

  private HashSet<String> setNodesReached = new HashSet<>();

  private void parseOptions(Map<String, String> options) {
    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
      switch (optionKey) {
        case IN_COLUMN_PREFIX:
          log.debug("inColumnPrefixes: "+optionValue);
          String[] prefixes = GraphuloUtil.splitD4mString(optionValue);
          inColumnPrefixes = new byte[prefixes.length][];
          for (int i = 0; i < prefixes.length; i++)
            inColumnPrefixes[i] = prefixes[i].getBytes(StandardCharsets.UTF_8);
          break;
        default:
          log.warn("Unrecognized option: " + optionEntry);
      }
    }
    if (inColumnPrefixes == null)
      throw new IllegalArgumentException("no "+IN_COLUMN_PREFIX);
  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) {
      parseOptions(options);
  }

  @Override
  public void reset() throws IOException {
    setNodesReached.clear();
  }

  private String findNodeAfter(byte[] cqBytes) {
    // sequential/linear search: try every inColumnPrefix.
    // Binary search could speed this but almost always low number of inColumnPrefixes.
    for (byte[] inColumnPrefix : inColumnPrefixes) {
      String nodeAfter = GraphuloUtil.stringAfter(inColumnPrefix, cqBytes);
//      log.debug((nodeAfter==null? "NO : " : "YES: ")+new String(cqBytes));
      if (nodeAfter != null)
        return nodeAfter;
    }
    return null;
  }

  @Override
  public void update(Key k, Value v) {
    String nodeAfter = findNodeAfter(k.getColumnQualifierData().toArray());
    if (nodeAfter != null)
      setNodesReached.add(nodeAfter);
//    log.debug("received colQ "+k.getColumnQualifier().toString()+" : now "+setNodesReached.toString());
  }

  @Override
  public void combine(HashSet<String> another) {
    setNodesReached.addAll(another);
  }

  @Override
  public boolean hasTopForClient() {
    return !setNodesReached.isEmpty();
  }

  @Override
  public HashSet<String> getSerializableForClient() {
    return setNodesReached;
  }
}
