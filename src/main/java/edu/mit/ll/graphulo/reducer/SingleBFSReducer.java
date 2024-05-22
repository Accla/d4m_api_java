package edu.mit.ll.graphulo.reducer;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

/**
 * Row "v1|v2" ==> "v2".
 * Stores a set of the "in-node" part of rows reached in one step of BFS on a single-table schema.
 */
public class SingleBFSReducer extends ReducerSerializable<HashSet<String>> {
  private static final Logger log = LoggerFactory.getLogger(SingleBFSReducer.class);

  public static final String EDGE_SEP = "edgeSep";
//      NEG_ONE_IN_DEG = "copyDeg",
//      DEGCOL = "degCol";

  private char edgeSep;
//  private boolean copyDeg = true;
//  private String degCol = "";

  private HashSet<String> setNodesReached = new HashSet<>();
//  private HashMap<String,Integer> setNodesReachedCount = new HashMap<>();

  private void parseOptions(Map<String, String> options) {
    boolean gotFieldSep = false;
    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
      switch (optionKey) {
        case EDGE_SEP:
          if (optionValue.length() != 1)
            throw new IllegalArgumentException("bad "+ EDGE_SEP +": "+optionValue);
          edgeSep = optionValue.charAt(0);
          gotFieldSep = true;
          break;
//        case NEG_ONE_IN_DEG:
//          copyDeg = Boolean.parseBoolean(optionValue);
//          break;
//        case DEGCOL:
//          degCol = optionValue;
//          break;
        default:
          log.warn("Unrecognized option: " + optionEntry);
      }
    }
    if (!gotFieldSep)
      throw new IllegalArgumentException("no "+ EDGE_SEP);
  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) {
    parseOptions(options);
  }

  @Override
  public void reset() throws IOException {
    setNodesReached.clear();
//    setNodesReachedCount.clear();
  }

  @Override
  public void update(Key k, Value v) {
    // SIGNAL from SingleTransposeIterator
    if (k.getTimestamp() % 2 != 0)
      return;

    String rStr;
    {
      ByteSequence rowData = k.getRowData();
      rStr = new String(rowData.toArray(), rowData.offset(), rowData.length());
    }
    int pos = rStr.indexOf(edgeSep);
    if (pos == -1)
      return;        // this is a degree row, not an edge row.

//    log.debug("edge row "+rStr+" : now "+setNodesReached.toString());


    String toNode = rStr.substring(pos+1);
    setNodesReached.add(toNode);

//    if (copyDeg) {
//      Integer cnt = setNodesReachedCount.get(toNode);
//      cnt = cnt == null ? new Integer(1) : new Integer(cnt+1);
//      setNodesReachedCount.put(toNode, cnt);
//    }


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

//  @Override
//  public Iterator<Map.Entry<Key,Value>> getForWrite() {
//    Map<Key,Value> map = new LinkedHashMap<>(setNodesReachedCount.size());
//    for (Map.Entry<String, Integer> entry : setNodesReachedCount.entrySet()) {
//      Key k = new Key(entry.getKey(), "", degCol);
//      Value v = new Value(entry.getValue().toString().getBytes(StandardCharsets.UTF_8));
//      map.put(k,v);
//    }
//    return map.entrySet().iterator();
//  }
}
