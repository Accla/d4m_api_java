package edu.mit.ll.graphulo.apply;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.skvi.RemoteSourceIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
 * Run this after a WholeRowIterator on the transpose of the main table TedgeT
 * with the degree table TedgeDeg of the main table held in memory.
 */
public class TfidfDegreeApply implements ApplyOp {
  private static final Logger log = LoggerFactory.getLogger(TfidfDegreeApply.class);

  /** Setup with {@link edu.mit.ll.graphulo.Graphulo#basicRemoteOpts(String, String, String, Authorizations)}
   * basicRemoteOpts(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX, ADeg, null, Aauthorizations)
   * options for RemoteSourceIterator. */
  public static IteratorSetting iteratorSetting(int priority, long numDocs, Map<String,String> remoteOpts) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class, remoteOpts);
    itset.addOption(ApplyIterator.APPLYOP, TfidfDegreeApply.class.getName());
    itset.addOption(ApplyIterator.APPLYOP+ GraphuloUtil.OPT_SUFFIX+NUMDOCS, Long.toString(numDocs));
    return itset;
  }

  public static final String NUMDOCS = "numDocs";

  private RemoteSourceIterator remoteDegTable;
  private Map<String,Double> degMap;
  private long numDocs;

  @Override
  public void init(Map<String, String> optionsOrig, IteratorEnvironment env) throws IOException {
    Preconditions.checkArgument(optionsOrig.containsKey(NUMDOCS), "Required argument "+NUMDOCS);
    Map<String,String> options = new HashMap<>(optionsOrig);
    numDocs = Long.parseLong(options.remove(NUMDOCS));
    remoteDegTable = new RemoteSourceIterator();
    remoteDegTable.init(null, options, env);
    degMap = new HashMap<>();
    scanDegreeTable();
  }

  private void scanDegreeTable() throws IOException {
    remoteDegTable.seek(new Range(), Collections.<ByteSequence>emptySet(), false);
    Text rowHolder = new Text();
    while (remoteDegTable.hasTop()) {
      degMap.put(remoteDegTable.getTopKey().getRow(rowHolder).toString(),
          Double.valueOf(remoteDegTable.getTopValue().toString()));
//      log.debug(remoteDegTable.getTopKey().getRow(rowHolder).toString() +" -> "+remoteDegTable.getTopValue().toString());
      remoteDegTable.next();
    }
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(final Key k0, Value v0) throws IOException {

    SortedMap<Key, Value> Trow = WholeRowIterator.decodeRow(k0, v0);
    int numWords = Trow.size();
    double idf = Math.log(1 + (((double)numDocs) / (1 + numWords)));
//    log.debug(String.format("(numDocs, numWords)= %2d, %2d", numDocs, numWords));

    Text wordHolder = new Text();
    Set<Map.Entry<Key, Value>> entrySet = Trow.entrySet();
    for (Map.Entry<Key, Value> entry : entrySet) {
      String word = entry.getKey().getColumnQualifier(wordHolder).toString();
      double sumWordsInDoc = degMap.get(word);
      double oldval = Double.parseDouble(entry.getValue().toString());
      double newval = oldval * (1/sumWordsInDoc) * idf;
//      log.debug(String.format("(numWords=%2d): %s %.3f * %.3f * %.3f = %.3f",
//          numWords, entry.getKey().toStringNoTime(),
//          oldval, (1/sumWordsInDoc), idf, newval));
      entry.setValue(new Value(Double.toString(newval).getBytes(StandardCharsets.UTF_8)));
    }

    return entrySet.iterator();
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
  }
}
