package edu.mit.ll.graphulo.apply;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.skvi.RemoteSourceIterator;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Applies <tt>J_ij = J_ij / (d_i + d_j - J_ij)</tt>.
 * <p>
 *   Only runs on scan and full major compactions,
 *   because JaccardDegreeApply must see all entries for a given key in order to correctly apply.
 *   Idempotent by a clever trick: JaccardDegreeApply will not touch values that have a decimal point '.'.
 *   It will run on values that do not have a decimal point, and it will always produce a decimal point when applied.
 * <p>
 * Possible future optimization: only need to scan the part of the degree table
 * that is after the seek range's beginning trow. For example, if seeked to [v3,v5),
 * we should scan the degree table on (v3,+inf) and load those degrees into a Map.
 * <p>
 * Preserves keys.
 */
public class JaccardDegreeApply implements ApplyOp {
//  private static final Logger log = LogManager.getLogger(JaccardDegreeApply.class);
  private static final Logger log = LoggerFactory.getLogger(JaccardDegreeApply.class);

  /** Setup with {@link edu.mit.ll.graphulo.Graphulo#basicRemoteOpts(String, String, String, Authorizations)}
   * basicRemoteOpts(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX, ADeg, null, Aauthorizations)
   * options for RemoteSourceIterator. */
  public static IteratorSetting iteratorSetting(int priority, Map<String,String> remoteOpts) {
    IteratorSetting JDegApply = new IteratorSetting(priority, ApplyIterator.class, remoteOpts);
    JDegApply.addOption(ApplyIterator.APPLYOP, JaccardDegreeApply.class.getName());
    return JDegApply;
  }

  private RemoteSourceIterator remoteDegTable;
  private Map<String,Double> degMap;

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    // only run on scan or full major compaction
    if (!env.getIteratorScope().equals(IteratorUtil.IteratorScope.scan)
        && !(env.getIteratorScope().equals(IteratorUtil.IteratorScope.majc) && env.isFullMajorCompaction())) {
      remoteDegTable = null;
      degMap = null;
      return;
    }
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
      remoteDegTable.next();
    }
  }

  // for debugging:
//  private static final Text t1 = new Text("1"), t10 = new Text("10");
//  private Text trow = new Text(), tcol = new Text();

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(final Key k, Value v) {
//    if (k.getRow(trow).equals(t1) && k.getColumnQualifier(tcol).equals(t10))
//      log.warn("On k="+k.toStringNoTime()+" v="+new String(v.get()));

    // check to make sure we're running on scan or full major compaction
    if (remoteDegTable == null)
      return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, v));

    // Period indicates already processed Double value. No period indicates unprocessed Long value.
    String vstr = v.toString();
    if (vstr.contains("."))
      return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, v));
    long Jij_long = Long.parseLong(vstr);
    if (Jij_long == 0)
      return null; // no need to keep entries with value zero
    double Jij = Jij_long;

    String row = k.getRow().toString(), col = k.getColumnQualifier().toString();
    Double rowDeg = degMap.get(row), colDeg = degMap.get(col);
//    if (trow.equals(t1) && tcol.equals(t10))
//      log.warn("On k="+k.toStringNoTime()+" v="+new String(v.get())+" do with rowDeg="+rowDeg+" and colDeg="+colDeg+" for: "+(Jij / (rowDeg+colDeg-Jij)));
    if (rowDeg == null)
      throw new IllegalStateException("Cannot find rowDeg in degree table:" +row);
    if (colDeg == null)
      throw new IllegalStateException("Cannot find colDeg in degree table:" +col);
    return Iterators.singletonIterator( new AbstractMap.SimpleImmutableEntry<>(k,
        new Value(Double.toString(Jij / (rowDeg+colDeg-Jij)).getBytes(StandardCharsets.UTF_8))
    ));
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
  }
}
