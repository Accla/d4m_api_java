package edu.mit.ll.graphulo_ocean;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.apply.ApplyOp;
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
 * No longer used since we are running Bray-Curtis on the relative abundances.
 */
public class DistanceApply implements ApplyOp {
  private static final Logger log = LoggerFactory.getLogger(DistanceApply.class);

  private void scanDegreeTable() throws IOException {
    remoteDegTable.seek(new Range(), Collections.<ByteSequence>emptySet(), false);
    while (remoteDegTable.hasTop()) {
      degMap.put(remoteDegTable.getTopKey().getRow(),
          Long.valueOf(remoteDegTable.getTopValue().toString()));
      remoteDegTable.next();
    }
  }

  /** Setup with {@link edu.mit.ll.graphulo.Graphulo#basicRemoteOpts(String, String, String, Authorizations)}
   * basicRemoteOpts(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX, ADeg, null, Aauthorizations)
   * options for RemoteSourceIterator. */
  public static IteratorSetting iteratorSetting(int priority, Map<String,String> remoteOpts) {
    IteratorSetting is = new IteratorSetting(priority, ApplyIterator.class, remoteOpts);
    is.addOption(ApplyIterator.APPLYOP, DistanceApply.class.getName());
    return is;
  }

  private RemoteSourceIterator remoteDegTable;
  private Map<Text,Long> degMap;

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


  private Text tA = new Text(), tB = new Text();

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(final Key k, Value v) {
    long da = degMap.get(k.getRow(tA));
    long db = degMap.get(k.getColumnQualifier(tB));
    double lv = Double.parseDouble(new String(v.get()));
    double nv = 1 - 2*lv / (da+db);

    return Iterators.singletonIterator( new AbstractMap.SimpleImmutableEntry<>(k,
        new Value(Double.toString(nv).getBytes(StandardCharsets.UTF_8))
    ));
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
  }
}
