package edu.mit.ll.graphulo.skvi;

import edu.mit.ll.graphulo.DynamicIteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

/**
 * Solves the problem of running out of iterator priority spaces.
 * Bundles several iterators together in one.
 * @see edu.mit.ll.graphulo.DynamicIteratorSetting
 */
public class DynamicIterator extends WrappingIterator {
  private static final Logger log = LoggerFactory.getLogger(DynamicIterator.class);

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    DynamicIteratorSetting dis = DynamicIteratorSetting.fromMap(options);
    EnumSet<DynamicIteratorSetting.MyIteratorScope> diScopes = dis.getDiScopes();
    boolean doit;
    switch (env.getIteratorScope()) {
      case majc:
        if (env.isFullMajorCompaction()) {
          doit = diScopes.contains(DynamicIteratorSetting.MyIteratorScope.MAJC_FULL);
//          System.out.println("FULL MAJC "+doit);
        }
        else
          doit = diScopes.contains(DynamicIteratorSetting.MyIteratorScope.MAJC_PARTIAL);
        break;
      case minc: doit = diScopes.contains(DynamicIteratorSetting.MyIteratorScope.MINC); break;
      case scan: doit = diScopes.contains(DynamicIteratorSetting.MyIteratorScope.SCAN); break;
      default: throw new AssertionError();
    }
    if (doit)
      source = dis.loadIteratorStack(source, env);
    else
      log.debug("Not applying this DynamicIterator "+dis.getDiName()+"; scope "+env.getIteratorScope()+" is not permitted");
    setSource(source);
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    DynamicIterator copy = new DynamicIterator();
    copy.setSource(getSource().deepCopy(env));
    return copy;
  }
}
