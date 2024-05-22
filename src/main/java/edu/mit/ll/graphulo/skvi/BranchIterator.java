package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;

/**
 * An abstract parent class for custom computation merged into a regular SKVI stack.
 * Handles turning performance timing on and off.
 */
public abstract class BranchIterator implements SortedKeyValueIterator<Key, Value> {
  private static final Logger log = LoggerFactory.getLogger(BranchIterator.class);

  /**
   * Return the *bottom-most* iterator of the custom computation stack.
   * The resulting iterator should be initialized; should not have to call init() on the resulting iterator.
   * Can be null, but if null, which means no computation performed.
   *
   * @param options Options passed to the BranchIterator.
   */
  public SortedKeyValueIterator<Key, Value> initBranchIterator(Map<String, String> options, IteratorEnvironment env) throws IOException {
    return null;
  }

  /**
   * Opportunity to apply an SKVI stack after merging the custom computation stack into the regular parent stack.
   * Returns parent by default.
   *
   * @param source  The MultiIterator merging the normal stack and the custom branch stack.
   * @param options Options passed to the BranchIterator.
   * @return The bottom iterator. Cannot be null. Return source if no additional computation is desired.
   */
  public SortedKeyValueIterator<Key, Value> initBranchAfterIterator(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    return source;
  }

  private SortedKeyValueIterator<Key, Value> botIterator;

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    IteratorUtil.IteratorScope scope = env.getIteratorScope();
    log.debug(this.getClass() + ": init on scope " + scope + (scope == IteratorUtil.IteratorScope.majc ? " fullScan=" + env.isFullMajorCompaction() : ""));

//    if (options.containsKey("trace")) {
//        Watch.enableTrace = Boolean.parseBoolean(options.get("trace"));
//      options = new HashMap<>(options);
//      options.remove("trace");
//    }


    SortedKeyValueIterator<Key, Value> branchIterator = initBranchIterator(options, env);
    if (branchIterator == null) {
      botIterator = source;
    } else {
      List<SortedKeyValueIterator<Key, Value>> list = new ArrayList<>(2);
      list.add(branchIterator);
      list.add(source);
      botIterator = new MultiIterator(list, false);
    }
    botIterator = initBranchAfterIterator(botIterator, options, env);
    if (botIterator == null)
      throw new IllegalStateException("--some subclass returned a null bottom iterator in branchAfter--");

    System.out.println("Reset Watch at init of BranchIterator");
//    Watch.getInstance().resetAll();
  }

  @Override
  public boolean hasTop() {
    return botIterator.hasTop();
  }

  @Override
  public void next() throws IOException {
//    System.out.println(this.getClass().getName()+" getTop: "+getTopKey()+" -> "+getTopValue());
//    Watch<Watch.PerfSpan> watch = Watch.getInstance();
//    watch.start(Watch.PerfSpan.All);
//    try {
      botIterator.next();
//    } finally {
//      watch.stop(Watch.PerfSpan.All);
//      if (!botIterator.hasTop()) {
//        watch.print();
//      }
//    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    System.out.println(this.getClass().getName()+" seek: "+range);
//    Watch<Watch.PerfSpan> watch = Watch.getInstance();
//    watch.start(Watch.PerfSpan.All);
//    try {
      botIterator.seek(range, columnFamilies, inclusive);
//    } finally {
//      watch.stop(Watch.PerfSpan.All);
//      if (!botIterator.hasTop())
//        watch.print();
//    }
  }

  @Override
  public Key getTopKey() {
    return botIterator.getTopKey();
  }

  @Override
  public Value getTopValue() {
    return botIterator.getTopValue();
  }

  @Override
  public BranchIterator deepCopy(IteratorEnvironment env) {
    BranchIterator copy;
    try {
      copy = this.getClass().newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("cannot construct deepCopy", e);
    }
    copy.botIterator = botIterator.deepCopy(env);
    return copy;
  }
}
