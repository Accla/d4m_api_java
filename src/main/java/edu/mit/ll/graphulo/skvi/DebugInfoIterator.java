package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * For debugging; sends information about iterator calls to log4j at INFO level.
 */
public class DebugInfoIterator extends WrappingIterator implements OptionDescriber {

  private String prefix;

  private static final Logger log = LoggerFactory.getLogger(DebugInfoIterator.class);

  public DebugInfoIterator() {
  }

  public DebugInfoIterator deepCopy(IteratorEnvironment env) {
    return new DebugInfoIterator(this, env);
  }

  private DebugInfoIterator(DebugInfoIterator other, IteratorEnvironment env) {
    setSource(other.getSource().deepCopy(env));
    prefix = other.prefix;
  }

  public DebugInfoIterator(String prefix, SortedKeyValueIterator<Key, Value> source) {
    this.prefix = prefix;
    this.setSource(source);
  }

  @Override
  public Key getTopKey() {
    Key wc = super.getTopKey();
    log.info(prefix + " getTopKey() --> " + wc);
    return wc;
  }

  @Override
  public Value getTopValue() {
    Value w = super.getTopValue();
    log.info(prefix + " getTopValue() --> " + w);
    return w;
  }

  @Override
  public boolean hasTop() {
    boolean b = super.hasTop();
    log.info(prefix + " hasTop() --> " + b);
    return b;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    log.info(prefix + " seek(" + range + ", " + columnFamilies + ", " + inclusive + ")");
    super.seek(range, columnFamilies, inclusive);
  }

  @Override
  public void next() throws IOException {
    log.info(prefix + " next()");
    super.next();
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    log.info("init(" + source + ", " + options + ", " + env + ")");

    if (null == prefix) {
      prefix = String.format("0x%08X", this.hashCode());
    }

    super.init(source, options, env);
  }

  @Override
  public IteratorOptions describeOptions() {
    return new IteratorOptions("debug", DebugInfoIterator.class.getSimpleName() + " prints INFO information on each SortedKeyValueIterator method invocation", null, null);
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    return true;
  }
}

