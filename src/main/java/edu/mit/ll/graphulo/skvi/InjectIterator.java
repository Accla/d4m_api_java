package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.*;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;

/**
 * For testing; interleaves data from a {@link BadHardListIterator} with parent iterator entries.
 */
public class InjectIterator extends BranchIterator implements OptionDescriber {
  private static final Logger log = LoggerFactory.getLogger(InjectIterator.class);


  @Override
  public SortedKeyValueIterator<Key, Value> initBranchIterator(Map<String, String> options, IteratorEnvironment env) throws IOException {
    return new HardListIterator();
//        side.init(null, null, env);
//        env.registerSideChannel( side );
  }


  @Override
  public OptionDescriber.IteratorOptions describeOptions() {
    return new OptionDescriber.IteratorOptions("inject", "injects hard-coded entries into iterator stream.", null, null);
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    return true;
  }
}
