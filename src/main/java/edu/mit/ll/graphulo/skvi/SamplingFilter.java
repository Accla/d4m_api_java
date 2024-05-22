package edu.mit.ll.graphulo.skvi;

import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Map;

/**
 * Filter based on minimum and maximum Value.
 * Interprets Values as one of the {@link MathTwoScalar.ScalarType} types,
 * encoded as a String.
 */
public class SamplingFilter extends Filter {
  private static final Logger log = LoggerFactory.getLogger(SamplingFilter.class);
  public static final String PROBABILITY = "probability";

  public static IteratorSetting iteratorSetting(int priority, double probability) {
    IteratorSetting itset = new IteratorSetting(priority, SamplingFilter.class);
    itset.addOption(PROBABILITY, Double.toString(probability));
    return itset;
  }

  private double probability;


  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (!options.containsKey(PROBABILITY))
      throw new IllegalArgumentException(PROBABILITY+" is a required option");
    probability = Double.parseDouble(options.get(PROBABILITY));
    log.debug("probability is "+probability);
  }

  @Override
  public boolean accept(Key k, Value v) {
    return Math.random() <= probability;
  }

  @Override
  public SamplingFilter deepCopy(IteratorEnvironment env) {
    SamplingFilter copy = (SamplingFilter)super.deepCopy(env);
    copy.probability = probability;
    return copy;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName(SamplingFilter.class.getCanonicalName());
    io.setDescription("Filter based on Value interpreted as a Long, encoded as String");
    io.addNamedOption(PROBABILITY, "Probability of accepting an entry.");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    if (!options.containsKey(PROBABILITY))
      throw new IllegalArgumentException(PROBABILITY+" is a required option");
    //noinspection ResultOfMethodCallIgnored
    Double.parseDouble(options.get(PROBABILITY));
    return super.validateOptions(options);
  }
}
