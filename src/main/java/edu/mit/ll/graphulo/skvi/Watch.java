package edu.mit.ll.graphulo.skvi;

//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.EnumMap;

/**
 * For measuring performance: spans and counters.
 * Based on {@link org.apache.accumulo.core.util.StopWatch}.
 */
public class Watch<K extends Enum<K>> {
  private static final Logger log = LoggerFactory.getLogger(Watch.class);
  public static volatile boolean enableTrace = false;

  static {
    log.info("Loading Watch");
  }

  public enum PerfSpan {
    ATnext, Bnext, RowSkipNum, All, WriteAddMut, WriteFlush, WriteGetNext, Multiply
  }

  /**
   * Holds thread-local timing information.
   */
  static ThreadLocal<Watch<PerfSpan>> ThreadWatches =
      new ThreadLocal<Watch<PerfSpan>>() {
        @Override
        protected Watch<PerfSpan> initialValue() {
          return new Watch<>(PerfSpan.class);
        }
      };

  public static Watch<PerfSpan> getInstance() {
    return enableTrace ? ThreadWatches.get() : noop_instance;
  }

  private static final Watch<PerfSpan> noop_instance = new Watch<PerfSpan>(PerfSpan.class) {
    @Override
    public synchronized void start(PerfSpan timer) {

    }

    @Override
    public synchronized void stopIfActive(PerfSpan timer) {

    }

    @Override
    public synchronized void stop(PerfSpan timer) {

    }

    @Override
    public synchronized void increment(PerfSpan counter, long amount) {

    }

    @Override
    public synchronized void reset(PerfSpan timer) {

    }

    @Override
    public synchronized void resetAll() {

    }

    @Override
    public synchronized Stats get(PerfSpan timer) {
      return null;
    }

    @Override
    public synchronized void print() {

    }
  };

  static class Stats {
    public long total=0, count=0, min=Long.MAX_VALUE, max=Long.MIN_VALUE;
  }

  private EnumMap<K, Long> startTime;
  private EnumMap<K, Stats> totalStats;

  public Watch(Class<K> s) {
    startTime = new EnumMap<>(s);
    totalStats = new EnumMap<>(s);
  }

  public synchronized void start(K timer) {
    if (!enableTrace)
      return;
    if (startTime.containsKey(timer)) {
      throw new IllegalStateException(timer + " already started");
    }
    startTime.put(timer, System.currentTimeMillis());
  }

  public synchronized void stopIfActive(K timer) {
    if (!enableTrace)
      return;
    if (startTime.containsKey(timer))
      stop(timer);
  }

  public synchronized void stop(K timer) {
    if (!enableTrace)
      return;
    Long st = startTime.remove(timer);

    if (st == null) {
      throw new IllegalStateException(timer + " not started");
    }

    long dur = System.currentTimeMillis() - st;

    increment(timer, dur);
  }

  public synchronized void increment(K counter, long amount) {
    if (!enableTrace)
      return;
    Stats stats = totalStats.get(counter);
    if (stats == null)
      stats = new Stats();

    stats.count++;
    stats.max = Math.max(stats.max, amount);
    stats.min = Math.min(stats.min, amount);
    stats.total = stats.total + amount;

    totalStats.put(counter, stats);
  }

  public synchronized void reset(K timer) {
    if (!enableTrace)
      return;
    totalStats.remove(timer);
  }

  public synchronized void resetAll() {
    if (!enableTrace)
      return;
    totalStats.clear();
  }

  public synchronized Stats get(K timer) {
    if (!enableTrace)
      return new Stats();
    Stats stats = totalStats.get(timer);
    if (stats == null)
      stats = new Stats();
    return stats;
  }

//  public synchronized double getSecs(S timer) {
//    Long existingTime = totalStats.get(timer);
//    if (existingTime == null)
//      existingTime = 0L;
//    return existingTime / 1000.0;
//  }

  public synchronized void print() {
    if (!enableTrace)
      return;
    System.out.println("THREAD: " + Thread.currentThread().getName());
//    log.info("THREAD: " + Thread.currentThread().getName());
    for (K timer : totalStats.keySet()) {
      Stats stats = get(timer);
      System.out.printf("%14s: %,7d tot %,10d cnt %,6d min %,6d max%n", timer.toString(),
          stats.total, stats.count, stats.min, stats.max);
//      log.info(String.format("%15s: %,7d tot %,10d cnt %,6d min %,6d max%n", timer.toString(),
//          stats.total, stats.count, stats.min, stats.max));
    }
  }


}
