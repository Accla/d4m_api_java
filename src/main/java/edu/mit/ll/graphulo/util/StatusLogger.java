package edu.mit.ll.graphulo.util;

//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Print out a status line every so often.
 */
public class StatusLogger {

  private long duration = 1000*60*2; // 2 minutes
  private long timeAtLastPrint = System.currentTimeMillis();

  public StatusLogger() {}
  public StatusLogger(long duration) {
    this.duration = duration;
  }

  public void printPeriodic(String msg) {
    long cur = System.currentTimeMillis();
    if (cur - timeAtLastPrint > duration) {
      System.out.println(msg);
      timeAtLastPrint = cur;
    }
  }

  public void logPeriodic(Logger logger, String msg) {
    long cur = System.currentTimeMillis();
    if (cur - timeAtLastPrint > duration) {
      logger.info(msg);
      timeAtLastPrint = cur;
    }
  }

}
