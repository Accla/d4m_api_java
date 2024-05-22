package edu.mit.ll.graphulo.util;

import edu.mit.ll.graphulo.IAccumuloTester;
import edu.mit.ll.graphulo.TEST_CONFIG;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * Similar to {@link org.apache.accumulo.harness.AccumuloIT}.
 */
public class AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(AccumuloTestBase.class);

  /** This is setup once for the entire class. */
  @ClassRule
  public static final IAccumuloTester tester = TEST_CONFIG.AccumuloTester;

  @Rule
  public TestName testName = new TestName();

  public String[] getUniqueNames(int num) {
    String[] names = new String[num];
    for (int i = 0; i < num; i++)
      names[i] = "test_" + this.getClass().getSimpleName() + "_" + testName.getMethodName() + i;
    return names;
  }
}
