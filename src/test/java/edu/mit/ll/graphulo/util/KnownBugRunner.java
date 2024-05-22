package edu.mit.ll.graphulo.util;

//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Ignore;
import org.junit.AssumptionViolatedException;
import org.junit.rules.RunRules;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.util.Arrays;

/**
 * Adds the KnownBugRule to all test methods.
 * Effect: for any test method annotated with {@link org.junit.Ignore}
 * with a value that starts with "KnownBug", run the test as usual.
 * If the test fails then convert the failure into an assumption failure (meaning, ignore the test).
 * If the test passes then log a message saying the bug has been fixed.
 */
public class KnownBugRunner extends BlockJUnit4ClassRunner {
  private static final Logger log = LoggerFactory.getLogger(KnownBugRunner.class);

  public KnownBugRunner(Class<?> klass) throws InitializationError {
    super(klass);
  }

  @Override
  protected void runChild(final FrameworkMethod method, RunNotifier notifier) {
    Description description = describeChild(method);
    Ignore ignore = method.getAnnotation(Ignore.class);
    if (ignore != null && !ignore.value().startsWith("KnownBug")) {
      notifier.fireTestIgnored(description);
    } else {
      RunRules runRules = new RunRules(methodBlock(method), Arrays.asList(new TestRule[]{new KnownBugRule()}), description);
      runLeaf(runRules, description, notifier);
    }
  }

  static class KnownBugRule implements TestRule {
    public Statement apply(final Statement base, final Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          String testclassname = description.getTestClass().getSimpleName();
          String testmethodname = description.getMethodName();
          Ignore ignore = description.getAnnotation(Ignore.class);
          try {
            base.evaluate();
            if (ignore != null && ignore.value().startsWith("KnownBug"))
              log.info("Fixed: "+ignore.value()+" ["+testclassname+'#'+testmethodname+']');
          } catch (AssumptionViolatedException e) {
            // some other assumption failed
            throw e;
          } catch (AssertionError | Exception e) {
            if (ignore != null && ignore.value().startsWith("KnownBug")) {
              log.info("KnownBug Test Failure: "+ignore.value()+" ["+testclassname+'#'+testmethodname+']',e);
              throw new AssumptionViolatedException("KnownBug Test Failure: "+ignore.value(), e);
            } else {
              throw e;
            }
          }
        }
      };
    }
  }
}
