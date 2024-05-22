package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.lang.time.StopWatch;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;

/**
 * Provides a MiniAccumuloCluster for use in testing.
 */
public class MiniAccumuloTester extends ExternalResource implements IAccumuloTester {
  private static final Logger log = LoggerFactory.getLogger(MiniAccumuloTester.class);
  private final boolean doDebug;
  /* Fixture State */
  private File tempDir;
  private MiniAccumuloCluster miniaccumulo;
  private Instance instance;
  private static final String USER = "root";
  private static final String PASSWORD = "password";
  private int numTservers;

  private boolean reuse = false;

  public MiniAccumuloTester(int numTservers, boolean doDebug, boolean reuse) {
    this.numTservers = numTservers;
    this.doDebug = doDebug;
    this.reuse = reuse;
  }

  public Connector getConnector() {
    Connector c;
    try {
      c = instance.getConnector(USER, new PasswordToken(PASSWORD));
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("failed to connect to MiniAccumulo instance", e);
      throw new RuntimeException(e);
    }
    return c;
  }

  @Override
  public String getUsername() {
    return USER;
  }

  @Override
  public PasswordToken getPassword() {
    return new PasswordToken(PASSWORD);
  }

  @Override
  protected void before() throws Throwable {
    if (instance == null) {
      StopWatch sw = new StopWatch();
      sw.start();
      tempDir = Files.createTempDirectory("tempMini", new FileAttribute<?>[]{}).toFile();
      log.info("Temp directory: " + tempDir.getPath());

      MiniAccumuloConfig mac = new MiniAccumuloConfig(tempDir, PASSWORD)
          .setNumTservers(numTservers);
      mac.setJDWPEnabled(doDebug);
      miniaccumulo = new MiniAccumuloCluster(mac);
      miniaccumulo.start();

      /*******************************************************************
       * MiniAccumulo DEBUG Section. Instructions:
       * Watch the test output with `tail -f `
       * When you see the debug port appear on screen for TABLET_SERVER,
       * connect to that port with your IDE.
       * You have 10 seconds before the test continues.
       *******************************************************************/
      if (doDebug) {
        System.out.println("DEBUG PORTS: " + miniaccumulo.getDebugPorts());
        Thread.sleep(10000);
      }

      instance = new ZooKeeperInstance(miniaccumulo.getInstanceName(), miniaccumulo.getZooKeepers());
      sw.stop();
      log.debug("MiniAccumulo created instance: " + instance.getInstanceName() + " - creation time: " + sw.getTime() / 1000.0 + "s");
    } else
      log.debug("Reusing MiniAccumulo instance "+instance.getInstanceName());
  }

  @Override
  protected void after() {
    if (!reuse && miniaccumulo != null) {
      instance = null;
      try {
        miniaccumulo.stop();
      } catch (IOException | InterruptedException e) {
        System.err.print("Error stopping MiniAccumuloCluster: ");
        e.printStackTrace();
      }
      boolean b = tempDir.delete();
      miniaccumulo = null;
      instance = null;
      log.debug("tearDown ok - instance destroyed; tempDir deleted=" + b);
    }
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (miniaccumulo != null) {
      try {
        miniaccumulo.stop();
      } catch (IOException | InterruptedException e) {
        System.err.print("Error stopping MiniAccumuloCluster in finalize: ");
        e.printStackTrace();
      }
      boolean b = tempDir.delete();
      miniaccumulo = null;
      log.debug("FINALIZE tearDown ok - instance destroyed; tempDir deleted=" + b);
    }
  }
}
