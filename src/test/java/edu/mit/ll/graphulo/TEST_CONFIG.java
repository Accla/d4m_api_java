package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.KnownBugSuite;
import edu.mit.ll.graphulo_ocean.GenomicEncoderTest;
import edu.mit.ll.graphulo_ocean.OceanTest;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

@RunWith(KnownBugSuite.class)
@Suite.SuiteClasses({
    UtilTest.class,
    AccumuloApiTest.class,
    InsertDataTest.class,
    InjectTest.class,
    RemoteIteratorTest.class,
    TableMultIteratorTest.class,
    TableMultTest.class,
    TableFilterTest.class,
    BFSTest.class,
    CountTest.class,
    RowMultiplyTest.class,
    ReducerTest.class,
    EWiseTest.class,
    SCCTest.class,
    LineTest.class,
    AlgorithmTest.class,
    GenomicEncoderTest.class,
    OceanTest.class
})

/**
 * Holds test configurations; default test suite for JUnit testing.
 */
public class TEST_CONFIG {
  private static final Logger log = LoggerFactory.getLogger(TEST_CONFIG.class);
  /**
   * Set the Accumulo config to use for all test classes here.
   */
  public static IAccumuloTester AccumuloTester;

  public static final String DEFAULT_TEST_CONFIG_FILE = "GraphuloTest.conf";

  static {
    String s = System.getProperty("TEST_CONFIG"); // environment variable
    if (s == null && new File(DEFAULT_TEST_CONFIG_FILE).exists()) {
      loadConfigFromFileOrSystem(DEFAULT_TEST_CONFIG_FILE);
      if (AccumuloTester == null)
        AccumuloTester = new MiniAccumuloTester(1, false, false);
    } else if (s == null) {
      AccumuloTester = new MiniAccumuloTester(1, false, false);
    } else if (s.startsWith("txe1-")) {
      String instance  = s.substring(5);
      File file = new File("/home/gridsan/groups/databases/"+instance+"/accumulo_user_password.txt");
      PasswordToken token;
      try (BufferedReader is = new BufferedReader(new FileReader(file))) {
        token = new PasswordToken(is.readLine());
      } catch (FileNotFoundException e) {
        log.error("Cannot find accumulo_user_password.txt for instance "+instance, e);
        throw new RuntimeException(e);
      } catch (IOException e) {
        log.error("Problem reading accumulo_user_password.txt for instance " + instance, e);
        throw new RuntimeException(e);
      }
      AccumuloTester = new RealAccumuloTester(instance, instance+".cloud.llgrid.txe1.mit.edu:2181", "AccumuloUser", token);

    } else {
      switch (s) {
        case "local":
          // G = edu.mit.ll.graphulo.MatlabGraphulo('instance','localhost:2181','root','secret')
          // DB = DBserver('localhost:2181','Accumulo','instance','root','secret')
          AccumuloTester = new RealAccumuloTester("instance", "localhost:2181", "root", new PasswordToken("secret"));
          break;
        case "local-1.7":
          // G = edu.mit.ll.graphulo.MatlabGraphulo('instance-1.7.0','localhost:2181','root','secret')
          // DB = DBserver('localhost:2181','Accumulo','instance-1.7.0','root','secret')
          AccumuloTester = new RealAccumuloTester("instance-1.7.0", "localhost:2181", "root", new PasswordToken("secret"));
          break;
        case "mini":
          AccumuloTester = new MiniAccumuloTester(1, false, false);
          break;
        case "miniDebug":   // Enables debugging on started MiniAccumulo process.
          AccumuloTester = new MiniAccumuloTester(1, true, false);
          break;
        case "mini2": // 2 tablet server MiniAccumuloCluster
          AccumuloTester = new MiniAccumuloTester(2, false, false);
          break;
        case "miniReuse":
          AccumuloTester = new MiniAccumuloTester(2, false, true);
          break;
        default:
          // interpret value as a file path
          loadConfigFromFileOrSystem(s);
          if (AccumuloTester == null)
            AccumuloTester = new MiniAccumuloTester(1, false, false);
          break;
      }
    }
  }

  public static final String
      KEY_INSTANCE_NAME = "accumulo.it.cluster.standalone.instance.name",
      KEY_ZOOKEEPERS = "accumulo.it.cluster.standalone.zookeepers",
      KEY_USER = "accumulo.it.cluster.standalone.admin.principal",
      KEY_PASSWORD = "accumulo.it.cluster.standalone.admin.password";

  private static void loadConfigFromFileOrSystem(String filename) {
    String instancename = null, zookeepers = null, user = null, pass = null;
    if (filename != null && !filename.isEmpty())
      try {
        Configuration properties = new PropertiesConfiguration(filename);
        instancename = properties.getString(KEY_INSTANCE_NAME);
        zookeepers = properties.getString(KEY_ZOOKEEPERS);
        user = properties.getString(KEY_USER);
        pass = properties.getString(KEY_PASSWORD);
      } catch (ConfigurationException e) {
        log.warn("Couldn't find a valid properties file named " + filename);
      }
    instancename = System.getProperty(KEY_INSTANCE_NAME, instancename);
    zookeepers = System.getProperty(KEY_ZOOKEEPERS, zookeepers);
    user = System.getProperty(KEY_USER, user);
    pass = System.getProperty(KEY_PASSWORD, pass);
    if (instancename == null || zookeepers == null || user == null || pass == null)
      return;
    AccumuloTester = new RealAccumuloTester(instancename, zookeepers, user, new PasswordToken(pass));
  }

  // Alternatives:
//    public static final IAccumuloTester AccumuloTester =
//            new RealAccumuloTester("instance","localhost:2181",5000,"root",new PasswordToken("secret"));

  //"classdb51.cloud.llgrid.txe1.mit.edu:2181"

}
