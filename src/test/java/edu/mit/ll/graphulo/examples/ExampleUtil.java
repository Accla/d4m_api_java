package edu.mit.ll.graphulo.examples;

import edu.mit.ll.graphulo.util.TripleFileWriter;
import org.apache.accumulo.core.client.Connector;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;

/**
 * Utility functions used by examples.
 */
public class ExampleUtil {
  private static final Logger log = LoggerFactory.getLogger(ExampleUtil.class);

  private ExampleUtil() {
  }

  public static File getDataFile(String name) {
    URL url = Thread.currentThread().getContextClassLoader().getResource("data/"+name);
    if (url == null)
      url = Thread.currentThread().getContextClassLoader().getResource("data/"+name+".gz");
    Assert.assertNotNull("Cannot find file "+name, url);
    return new File(url.getPath());
  }

  /** Reads files from src/test/resource/data and inserts into Accumulo using D4M Schema table+transpose+degree. */
  public static void ingestAdjacencySCALE(int SCALE, char version, String baseName, Connector conn) throws FileNotFoundException {
    TripleFileWriter tripleFileWriter = new TripleFileWriter(conn);
    File rowFile = getDataFile(String.valueOf(SCALE)+version+"r.txt");
    File colFile = getDataFile(String.valueOf(SCALE)+version+"c.txt");

    // deleteExistingTables
    long cnt = tripleFileWriter.writeTripleFile_Adjacency(rowFile, colFile, null, ",", baseName, true, true);
    log.info("Wrote "+cnt+" edges to D4M Adjacency tables with base name "+baseName);
  }

  public static void ingestIncidenceSCALE(int SCALE, char version, String baseName, Connector conn) {
//    D4mDbTableOperations d4mtops = new D4mDbTableOperations(conn.getInstance().getInstanceName(), conn.getInstance().getZooKeepers(),
//        conn.whoami(), pass );
    TripleFileWriter tripleFileWriter = new TripleFileWriter(conn);
    File rowFile = getDataFile(String.valueOf(SCALE)+version+"r.txt");
    File colFile = getDataFile(String.valueOf(SCALE)+version+"c.txt");

    // deleteExistingTables
    log.debug("estimated # of nodes in "+baseName+" about to insert: "+(1 << SCALE)*16);
    long cnt = tripleFileWriter.writeTripleFile_Incidence(rowFile, colFile, null, ",", baseName, true, true, (1 << SCALE) * 16); // upper bound on #edges
    log.info("Wrote "+cnt+" edges to D4M Incidence tables with base name "+baseName);
  }

  public static void ingestIncidenceFromAdjacencySCALE(int SCALE, char version, String baseName, Connector conn) {
//    D4mDbTableOperations d4mtops = new D4mDbTableOperations(conn.getInstance().getInstanceName(), conn.getInstance().getZooKeepers(),
//        conn.whoami(), pass );
    TripleFileWriter tripleFileWriter = new TripleFileWriter(conn);

    // deleteExistingTables
    log.debug("estimated # of nodes in " + baseName+" about to insert: " + (1 << SCALE) * 16);
    long cnt = tripleFileWriter.writeFromAdjacency_Incidence(baseName, true, true, (1 << SCALE) * 16); // upper bound on #edges
    log.info("Wrote "+cnt+" edges to D4M Incidence tables with base name "+baseName);
  }

  /** Reads files from src/test/resource/data and inserts into Accumulo single-table schema. */
  public static void ingestSingleSCALE(int SCALE, char version, String baseName, Connector conn) throws FileNotFoundException {
    TripleFileWriter tripleFileWriter = new TripleFileWriter(conn);
    File rowFile = getDataFile(String.valueOf(SCALE)+version+"r.txt");
    File colFile = getDataFile(String.valueOf(SCALE)+version+"c.txt");

    // deleteExistingTables
    log.debug("estimated # of nodes in " + baseName+" about to insert: "+(1 << SCALE)*16);
    long cnt = tripleFileWriter.writeTripleFile_Single(rowFile, colFile, null, ",", baseName, true, true);
    log.info("Wrote "+cnt+" edges to single-table with base name "+baseName);
  }

}
