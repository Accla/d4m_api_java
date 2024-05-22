package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.TripleFileWriter;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import org.apache.accumulo.core.client.Connector;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;

/**
 * Insert graph data from a file in test/resources/
 * into Accumulo and test Graphulo functions.
 */
public class InsertDataTest extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(InsertDataTest.class);

  public static final String PREFIX_TABLE_SCALE = "test_SCALE_";

  @Test
  public void putSCALE10() throws FileNotFoundException {
    Connector conn = tester.getConnector();
    String baseName = PREFIX_TABLE_SCALE+"10A";

    TripleFileWriter tripleFileWriter = new TripleFileWriter(conn);
    URL url = Thread.currentThread().getContextClassLoader().getResource("data/10Ar.txt");
    Assert.assertNotNull(url);
    File rowFile = new File(url.getPath());
    url = Thread.currentThread().getContextClassLoader().getResource("data/10Ac.txt");
    Assert.assertNotNull(url);
    File colFile = new File(url.getPath());

    // deleteExistingTables
    long cnt = tripleFileWriter.writeTripleFile_Adjacency(rowFile, colFile, null, ",", baseName, true, false);
    log.info("Wrote "+cnt+" triples to D4M tables with base name "+baseName);
  }


}
