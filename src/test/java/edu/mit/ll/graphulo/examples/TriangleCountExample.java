package edu.mit.ll.graphulo.examples;

import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.tricount.TriangleIngestor;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Example for counting the number of triangles in a graph,
 * given the incidence matrix and the lower triangle of the adjacency matrix.
 */
public class TriangleCountExample extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(TriangleCountExample.class);

  /** Corresponds to saved files in the test/java/resources/data folder. */
  private static final int SCALE = 10;

  @Test
  public void exampleTriCount() throws FileNotFoundException, TableNotFoundException, AccumuloSecurityException, AccumuloException {
    final String Etable = "ex" + SCALE + "AEdge";                  // Incidence table A.
    final String AtableLower = "ex" + SCALE + "AAdjUULower";       // Adjacency table A (lower triangle).
    final String AtableUpper = "ex" + SCALE + "AAdjUUUpper";       // Adjacency table A (upper triangle).

    // In your code, you would connect to an Accumulo instance by writing something similar to:
//    ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance("instance").withZkHosts("localhost:2181").withZkTimeout(5000);
//    Instance instance = new ZooKeeperInstance(cc);
//    Connector c = instance.getConnector("root", new PasswordToken("secret"));
    // Here, we connect to the Accumulo instance given by TEST_CONFIG.java.
    // You can change this by passing the option -DTEST_CONFIG=local or -DTEST_CONFIG=txe1 or similar.
    final Connector conn = tester.getConnector();

    // Delete result table if it exists, so that we don't sum in previous runs with our results.
    GraphuloUtil.deleteTables(conn, Etable,
        AtableLower, AtableLower + Graphulo.TRICOUNT_TEMP_TABLE_SUFFIX,
        AtableUpper, AtableUpper + Graphulo.TRICOUNT_TEMP_TABLE_SUFFIX);

    // Insert data from the file test/resources/data/10Ar.txt and 10Ac.txt into Accumulo.
    // Deletes tables if they already exist.
    final File rowFile = ExampleUtil.getDataFile(String.valueOf(SCALE) + "Ar.txt");
    final File colFile = ExampleUtil.getDataFile(String.valueOf(SCALE) + "Ac.txt");
    final TriangleIngestor ti = new TriangleIngestor(conn);
    ti.ingestFile(rowFile, colFile, AtableLower, Etable, false, false);

    // Create Graphulo executor. Supply the password for your Accumulo user account.
    final Graphulo graphulo = new Graphulo(conn, tester.getPassword());

    // Count triangles with the Adjaceny+Edge table algorithm
    final long numTrianglesAdjEdge = graphulo.triCountAdjEdge(AtableLower, Etable, null, null, null, null);
    log.info("Count of triangles from AdjEdge algorithm: "+numTrianglesAdjEdge);


    // Count triangles with the Adjaceny-only table algorithm
    ti.ingestFile(rowFile, colFile, AtableUpper, null, true, true);
    final long numTrianglesAdj = graphulo.triCount(AtableUpper, null, null, null);
    log.info("Count of triangles from Adj algorithm: "+numTrianglesAdj);

    Assert.assertEquals("The two triangle counting algorithms should produce the same triangle count",
        numTrianglesAdj, numTrianglesAdjEdge);
  }

  /**
   * This tests the same algorithm when the input file is "combined";
   * each line takes the form of
   * `row_num col_num`.
   */
  @Test
  public void exampleTriCount_CombinedFile() throws FileNotFoundException, TableNotFoundException, AccumuloSecurityException, AccumuloException {
    final String Etable = "ex" + SCALE + "AEdge2";                 // Incidence table A.
    final String AtableLower = "ex" + SCALE + "AAdjUULower2";       // Adjacency table A (lower triangle).
    final String AtableUpper = "ex" + SCALE + "AAdjUUUpper2";       // Adjacency table A (upper triangle).
    final Connector conn = tester.getConnector();
    GraphuloUtil.deleteTables(conn, Etable,
        AtableLower, AtableLower + Graphulo.TRICOUNT_TEMP_TABLE_SUFFIX,
        AtableUpper, AtableUpper + Graphulo.TRICOUNT_TEMP_TABLE_SUFFIX);
    final Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    final File file = ExampleUtil.getDataFile("example_arcs");
    final TriangleIngestor ti = new TriangleIngestor(conn);

    ti.ingestCombinedFile(file, AtableLower, Etable, false, false);
    final long numTrianglesAdjEdge = graphulo.triCountAdjEdge(AtableLower, Etable, null, null, null, null);
    log.info("Count of triangles from AdjEdge algorithm: "+numTrianglesAdjEdge);
    Assert.assertEquals(32, numTrianglesAdjEdge);

    ti.ingestCombinedFile(file, AtableUpper, null, true, true);
    final long numTrianglesAdj = graphulo.triCount(AtableUpper, null, null, null);
    log.info("Count of triangles from Adj algorithm: "+numTrianglesAdj);
    Assert.assertEquals(32, numTrianglesAdj);
  }

//  @Test
//  public void temp() {
//    final Graphulo graphulo = new Graphulo(tester.getConnector(), tester.getPassword());
//    graphulo.OneTable("DH_pg03_20160331_TgraphEdge", null, "DH_pg03_20160331_TgraphEdgeT", null,
//        55, null, null, null, null, null, null, null, null);
//  }
}
