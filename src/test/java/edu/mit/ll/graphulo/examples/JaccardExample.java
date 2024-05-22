package edu.mit.ll.graphulo.examples;

import com.google.common.collect.Iterables;
import edu.mit.ll.graphulo.ClientSideIteratorAggregatingScanner;
import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.apply.KeyRetainOnlyApply;
import edu.mit.ll.graphulo.simplemult.ConstantTwoScalar;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo_ndsi.DoubleStatsCombiner;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

/**
 * Example demonstrating
 * (1) ingest the adjacency matrix representation of a graph into the D4M Schema tables ex10A, ex10AT, ex10ADeg;
 * (2) copy a subset of ex10A to a table ex10ASub, using low-pass degree filtering;
 * (3) create a new Accumulo table ex10J holding the Jaccard coefficients in the upper triangle of a subset of ex10A;
 * (4) find the minimum, maximum, sum, count and average of the Jaccard coefficients in ex10J.
 */
public class JaccardExample extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(JaccardExample.class);

  /** Corresponds to saved files in the test/java/resources/data folder. */
  public static final int SCALE = 10;

  @Test
  public void exampleJaccard() throws FileNotFoundException, TableNotFoundException, AccumuloSecurityException, AccumuloException {
    String Atable = "ex" + SCALE + "A";                 // Adjacency table A.
    String Rtable = "ex" + SCALE + "J";                 // Table to write Jaccard coefficients.
    String ADegtable = "ex" + SCALE + "ADeg";           // Adjacency table A containing out-degrees.
    String newVisibility = "";                          // Column Visibility to use for newly created entries.


    // In your code, you would connect to an Accumulo instance by writing something similar to:
//    ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance("instance").withZkHosts("localhost:2181").withZkTimeout(5000);
//    Instance instance = new ZooKeeperInstance(cc);
//    Connector c = instance.getConnector("root", new PasswordToken("secret"));
    // Here, we connect to the Accumulo instance given by TEST_CONFIG.java.
    // You can change this by passing the option -DTEST_CONFIG=local or -DTEST_CONFIG=txe1 or similar.
    Connector conn = tester.getConnector();

    // Delete result table if it exists, so that we don't sum in previous runs with our results.
    if (conn.tableOperations().exists(Rtable))
      conn.tableOperations().delete(Rtable);

    // Insert data from the file test/resources/data/10Ar.txt and 10Ac.txt into Accumulo.
    // Deletes tables if they already exist.
    ExampleUtil.ingestAdjacencySCALE(SCALE, 'A', Atable, conn);

    // Create Graphulo executor. Supply the password for your Accumulo user account.
    Graphulo graphulo = new Graphulo(conn, tester.getPassword());

    // Materialize a subset of the adjacency table as a new table AtableSub.
    // Subset is all nodes and edges reachable in one step from v0.
    // By writing both the normal table and the transpose to the same table,
    // we create an undirected adjacency table subset.
    // Low-pass Degree Filtering: limit the maximum degree to 75.
    String AtableSub = Atable+"Sub";
    String v0 = "2,4,6,8,15,25,37,42,69,150,155,";
    // Iterator on new table forces Values to 1, creating an unweighted adj. table.
    IteratorSetting itset = ConstantTwoScalar.iteratorSetting(
        Graphulo.PLUS_ITERATOR_BIGDECIMAL.getPriority(), new Value("1".getBytes(StandardCharsets.UTF_8)));

    String nodesReached = graphulo.AdjBFS(Atable, v0, 1, AtableSub, AtableSub, null, -1,
        ADegtable, "out", false, 1, 75, itset);
    log.info("Nodes reached from v0: "+nodesReached);
    log.info("Does AtableSub exist? "+conn.tableOperations().exists(AtableSub));

    String filterRowCol = null; // no filtering beyond what we already did with the AdjBFS
    long npp = graphulo.Jaccard(AtableSub, ADegtable, Rtable, filterRowCol, Authorizations.EMPTY, newVisibility);
    log.info("Number of partial products sent to result table: " + npp);

    // Result is in output table. Do whatever you like with it.
    // In this example, we scan the result table with a special Combiner
    // that emits statistics based on entries it sees in each row-column.
    // The KeyRetainOnlyApply iterator puts all entries in the same row-column in each tablet.
    // It must be run once more at the client to aggregate across tablets.
    // See ClientSideIteratorAggregatingScanner, which is usable because
    // we can hold one entry from each tablet in memory.
    IteratorSetting scanIters = new DynamicIteratorSetting(19, "statsCombineAll")
        .append(KeyRetainOnlyApply.iteratorSetting(1, null))
        .append(DoubleStatsCombiner.iteratorSetting(1, null))
        .toIteratorSetting();
    BatchScanner bs = conn.createBatchScanner(Rtable, Authorizations.EMPTY, 2);
    bs.setRanges(Collections.singleton(new Range()));   // Scan whole table.
    bs.addScanIterator(scanIters);  // Run at server.
    bs = new ClientSideIteratorAggregatingScanner(bs);
    bs.addScanIterator(scanIters);  // Run at client.
    Map.Entry<Key, Value> allStatsEntry = Iterables.getOnlyElement(bs); // Single entry returned.
    bs.close();

    String[] statParts = allStatsEntry.getValue().toString().split(",");
    double min = Double.parseDouble(statParts[0]),
        max = Double.parseDouble(statParts[1]),
        sum = Double.parseDouble(statParts[2]),
        cnt = Double.parseDouble(statParts[3]);
    log.info("Jaccard min: "+min);
    log.info("Jaccard max: "+max);
    log.info("Jaccard sum: "+sum);
    log.info("Jaccard cnt: " + cnt);
    log.info("Jaccard avg: " + (sum/cnt));
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  /*  Variations of above example:

  1)  Change the subset by modifying the AdjBFS call.

  2)  Increase the SCALE parameter to 12, 14 or 16 to run on larger graphs.

  3)  Run the Jaccard coefficients on a static set of nodes instead of the nodes reachable from v0.
      Remove the AdjBFS call and let
        filterRowCol = "8,15,25,37,42,69,150,";
      for example.

  */
  ////////////////////////////////////////////////////////////////////////////////////////////////


}
