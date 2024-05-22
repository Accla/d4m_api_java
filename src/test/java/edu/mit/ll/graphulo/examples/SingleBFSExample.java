package edu.mit.ll.graphulo.examples;

import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.mutable.MutableLong;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Map;

/**
 * Example demonstrating
 * (1) ingest the single-table schema of a graph into the table ex10ASingle;
 * (2) create a new Accumulo table ex10ASingleStep3 with the union sum of three BFS steps from node 1;
 * (3) count the number of entries in ex10ASingleStep3.
 */
public class SingleBFSExample extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(SingleBFSExample.class);

  /** Corresponds to saved files in the test/java/resources/data folder. */
  public static final int SCALE = 10;

  public static final int numSteps = 3;

  @Test
  public void exampleSingleBFS() throws FileNotFoundException, TableNotFoundException, AccumuloSecurityException, AccumuloException {
    String Atable = "ex" + SCALE + "A";                 // Base name.
    String Stable = Atable + "Single";                  // Single table name.
    String edgeColumn = "edge";                         // Name of column that edges are stored in.
    char edgeSep = '|';                                 // Separator character for edges, e.g. the '|' in "v1|v2"
    String Rtable = "ex" + SCALE + "ASingleStep" + numSteps;   // Result of BFS is summed into Rtable.
    String degColumn = "deg";                           // Name of column qualifier under which degrees appear in ADegtable.
    boolean degInColQ = false;                          // Degree is stored in the Value, not the Column Qualifier.
    boolean copyOutDegrees = true;                      // Copy out-degrees to the result table. Note that in-degrees are not copied.
    boolean computeInDegrees = true;                    // Use extra client scans/writes after the BFS to compute result table in-degrees.
    MathTwoScalar.ScalarType degSumType = MathTwoScalar.ScalarType.LONG; // Type to use for computing degrees of last nodes reached in BFS.
    int minDegree = 20;                                 // Bounding minimum degree: only include nodes with degree 20 or higher.
    int maxDegree = Integer.MAX_VALUE;                  // Unbounded maximum degree.  This and the minimum degree make a High-pass Filter.
    String v0 = "1,25,:,27,";                           // Starting nodes: node 1 (the supernode) and all the nodes from 25 to 27 inclusive.
    boolean outputUnion = false;                        // Output nodes reached in EXACTLY numSteps distance from the v0 nodes.
    boolean trace = false;                              // Disable debug printing.
    MutableLong numEntriesWritten = new MutableLong();  // Counts number of entries written to Stable.

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
    ExampleUtil.ingestSingleSCALE(SCALE, 'A', Atable, conn);

    // Create Graphulo executor. Supply the password for your Accumulo user account.
    Graphulo graphulo = new Graphulo(conn, tester.getPassword());

    // We choose to use Accumulo's SummingCombiner as the plus operation.
    // Satisfies requirement that 0 is additive identity.
    // This iterator decodes values as longs and sums them using long-type addition.
    int sumPriority = 6;
    IteratorSetting plusOp = new IteratorSetting(sumPriority, SummingCombiner.class);
    // Options for plus operator: encode/decode with a string representation; act on all columns of Ctable.
    LongCombiner.setEncodingType(plusOp, LongCombiner.Type.STRING);
    Combiner.setCombineAllColumns(plusOp, true);
    // Note: this is the same as Graphulo.PLUS_ITERATOR_LONG

    // Single-table Breadth First Search.
    // This call blocks until the BFS completes.
    String vReached = graphulo.SingleBFS(Stable, edgeColumn, edgeSep, v0, numSteps,
        Rtable, Stable, degColumn, copyOutDegrees, computeInDegrees, degSumType, null, minDegree, maxDegree,
        plusOp, outputUnion, Authorizations.EMPTY, numEntriesWritten);
    System.out.println("Wrote "+numEntriesWritten+" entries to Stable.");
    System.out.println("First few nodes reachable in exactly "+numSteps+" steps: " +
        vReached.substring(0,Math.min(20,vReached.length())));

    // Result is in output table. Do whatever you like with it.
    BatchScanner bs = conn.createBatchScanner(Rtable, Authorizations.EMPTY, 2);
    bs.setRanges(Collections.singleton(new Range()));   // Scan whole table.
    int cnt = 0;
    for (Map.Entry<Key, Value> entry : bs) {
      cnt++;
      System.out.println(entry.getKey().toStringNoTime()+" -> "+entry.getValue());
    }
    bs.close();
    log.info("# of entries in output table " + Rtable + ": " + cnt +
        "\n  (this could be less than the total number of entries written " + numEntriesWritten + " because of summing)");
    Assert.assertTrue(cnt <= numEntriesWritten.intValue());
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  /*  Variations of above example:

  1)  Change the minimum and maximum degrees, the starting nodes and the plus operation.
      Setting plusOp to null means that entries sent to Rtable overwrite existing entries
      instead of summing.

  2)  Increase the SCALE parameter to 12, 14 or 16 to run on larger graphs.

  3)  Set the SDegtable parameter to something other than Stable to
      to obtain degrees from a different table.

  4)  Set Rtable to null to obtain the nodes reachable
      in exactly numSteps as a return value from the BFS call,
      without writing the subgraph traversed at each step to result tables.

  5)  Set outputUnion = true to obtain (at the client) all the nodes reached
      in UP TO numSteps distance from the v0 nodes.

  6)  Set degSumType to null to count columns instead of summing values
      for the degrees of nodes in the last degree of the BFS.

  */
  ////////////////////////////////////////////////////////////////////////////////////////////////


}
