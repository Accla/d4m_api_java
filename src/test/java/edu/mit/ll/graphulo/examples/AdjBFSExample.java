package edu.mit.ll.graphulo.examples;

import edu.mit.ll.graphulo.Graphulo;
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
 * (1) ingest the adjacency matrix representation of a graph into the D4M Schema tables ex10A, ex10AT, ex10ADeg;
 * (2) create a new Accumulo table ex10Astep3 with the union sum of three BFS steps;
 * (3) count the number of entries in ex10Astep3.
 */
public class AdjBFSExample extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(AdjBFSExample.class);

  /** Corresponds to saved files in the test/java/resources/data folder. */
  public static final int SCALE = 10;

  public static final int numSteps = 3;

  @Test
  public void exampleAdjBFS() throws FileNotFoundException, TableNotFoundException, AccumuloSecurityException, AccumuloException {
    String Atable = "ex" + SCALE + "A";                 // Adjacency table A.
    String Rtable = "ex" + SCALE + "Astep" + numSteps;  // Result of BFS is summed into Rtable.
    String RTtable = null;                              // Don't write transpose of BFS.
    String ADegtable = "ex" + SCALE + "ADeg";           // Adjacency table A containing out-degrees.
    String degColumn = "out";                           // Name of column qualifier under which out-degrees appear in ADegtable.
    boolean degInColQ = false;                          // Degree is stored in the Value, not the Column Qualifier.
    int minDegree = 20;                                 // Bounding minimum degree: only include nodes with degree 20 or higher.
    int maxDegree = Integer.MAX_VALUE;                  // Unbounded maximum degree.  This and the minimum degree make a High-pass Filter.
    int AScanIteratorPriority = -1;                     // Use default priority for scan-time iterator on table A
    String v0 = "1,25,:,27,";                           // Starting nodes: node 1 (the supernode) and all the nodes from 25 to 27 inclusive.
    Map<Key,Value> clientResultMap = null;              // Unused because we are writing entries to a remote table instead of gathering at the client.
    Authorizations Aauth = Authorizations.EMPTY;        // Authorizations to use for scanning Atable.
    Authorizations ADegauth = Authorizations.EMPTY;     // Authorizations to use for scanning ADegtable.
    boolean outputUnion = false;                        // Return nodes reached in EXACTLY k steps.
    MutableLong numEntriesWritten = new MutableLong();  // Counts number of entries written to Rtable.


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

    // We choose to use Accumulo's SummingCombiner as the plus operation.
    // Satisfies requirement that 0 is additive identity.
    // This iterator decodes values as longs and sums them using long-type addition.
    int sumPriority = 6;
    IteratorSetting plusOp = new IteratorSetting(sumPriority, SummingCombiner.class);
    // Options for plus operator: encode/decode with a string representation; act on all columns of Ctable.
    LongCombiner.setEncodingType(plusOp, LongCombiner.Type.STRING);
    Combiner.setCombineAllColumns(plusOp, true);
    // Note: this is the same as Graphulo.PLUS_ITERATOR_LONG

    // Adjacency Table Breadth First Search.
    // This call blocks until the BFS completes.
    String vReached = graphulo.AdjBFS(Atable, v0, numSteps, Rtable, RTtable, clientResultMap, AScanIteratorPriority,
        ADegtable, degColumn, degInColQ, minDegree, maxDegree, plusOp, Aauth, ADegauth, outputUnion, numEntriesWritten);
    System.out.println("Wrote "+numEntriesWritten+" entries to Rtable.");
    log.info("First few nodes reachable in exactly "+numSteps+" steps: " +
            vReached.substring(0,Math.min(20,vReached.length())));

    // Result is in output table. Do whatever you like with it.
    BatchScanner bs = conn.createBatchScanner(Rtable, Authorizations.EMPTY, 2);
    bs.setRanges(Collections.singleton(new Range()));   // Scan whole table.
    int cnt = 0;
    for (Map.Entry<Key, Value> entry : bs) {
      cnt++;
//      System.out.println(entry.getKey().toStringNoTime()+" -> "+entry.getValue());
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

  3)  Set ADegtable to null to instruct Graphulo to filter degrees on the fly using an iterator.

  4)  Set Rtable and RTtable both to null to obtain the nodes reachable
      in exactly numSteps as a return value from the BFS call,
      without writing the subgraph traversed at each step to result tables.

  5)  Set outputUnion=true to return the nodes reached in UP TO k steps.

  */
  ////////////////////////////////////////////////////////////////////////////////////////////////


}
