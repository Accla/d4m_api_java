package edu.mit.ll.graphulo.examples;

import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.DistributedTrace;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Map;

/**
 * Example demonstrating
 * (1) ingest the incidence matrix representation of a graph into the D4M Schema tables ex10AEdge, ex10AEdgeT, ex10AEdgeDegT;
 * (2) sample the graph uniformly with a given probability;
 * (3) create new Accumulo tables ex10AEdgeW, -WT, -H, and -HT as output from Non-negative Matrix Factorization;
 * (4) multiply the factor tables W and H into a new table ex10AEdgeApprox that approximates the original incidence matrix.
 * In-memory NMF runs fast.  Fully distributed NMF runs slow but does not require holding the input table in memory.
 */
public class NMFExample extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(NMFExample.class);

  /** Corresponds to saved files in the test/java/resources/data folder. */
  public static final int SCALE = 10;

  @Test
  public void exampleNMF() throws FileNotFoundException, TableNotFoundException, AccumuloSecurityException, AccumuloException {
    String Atable = "ex" + SCALE + "A";                 // Table base name.
    String Etable = "ex" + SCALE + "AEdge";             // Incidence table.
    String ETtable = "ex" + SCALE + "AEdgeT";           // Transpose of incidence table.
    String EtableSample = "ex" + SCALE + "AEdgeSample";   // Sampled-down version of incidence table.
    String ETtableSample = "ex" + SCALE + "AEdgeTSample"; // Sampled-down version of transpose of incidence table.
    String Wtable = "ex" + SCALE + "AEdgeW";            // Output table W.
    String WTtable = "ex" + SCALE + "AEdgeWT";          // Transpose of output table W.
    String Htable = "ex" + SCALE + "AEdgeH";            // Output table H.
    String HTtable = "ex" + SCALE + "AEdgeHT";          // Transpose of output table HT.
    int K = 3;                                          // 3 topics
    int maxiter = 5;                                    // 3 iterations of NMF maximum
    double cutoffThreshold = 0.0;                       // Threshold to cut off entries with value less than this
    int maxColsPerTopic = 10;                           // Threshold - only keep 10 nodes in H per topic
    String newVisibility = "";                          // Column Visibility to use for newly created entries.


    // In your code, you would connect to an Accumulo instance by writing something similar to:
//    ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance("instance").withZkHosts("localhost:2181").withZkTimeout(5000);
//    Instance instance = new ZooKeeperInstance(cc);
//    Connector c = instance.getConnector("root", new PasswordToken("secret"));
    // Here, we connect to the Accumulo instance given by TEST_CONFIG.java.
    // You can change this by passing the option -DTEST_CONFIG=local or -DTEST_CONFIG=txe1 or similar.
    Connector conn = tester.getConnector();

    // Delete result table if it exists, so that we don't sum in previous runs with our results.
    GraphuloUtil.deleteTables(conn, Htable, HTtable, Wtable, WTtable);
    if (conn.tableOperations().exists(Htable))
      conn.tableOperations().delete(Htable);

    // Insert data from the file test/resources/data/10Ar.txt and 10Ac.txt into Accumulo.
    // Deletes tables if they already exist.
    ExampleUtil.ingestIncidenceSCALE(SCALE, 'A', Atable, conn);

    // Create Graphulo executor. Supply the password for your Accumulo user account.
    Graphulo graphulo = new Graphulo(conn, tester.getPassword());

//    DistributedTrace.enable("NMFExample");  // remove this for no tracing

    // Option to use in-memory version
    double nmfError = graphulo.NMF_Client(Etable, false, WTtable, true, Htable, false, K, maxiter,
        cutoffThreshold, maxColsPerTopic);

    // Sample the graph with 10% uniform sampling and materialize the result in a sampled table
//    double probability = 0.1;
//    long nnzSample = graphulo.SampleCopy(Etable, EtableSample+"tmp", null, probability, trace);
//    long nnzSample = graphulo.OneTable(Etable, EtableSample, ETtableSample, null, -1, null, null, null, null, null,
//        Collections.singletonList(SamplingFilter.iteratorSetting(1, probability)), null, Authorizations.EMPTY);
//    System.out.println("Sample finished; #entries in sample is "+nnzSample);

    // Non-negative matrix factorization.
    // This call blocks until the NMF completes.
//    double nmfError = graphulo.NMF(EtableSample, ETtableSample, Wtable, WTtable, Htable, HTtable, KMER, maxiter, true,
//        cutoffThreshold);
    System.out.println("Final NMF absolute difference in error: " + nmfError);

    // Result is in Htable, HTtable, Wtable, WTtable. Do whatever you like with it.
    // For this example we will multiply H*W into a new table that approximates the original incidence matrix.
    String APtable = "ex" + SCALE + "AEdgeApprox";        // Approximation of the incidence table Etable.
    GraphuloUtil.deleteTables(conn, APtable);
    graphulo.TableMult(WTtable, Htable, APtable, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.DOUBLE, newVisibility, false),
        MathTwoScalar.combinerSetting(Graphulo.PLUS_ITERATOR_BIGDECIMAL.getPriority(), null, MathTwoScalar.ScalarOp.PLUS, MathTwoScalar.ScalarType.DOUBLE, false),
        null, null, null, false, false, -1);

    DistributedTrace.disable();

    // Now Scanning APtable
    BatchScanner bs = conn.createBatchScanner(APtable, Authorizations.EMPTY, 2);
    bs.setRanges(Collections.singleton(new Range()));   // Scan whole table.
    int cnt = 0;
    for (Map.Entry<Key, Value> entry : bs) {
      cnt++;
//      System.out.println(entry.getKey().toStringNoTime()+" -> "+entry.getValue());
    }
    bs.close();
    log.info("# of entries in approximation table " + APtable + ": " + cnt);
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  /*  Variations of above example:

  0)  Use the method graphulo.NMF_Client instead of graphulo.NMF
      to use an in-memory version of NMF that performs the computation at the client.
      It will run faster than the standard Graphulo version, as long as the entire input table can be held in memory.

  1)  Change the number of topics KMER and the maximum number of iterations.

  */
  ////////////////////////////////////////////////////////////////////////////////////////////////

}
