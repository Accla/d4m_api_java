package edu.mit.ll.graphulo.examples;

import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.reducer.Reducer;
import edu.mit.ll.graphulo.rowmult.MultiplyOp;
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
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Example demonstrating
 * (1) ingest the adjacency matrix representation of a graph into the D4M Schema tables ex10A, ex10AT, ex10ADeg;
 * do the same for a second table B;
 * (2) multiply A*B and store the result in Accumulo table ex10C;
 * (3) count the number of entries in ex10C.
 */
public class TableMultExample extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(TableMultExample.class);

  /**
   * Corresponds to saved files in the test/java/resources/data folder.
   */
  public static final int SCALE = 10;

  @Test
  public void example1() throws FileNotFoundException, TableNotFoundException, AccumuloSecurityException, AccumuloException {
    String Atable = "ex" + SCALE + "A";     // Adjacency table A
    String ATtable = "ex" + SCALE + "AT";   // Adjacency table A Transpose (row and column qualifier switched)
    String Btable = "ex" + SCALE + "B";     // Adjacency table B
    String Ctable = "ex" + SCALE + "C";     // Adjacency table C to contain the operation result

    // In your code, you would connect to an Accumulo instance by writing something similar to:
//    ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance("instance").withZkHosts("localhost:2181").withZkTimeout(5000);
//    Instance instance = new ZooKeeperInstance(cc);
//    Connector c = instance.getConnector("root", new PasswordToken("secret"));
    // Here, we connect to the Accumulo instance given by TEST_CONFIG.java.
    // You can change this by passing the option -DTEST_CONFIG=local or -DTEST_CONFIG=txe1 or similar.
    Connector conn = tester.getConnector();

    // Delete result table if it exists, so that we don't sum in previous runs with our results.
    if (conn.tableOperations().exists(Ctable))
      conn.tableOperations().delete(Ctable);

    // Insert data from the file test/resources/data/10Ar.txt and 10Ac.txt into Accumulo.
    // Deletes tables if they already exist.
    ExampleUtil.ingestAdjacencySCALE(SCALE, 'A', Atable, conn);
    // Insert second table from 10Br.txt and 10Bc.txt
    ExampleUtil.ingestAdjacencySCALE(SCALE, 'B', Btable, conn);

    // Create Graphulo executor. Supply the password for your Accumulo user account.
    Graphulo graphulo = new Graphulo(conn, tester.getPassword());

    // Plus operation. Satisfies requirement that 0 is additive identity.
    int sumPriority = 6;
    IteratorSetting plusOp = new IteratorSetting(sumPriority, SummingCombiner.class);
    // Options for plus operator: encode/decode with a string representation; act on all columns of Ctable.
    LongCombiner.setEncodingType(plusOp, LongCombiner.Type.STRING);
    Combiner.setCombineAllColumns(plusOp, true);
    // Note: this is the same as Graphulo.PLUS_ITERATOR_BIGDECIMAL

    // Other options to TableMult
    String CTtable = null;                // Don't write the table transpose.
    int BScanIteratorPriority = -1;       // Use the default priority for the scan-time table multiplication iterator
    //  multOp must satisfy the requirement that 0 is multiplicative annihilator.
    Class<? extends MultiplyOp> multOp = MathTwoScalar.class; // Multiply operation, generic to many kinds of mathematical operators.
    Map<String,String> multOpOptions = null; // Null uses the default mode of MathTwoScalar is TIMES, which is what we want.
    String rowFilter = null;              // No row subsetting; run on whole tables.
    String colFilterAT = null;            // No column subsetting for ATtable; run on the whole table.
    String colFilterB = null;             // No column subsetting for  Btable; run on the whole table.
    boolean alsoDoAA = false;             // Don't also add in the product of A*A at the same time as A*B.
    boolean alsoDoBB = false;             // Don't also add in the product of B*B at the same time as A*B.
    List<IteratorSetting>
        iteratorsBeforeA = null,          // No extra iterators used on ATtable before TableMult.
        iteratorsBeforeB = null,          // No extra iterators used on Btable before TableMult.
        iteratorsAfterTwoTable = null;    // No extra iterators used after TableMult but before writing entries to Ctable and CTtable.
    Reducer reducer = null;               // No reducer. This is used for reduce operations, to gather
    Map<String,String> reducerOpts = null;// summary information about entries processed at the server. No reducer options.
    int numEntriesCheckpoint = -1;        // Don't monitor TableMult progress.
    boolean trace = false;                // Don't record performance times at the server.

    // Matrix multiply A*B.  Uses +.* algebra.
    // This call blocks until the multiply completes,
    // i.e., until all partial products are sent to Ctable.
    graphulo.TableMult(ATtable, Btable, Ctable, CTtable, BScanIteratorPriority, multOp, multOpOptions, plusOp,
        rowFilter, colFilterAT, colFilterB, alsoDoAA, alsoDoBB,
        iteratorsBeforeA, iteratorsBeforeB, iteratorsAfterTwoTable,
        reducer, reducerOpts, numEntriesCheckpoint, Authorizations.EMPTY, Authorizations.EMPTY);

    // Result is in Ctable. Do whatever you like with it.
    BatchScanner bs = conn.createBatchScanner(Ctable, Authorizations.EMPTY, 2);
    bs.setRanges(Collections.singleton(new Range()));   // Scan whole table.
    int cnt = 0;
    for (Map.Entry<Key, Value> entry : bs) {
      cnt++;
    }
    bs.close();
    log.info("# of entries in output table " + Ctable + ": " + cnt);
  }


  ////////////////////////////////////////////////////////////////////////////////////////////////
  /*  Variations of above example:

  0)  Increase the SCALE parameter to 12, 14 or 16 to run on larger graphs.

  1)  Replace: SummingCombiner.class ==> MaxCombiner.class
      and Replace: multOpOptions = null; ==> multOpOptions = MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.MIN, MathTwoScalar.ScalarType.LONG);
      to use max.min algebra.  We assume all nonzero values are integers greater than 0.
      This maintains 0 as the additive identity since for any a, max(a,0)=a.
      This maintains 0 as the multiplicative annihilator since for any a, min(a,0)=0.
      The result will have the same sparsity pattern as the +.* algebra.
      One difference is that if all nonzero input values are 1, then all nonzero output values are 1.

  2)  Replace: String rowFilter = null; ==> String rowFilter = "2,:,";
      to include in the TableMult only columns after and including column "2" of A (which is a row of AT)
      and rows after row "1" of B. This restricts the dimension of the tables on which they are "joined."

  3)  Replace: String colFilterAT = null; ==> String colFilterAT = "7,23,85,";
      to include in the TableMult only rows "7", "23", "85" of A (which are columns of AT).
      This restricts the rowspace of table C.
      Applying a column filter to colFilterB similarly restricts the columnspace of table C.

  4)  Change numEntriesCheckpoint to an amount greater than zero in order to receive
      "monitoring entries" in the middle of the operation.  An Accumulo tablet server will send
      one monitoring entry after numEntriesCheckpoint entries are processed.
      NOTE: Monitoring will make concurrent scans on table B unstable
            since it adjusts a table-level memory size property.

  5)  Set alsoDoAA and/or alsoDoBB to aid in loop fusion-- an optimization where we perform
      steps at the same time that would normally be performed one after another serially.
      This would comute A*A or B*B at the same time as A*B, eliminating the need
      to compute those products separately before/after A*B when they are needed.

  */
  ////////////////////////////////////////////////////////////////////////////////////////////////






}
