package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.simplemult.MathTwoScalar.ScalarType;
import edu.mit.ll.graphulo.skvi.LruCacheIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.mutable.MutableLong;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 * Contains convenience functions for calling Graphulo functions from Matlab.
 * Matlab works best when the arguments are all primitive types and strings.
 */
@SuppressWarnings("unused") // Used in Matlab
public class MatlabGraphulo extends Graphulo {
  private static final Logger log = LoggerFactory.getLogger(MatlabGraphulo.class);

  //static {
    // load log4j once, when this class is loaded
  //  DOMConfigurator.configure(MatlabGraphulo.class.getClassLoader().getResource("log4j.xml"));
  //}

  public MatlabGraphulo(String instanceName, String zookeepers, String username, String password)
      throws AccumuloSecurityException, AccumuloException {
    super(new ZooKeeperInstance(instanceName, zookeepers).getConnector(username, new PasswordToken(password)), new PasswordToken(password));
  }

  public long TableMult(String ATtable, String Btable, String Ctable) {
    return TableMult(ATtable, Btable, Ctable, -1, true);
  }

  public long TableMult(String ATtable, String Btable, String Ctable, String rowFilter, String colFilterAT, String colFilterB) {
    return TableMult(ATtable, Btable, Ctable, rowFilter, colFilterAT, colFilterB, -1, true);
  }

  public long TableMult(String ATtable, String Btable, String Ctable, int numEntriesCheckpoint, boolean trace) {
    return TableMult(ATtable, Btable, Ctable, null, null, null, numEntriesCheckpoint, true);
  }

  public long TableMult(String ATtable, String Btable, String Ctable,
                        String rowFilter, String colFilterAT, String colFilterB,
                        int numEntriesCheckpoint, boolean trace) {
    return TableMult(ATtable, Btable, Ctable, null,
        rowFilter, colFilterAT, colFilterB, numEntriesCheckpoint, trace);
  }

  public long TableMult(String ATtable, String Btable, String Ctable, String CTtable,
                        String rowFilter, String colFilterAT, String colFilterB,
                        int numEntriesCheckpoint, boolean trace) {
    return TableMult(ATtable, Btable, Ctable, null,
        rowFilter, colFilterAT, colFilterB, -1, numEntriesCheckpoint, trace);
  }

  public long TableMult(String ATtable, String Btable, String Ctable, String CTtable,
                        String rowFilter, String colFilterAT, String colFilterB,
                        int presumCacheSize,
                        int numEntriesCheckpoint, boolean trace) {
    List<IteratorSetting> itAfterTT =
        presumCacheSize <= 0 ? null :
            Collections.singletonList(LruCacheIterator.combinerSetting(
                1, null, presumCacheSize, MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.PLUS, MathTwoScalar.ScalarType.LONG, "", false)
            ));

    return TableMult(ATtable, Btable, Ctable, CTtable, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG, "", false), Graphulo.PLUS_ITERATOR_LONG,
        rowFilter, colFilterAT, colFilterB, false, false, null, null, itAfterTT, null, null, numEntriesCheckpoint, Authorizations.EMPTY, Authorizations.EMPTY);
  }


  public void CancelCompact(String table) throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
//    try {
      connector.tableOperations().cancelCompaction(table);
//    } catch (AccumuloException | AccumuloSecurityException e) {
//      log.error("error trying to cancel compaction for " + table, e);
//    } catch (TableNotFoundException e) {
//      log.error("", e);
//    }
  }

  /** Full major compact a table and wait for it to finish. */
  public void Compact(String table) throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    System.out.println("Compacting " + table + "...");
//    try {
      connector.tableOperations().compact(table, null, null, true, true);
//    } catch (AccumuloException | AccumuloSecurityException e) {
//      log.error("error trying to compact " + table, e);
//    } catch (TableNotFoundException e) {
//      log.error("", e);
//    }
  }

  public void CloneTable(String t1, String t2, boolean logDur)throws TableExistsException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    System.out.println("Clone " + t1 + " to "+t2);
    Map<String,String> propsToSet = logDur ? Collections.singletonMap("table.durability", "log") : null;
    connector.tableOperations().clone(t1, t2, true, propsToSet, null);
  }

  /** Flush a table, writing entries in memory to disk. */
  public void Flush(String table) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    System.out.println("Flushing " + table + "...");
//    try {
      connector.tableOperations().flush(table, null, null, true);
//    } catch (AccumuloException | AccumuloSecurityException e) {
//      log.error("error trying to compact " + table, e);
//    } catch (TableNotFoundException e) {
//      log.error("", e);
//    }
  }

  public boolean Exists(String table) {
    return connector.tableOperations().exists(table);
  }

  public void PrintTable(String table) throws TableNotFoundException {
    Scanner scanner;
//    try {
      scanner = connector.createScanner(table, Authorizations.EMPTY);
//    } catch (TableNotFoundException e) {
//      log.error("error scaning table "+table, e);
//      throw new RuntimeException(e);
//    }

    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime()+"    "+entry.getValue());
    }
    scanner.close();
  }

  public void PrintTableDebug(String table) throws TableNotFoundException {
    Scanner scanner;
//    try {
      scanner = connector.createScanner(table, Authorizations.EMPTY);
//    } catch (TableNotFoundException e) {
//      log.error("error scaning table "+table, e);
//      throw new RuntimeException(e);
//    }

    for (Map.Entry<Key, Value> entry : scanner) {
      byte[] b = entry.getValue().get();
      System.out.println(entry.getKey().toStringNoTime()+"    "+
          Key.toPrintableString(b, 0, b.length, 300));
    }
    scanner.close();
  }

  public void ApplyIteratorAll(String table, IteratorSetting itset) {
    GraphuloUtil.applyIteratorSoft(itset, connector.tableOperations(), table);
  }

  public void ApplyIteratorScan(String table, IteratorSetting itset) {
    GraphuloUtil.addOnScopeOption(itset, EnumSet.of(IteratorUtil.IteratorScope.scan));
    GraphuloUtil.applyIteratorSoft(itset, connector.tableOperations(), table);
  }

  public void RemoveIterator(String table, IteratorSetting itset) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
//    try {
      connector.tableOperations().removeIterator(table, itset.getName(), EnumSet.allOf(IteratorUtil.IteratorScope.class));
//    } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
//      log.error("Problem removing iterator "+itset+" from table "+table, e);
//    }
  }

  public void SetConfig(String table, String key, String value) throws AccumuloSecurityException, AccumuloException {
//    try {
      connector.tableOperations().setProperty(table, key, value);
//    } catch (AccumuloException | AccumuloSecurityException e) {
//      log.error("problem setting table :: key :: value ==> "+table+" :: "+key+" :: "+value, e);
//    }
  }

  /**
   * MATLAB/Octave-friendly bridge to {@link Graphulo#SingleBFS}.
   * @param edgeSep String version of a char. Pass a length-1 string. Used to workaround Octave which does not understand char.
   * @param degSumType String version of ScalarType. Choices: LONG, DOUBLE, BIGDECIMAL, LONG_OR_DOUBLE, LEX_LONG.
   */
  public String SingleBFS(String Stable, String edgeColumn, String edgeSep,
                          String v0, int k, String Rtable, String SDegtable, String degColumn,
                          boolean copyOutDegrees, boolean computeInDegrees,
                          String degSumType, ColumnVisibility newVisibility,
                          int minDegree, int maxDegree, IteratorSetting plusOp,
                          boolean outputUnion, Authorizations Sauthorizations, MutableLong numEntriesWritten) {
    if( edgeSep == null || edgeSep.length() != 1 )
      throw new IllegalArgumentException("edgeSep should be a single character but given "+edgeSep);
    ScalarType dst = degSumType != null && !degSumType.isEmpty() ? ScalarType.valueOf(degSumType) : null;
    return SingleBFS(Stable, edgeColumn, edgeSep.charAt(0), v0, k, Rtable, SDegtable, degColumn, copyOutDegrees, computeInDegrees,
        dst, newVisibility, minDegree, maxDegree, plusOp, outputUnion, Sauthorizations, numEntriesWritten);
  }

}
