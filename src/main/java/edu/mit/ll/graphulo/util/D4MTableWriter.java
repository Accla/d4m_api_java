package edu.mit.ll.graphulo.util;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;

/**
 * Wrapper around the following tables:
 * table,
 * tableT,
 * tableDeg,
 * tableTDeg.
 * Can set the column family if you want to use a non-empty one.
 */
public class D4MTableWriter implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(D4MTableWriter.class);

  @Override
  public void close() {
    this.closeIngest();
  }

  private enum State { New, Open, Closed }
  private State state = State.New;

  public static final Text DEFAULT_DEGCOL = new Text("deg");
  public static final Value VALONE = new Value("1".getBytes(StandardCharsets.UTF_8));
  public static final Text EMPTYCF =new Text("");

  /** Holds configuration options to pass to constructor of D4MTableWriter. */
  public static class D4MTableConfig {
    public String baseName;
    public Connector connector;
    public boolean
        useTable = false,
        useTableT = false,
        useTableDeg = false,
        useTableDegT = false,
        useTableField = false,
        useTableFieldT = false,
    useSameDegTable = false,
    useSameFieldTable = false,
        sumTable = false,
        sumTableT = false,
    useEdgeTable = false,
    useEdgeTableT = false,
    useEdgeTableDegT = false,
    degreeUseValue = false; // true = sum value in degree, false = sum 1 in degree per entry
    public Text colDeg = DEFAULT_DEGCOL;
    public Text colDegT = DEFAULT_DEGCOL;
    public Text cf = EMPTYCF;
    /** The number of bytes until we flush data to the server. */
    public long batchBytes = 2_000_000L;
    public boolean deleteExistingTables = false;

    public D4MTableConfig() {}

    public D4MTableConfig(D4MTableConfig c) {
      baseName = c.baseName;
      connector = c.connector;
      useTable = c.useTable;
      useTableT = c.useTableT;
      useTableDeg = c.useTableDeg;
      useTableDegT = c.useTableDegT;
      useTableField = c.useTableField;
      useTableFieldT = c.useTableFieldT;
      useSameDegTable = c.useSameDegTable;
      useSameFieldTable = c.useSameFieldTable;
      colDeg = c.colDeg;
      colDegT = c.colDegT;
      batchBytes = c.batchBytes;
      cf = c.cf;
      deleteExistingTables = c.deleteExistingTables;
      sumTable = c.sumTable;
      sumTableT = c.sumTableT;
      useEdgeTable = c.useEdgeTable;
      useEdgeTableDegT = c.useEdgeTableDegT;
      useEdgeTableT = c.useEdgeTableT;
      degreeUseValue = c.degreeUseValue;
    }
  }
  private final D4MTableConfig tconf;

  private String TNtable,TNtableT,TNtableDeg, TNtableDegT, TNtableField, TNtableFieldT, TNtableEdge, TNtableEdgeT, TNtableEdgeDegT;
  private BatchWriter
      Btable=null,
      BtableT=null,
      BtableDeg=null,
      BtableDegT =null,
      BtableField=null,
      BtableFieldT=null,
  BtableEdge=null,
  BtableEdgeT=null,
  BtableEdgeDegT=null;
  private MultiTableBatchWriter mtbw;



  private static final String ITER_SUMALL_NAME = "sumAll";

  /** Put a SummingIterator on all columns. */
  public static void assignDegreeAccumulator(String tableName, Connector c) {
    IteratorSetting cfg = null;
    try {
      cfg = c.tableOperations().getIteratorSetting(tableName, ITER_SUMALL_NAME, IteratorUtil.IteratorScope.scan);
    } catch (AccumuloSecurityException | AccumuloException ignored) {

    } catch (TableNotFoundException e) {
      log.warn(tableName + " does not exist", e);
    }

    if (cfg != null) {
      log.info("table "+tableName+": iterator "+ITER_SUMALL_NAME+" already exists with priority "+cfg.getPriority()+" and options: "+cfg.getOptions());

    } else {
      cfg = new IteratorSetting(2, ITER_SUMALL_NAME, SummingCombiner.class);
      //Combiner.setColumns(cfg, columnList);
      Combiner.setCombineAllColumns(cfg, true);
      LongCombiner.setEncodingType(cfg, LongCombiner.Type.STRING);
      try {
        //c.tableOperations().checkIteratorConflicts(tableName, cfg, EnumSet.allOf(IteratorUtil.IteratorScope.class));
        c.tableOperations().attachIterator(tableName, cfg);
      } catch (AccumuloSecurityException | AccumuloException e) {
        log.warn("error trying to add "+ITER_SUMALL_NAME+" iterator to " + tableName, e);
      } catch (TableNotFoundException e) {
        log.warn(tableName + " does not exist", e);
      }
    }
  }

  /** Create a table if not already existing. Return whether table created. */
  public static boolean createTableSoft(String tableName, Connector c, boolean deleteExistingTable) {
    TableOperations to = c.tableOperations();
    try {
      if (to.exists(tableName)) {
        if (deleteExistingTable)
          to.delete(tableName);
        else
          return false;
      }
      to.create(tableName);
      return true;
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.warn("error creating table "+tableName,e);
      return false;
    } catch (TableExistsException e) {
      log.error("impossible! Table checked to be created!", e);
      return false;
    } catch (TableNotFoundException e) {
      log.error("impossible! Table checked to be created!", e);
      return false;
    }
  }

  /** All values from the config object are copied. */
  public D4MTableWriter(D4MTableConfig config) {
    tconf = new D4MTableConfig(config); // no aliasing
    initBaseBames(tconf.baseName);
    openIngest();
  }

  private void initBaseBames(String baseName) {
    if (tconf.useTable)     TNtable=baseName;
    if (tconf.useTableT)    TNtableT=baseName+"T";
    if (tconf.useTableDeg)  TNtableDeg=baseName+"Deg";
    if (tconf.useTableDegT) TNtableDegT = tconf.useSameDegTable ? TNtableDeg : baseName + "DegT";
    if (tconf.useTableField) TNtableField =baseName + "Field";
    if (tconf.useTableFieldT) TNtableFieldT = tconf.useSameFieldTable ? TNtableField : baseName + "FieldT";
    if (tconf.useEdgeTable) TNtableEdge=baseName+"Edge";
    if (tconf.useEdgeTableT) TNtableEdgeT=baseName+"EdgeT";
    if (tconf.useEdgeTableDegT) TNtableEdgeDegT=baseName+"EdgeDegT";
  }


  /**
   * Create the tables to ingest to if they do not already exist.
   * Sets up iterators on degree tables if enabled.
   */
  public void createTablesSoft() {
    boolean btReg=false, btRegT=false, btDeg=false, btDegT=false, btField=false, btFieldT=false, btEdge=false, btEdgeT=false, btEdgeDegT=false;
    if (tconf.useTable)    btReg = createTableSoft(TNtable, tconf.connector, tconf.deleteExistingTables);
    if (tconf.useTableT && !TNtableT.equals(TNtable))   btRegT =  createTableSoft(TNtableT, tconf.connector, tconf.deleteExistingTables);
    if (tconf.useTableDeg)  btDeg = createTableSoft(TNtableDeg, tconf.connector, tconf.deleteExistingTables);
    if (tconf.useTableDegT && !TNtableDegT.equals(TNtableDeg)) btDegT = createTableSoft(TNtableDegT, tconf.connector, tconf.deleteExistingTables);
    if (tconf.useTableField) btField = createTableSoft(TNtableField, tconf.connector, tconf.deleteExistingTables);
    if (tconf.useTableFieldT && !TNtableFieldT.equals(TNtableField)) btFieldT = createTableSoft(TNtableFieldT, tconf.connector, tconf.deleteExistingTables);
    if (tconf.useEdgeTable) btEdge = createTableSoft(TNtableEdge, tconf.connector, tconf.deleteExistingTables);
    if (tconf.useEdgeTableT) btEdgeT= createTableSoft(TNtableEdgeT, tconf.connector, tconf.deleteExistingTables);
    if (tconf.useEdgeTableDegT) btEdgeDegT= createTableSoft(TNtableEdgeDegT, tconf.connector, tconf.deleteExistingTables);
    if (btReg && tconf.sumTable) assignDegreeAccumulator(TNtable, tconf.connector);
    if (btRegT && tconf.sumTableT) assignDegreeAccumulator(TNtableT, tconf.connector);
    if (btDeg)  assignDegreeAccumulator(TNtableDeg, tconf.connector);
    if (btDegT) assignDegreeAccumulator(TNtableDegT, tconf.connector);
    if (btField) assignDegreeAccumulator(TNtableField, tconf.connector);
    if (btFieldT) assignDegreeAccumulator(TNtableFieldT, tconf.connector);
    if (btEdge && tconf.sumTable) assignDegreeAccumulator(TNtableEdge, tconf.connector);
    if (btEdgeT && tconf.sumTableT) assignDegreeAccumulator(TNtableEdgeT, tconf.connector);
    if (btEdgeDegT) assignDegreeAccumulator(TNtableEdgeDegT, tconf.connector);

  }

  public void openIngest() {
    switch(state) {
      case New: createTablesSoft(); break;
      case Open: throw new IllegalStateException("tried to open ingest when already open");
      case Closed: break;
    }

    BatchWriterConfig BWconfig = new BatchWriterConfig();
    BWconfig.setMaxMemory(tconf.batchBytes);
    mtbw = tconf.connector.createMultiTableBatchWriter(BWconfig);
    try {
      if (tconf.useTable) Btable         = mtbw.getBatchWriter(TNtable);
      if (tconf.useTableT) BtableT       = TNtableT.equals(TNtable) ? Btable : mtbw.getBatchWriter(TNtableT);
      if (tconf.useTableDeg) BtableDeg   = mtbw.getBatchWriter(TNtableDeg);
      if (tconf.useTableDegT) BtableDegT = TNtableDegT.equals(TNtableDeg) ? BtableDeg : mtbw.getBatchWriter(TNtableDegT);
      if (tconf.useTableField) BtableField = mtbw.getBatchWriter(TNtableField);
      if (tconf.useTableFieldT) BtableFieldT = TNtableFieldT.equals(TNtableField) ? BtableField : mtbw.getBatchWriter(TNtableFieldT);
      if (tconf.useEdgeTable) BtableEdge         = mtbw.getBatchWriter(TNtableEdge);
      if (tconf.useEdgeTableT) BtableEdgeT         = mtbw.getBatchWriter(TNtableEdgeT);
      if (tconf.useEdgeTableDegT) BtableEdgeDegT         = mtbw.getBatchWriter(TNtableEdgeDegT);
    } catch (TableNotFoundException e) {
      log.error("crazy. Tables should have been created.", e);
    } catch (AccumuloSecurityException | AccumuloException e) {
      log.warn("error creating one of the batch writers for D4MTableWriter base " + TNtable, e);
    }
    state = State.Open;
  }

  public void flushBuffers() {
    if (state != State.Open)
      throw new IllegalStateException("flushing buffer when not open");
    try {
      mtbw.flush();
    } catch (MutationsRejectedException e) {
      log.warn("mutations rejected while flushing",e);
    }
  }

  /**
   * Close all enabled table batch writers.
   */
  public void closeIngest() {
    if (state != State.Open)
      throw new IllegalStateException("tried to close when already closed");
    Btable     = null;
    BtableT    = null;
    BtableDeg  = null;
    BtableDegT = null;
    BtableField = null;
    BtableFieldT = null;
    BtableEdge = null;
    BtableEdgeT = null;
    BtableEdgeDegT = null;
    try {
      mtbw.close();
    } catch (MutationsRejectedException e) {
      log.warn("error closing multi table writer for D4MTableWriter",e);
    }
    state = State.Closed;
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (state == State.Open)
      closeIngest();
  }

  public static final char FIELD_SEPERATOR = '|';

  /** Use "1" as the Value. */
  public void ingestRow(Text rowID, Text cq) {
    ingestRow(rowID, cq, VALONE);
  }
  /** Ingest to all enabled tables. Use "1" for the degree table values. */
  public void ingestRow(Text rowID, Text cq, Value v) {
    if (state != State.Open)
      openIngest();
    if (tconf.useTable)     ingestRow(Btable    , rowID, tconf.cf, cq, v);
    if (tconf.useTableT)    ingestRow(BtableT   , cq, tconf.cf, rowID, v);
    if (tconf.useTableDeg)  ingestRow(BtableDeg , rowID, tconf.cf, tconf.colDeg, tconf.degreeUseValue ? v : VALONE);
    if (tconf.useTableDegT) ingestRow(BtableDegT, cq, tconf.cf, tconf.colDegT, tconf.degreeUseValue ? v : VALONE);
    if (tconf.useTableField) {
      String rowIDString = rowID.toString();
      int fieldSepPos;
      if ((fieldSepPos = rowIDString.indexOf(FIELD_SEPERATOR)) == -1)
        log.warn(TNtableField +" is turned on, but the row "+rowIDString+" to ingest does not have a field seperator "+FIELD_SEPERATOR);
      else {
        Text rowIDField = new Text(rowIDString.substring(0, fieldSepPos));
        ingestRow(BtableField, rowIDField, tconf.cf, tconf.colDeg, tconf.degreeUseValue ? v : VALONE);
      }
    }
    if (tconf.useTableFieldT){
      String cqString = cq.toString();
      int fieldSepPos;
      if ((fieldSepPos = cqString.indexOf(FIELD_SEPERATOR)) == -1)
        log.warn(TNtableFieldT +" is turned on, but the row "+cqString+" to ingest does not have a field seperator "+FIELD_SEPERATOR);
      else {
        Text cqField = new Text(cqString.substring(0, fieldSepPos));
        ingestRow(BtableFieldT, cqField, tconf.cf, tconf.colDegT, tconf.degreeUseValue ? v : VALONE);
      }
    }
  }

  static byte[] concatBytes(byte[]... bs) {
    int totlen = 0;
    for (byte[] b : bs)
      totlen += b.length;
    byte[] ret = new byte[totlen];
    int pos = 0;
    for (byte[] b : bs) {
      System.arraycopy(b,0,ret,pos,b.length);
      pos += b.length;
    }
    return ret;
  }

  /** In addiiton to {@link #ingestRow(Text, Text, Value)}, handles edge table. */
  public void ingestRow(Text rowID, Text cq, Value v, Text edgeID) {
    ingestRow(rowID, cq, v);
    if (tconf.useEdgeTable || tconf.useEdgeTableT) {
      Text out = new Text(concatBytes(tconf.colDeg.getBytes(), String.valueOf(FIELD_SEPERATOR).getBytes(StandardCharsets.UTF_8), rowID.getBytes()));
      Text in = new Text(concatBytes(tconf.colDegT.getBytes(), String.valueOf(FIELD_SEPERATOR).getBytes(StandardCharsets.UTF_8), cq.getBytes()));
      if (tconf.useEdgeTable) {
        ingestRow(BtableEdge, edgeID, tconf.cf, out, v);
        ingestRow(BtableEdge, edgeID, tconf.cf, in, v);
      }
      if (tconf.useEdgeTableT) {
        ingestRow(BtableEdgeT, out, tconf.cf, edgeID, v);
        ingestRow(BtableEdgeT, in,  tconf.cf, edgeID, v);
      }
    }
    if (tconf.useEdgeTableDegT) {
      ingestRow(BtableEdgeDegT, rowID, tconf.cf, tconf.colDeg, tconf.degreeUseValue ? v : VALONE);
      ingestRow(BtableEdgeDegT, cq, tconf.cf, tconf.colDegT, tconf.degreeUseValue ? v : VALONE);
    }
  }

  public static void ingestRow(BatchWriter bw, Text rowID, Text cf, Text cq, Value v) {
    Mutation m = new Mutation(rowID);
    m.put(cf, cq, v);
    try {
      bw.addMutation(m);
    } catch (MutationsRejectedException e) {
      log.warn("mutation rejected: (row,cf,cq,v)=("+rowID+','+cf+','+cq+','+v+")",e);
    }
  }
}
