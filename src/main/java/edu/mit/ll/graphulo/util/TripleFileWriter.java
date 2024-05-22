package edu.mit.ll.graphulo.util;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

/**
 * Write row, column and (optionally) value files to a table.
 */
public class TripleFileWriter {
  private static final Logger log = LoggerFactory.getLogger(TripleFileWriter.class);

  private Connector connector;

  public TripleFileWriter(Connector connector) {
    this.connector = connector;
  }

  /**
   * Writes triples from component files to a main table, transpose table and degree table.
   *
   * @param valFile Optional value file. Uses "1" if not given.
   * @param delimiter Delimiter that separates items.
   * @param baseName Name of tables is the base name plus "", "T", "Deg"
   * @param deleteExistingTables Delete tables if present.
   * @param trackTime Log the rate of ingest or not.
   * @return Number of triples written.
   */
  public long writeTripleFile_Adjacency(File rowFile, File colFile, File valFile,
                                        String delimiter, String baseName,
                                        boolean deleteExistingTables, boolean trackTime) {
    long count = 0, startTime, origStartTime;
    Text row = new Text(), col = new Text(), valText = null;
    Value val = D4MTableWriter.VALONE;

    Scanner valScanner = null;
    try (Scanner rowScanner = new Scanner( rowFile.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(rowFile)) : new FileInputStream(rowFile) );
         Scanner colScanner = new Scanner( colFile.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(colFile)) : new FileInputStream(colFile) )) {
      rowScanner.useDelimiter(delimiter);
      colScanner.useDelimiter(delimiter);
      if (valFile != null) {
        valScanner = new Scanner(valFile);
        valScanner.useDelimiter(delimiter);
        valText = new Text();
      }

      D4MTableWriter.D4MTableConfig config = new D4MTableWriter.D4MTableConfig();
      config.baseName = baseName;
      config.useTable = config.useTableT = true;
      config.sumTable = config.sumTableT = true;
      config.useTableDeg = config.useTableDegT = true;
      config.useTableField = config.useTableFieldT = false;
      config.useSameDegTable = true;
      config.colDeg = new Text("out");
      config.colDegT = new Text("in");
      config.connector = connector;
      config.deleteExistingTables = deleteExistingTables;

      origStartTime = startTime = System.currentTimeMillis();
      try (D4MTableWriter tw = new D4MTableWriter(config)) {
        while (rowScanner.hasNext()) {
          if (!colScanner.hasNext() || (valScanner != null && !valScanner.hasNext())) {
            throw new IllegalArgumentException("row, col and val files do not have the same number of elements. " +
                " rowScanner.hasNext()=" + rowScanner.hasNext() +
                " colScanner.hasNext()=" + colScanner.hasNext() +
                (valScanner == null ? "" : " valScanner.hasNext()=" + valScanner.hasNext()));
          }
          row.set(rowScanner.next());
          col.set(colScanner.next());
          if (valFile != null) {
            valText.set(valScanner.next());
            val.set(valText.getBytes());
          }
          tw.ingestRow(row, col, val);
          count++;

          if (trackTime && count % 100000 == 0) {
            long stopTime = System.currentTimeMillis();
            if (startTime - stopTime > 1000*60) {
              System.out.printf("Ingest: %9d cnt, %6d secs, %8d entries/sec%n", count, (stopTime - origStartTime)/1000,
                  Math.round(count / ((stopTime - origStartTime)/1000.0)));
              startTime = stopTime;
            }
          }
        }
      }
    } catch (IOException e) {
      log.warn(" Bad config.basename: " +baseName,e);
      throw new RuntimeException(e);
    } finally {
      if (valScanner != null)
        valScanner.close();
    }
    return count;
  }

  /**
   * Writes triples from component files to a main table, transpose table and transpose degree table.
   *
   * @param valFile Optional value file. Uses "1" if not given.
   * @param delimiter Delimiter that separates items.
   * @param baseName Name of tables is the base name plus "", "T", "Deg"
   * @param deleteExistingTables Delete tables if present.
   * @param trackTime Log the rate of ingest or not.
   * @return Number of triples written.
   */
  public long writeTripleFile_Incidence(File rowFile, File colFile, File valFile,
                                        String delimiter, String baseName,
                                        boolean deleteExistingTables, boolean trackTime, long estimateNumEntries) {
    int numBits = (int) (Math.log10(estimateNumEntries)+1);

    long count = 0, startTime, origStartTime;
    Text row = new Text(), col = new Text(), valText = null;
    Value val = D4MTableWriter.VALONE;

    Scanner valScanner = null;
    try (Scanner rowScanner = new Scanner( rowFile.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(rowFile)) : new FileInputStream(rowFile) );
         Scanner colScanner = new Scanner( colFile.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(colFile)) : new FileInputStream(colFile) )) {
      rowScanner.useDelimiter(delimiter);
      colScanner.useDelimiter(delimiter);
      if (valFile != null) {
        valScanner = new Scanner(valFile);
        valScanner.useDelimiter(delimiter);
        valText = new Text();
      }

      D4MTableWriter.D4MTableConfig config = new D4MTableWriter.D4MTableConfig();
      config.baseName = baseName;
      config.useTable = config.useTableT = false;
      config.sumTable = config.sumTableT = true;
      config.useTableDeg = config.useTableDegT = false;
      config.useTableField = config.useTableFieldT = false;
      config.useEdgeTable = config.useEdgeTableT = true;
      config.useEdgeTableDegT = true;
//      config.useSameDegTable = true;
      config.colDeg = new Text("out");
      config.colDegT = new Text("in");
      config.connector = connector;
      config.deleteExistingTables = deleteExistingTables;

      origStartTime = startTime = System.currentTimeMillis();
      try (D4MTableWriter tw = new D4MTableWriter(config)) {
        while (rowScanner.hasNext()) {
          if (!colScanner.hasNext() || (valScanner != null && !valScanner.hasNext())) {
            throw new IllegalArgumentException("row, col and val files do not have the same number of elements. " +
                " rowScanner.hasNext()=" + rowScanner.hasNext() +
                " colScanner.hasNext()=" + colScanner.hasNext() +
                (valScanner == null ? "" : " valScanner.hasNext()=" + valScanner.hasNext()));
          }
          row.set(rowScanner.next());
          col.set(colScanner.next());
          if (valFile != null) {
            valText.set(valScanner.next());
            val.set(valText.getBytes());
          }

          count++;
          Text edgeID = new Text(StringUtils.rightPad(StringUtils.reverse(Long.toString(count)), numBits, '0'));

          tw.ingestRow(row, col, val, edgeID);


          if (trackTime && count % 100000 == 0) {
            long stopTime = System.currentTimeMillis();
            if (startTime - stopTime > 1000*60) {
              System.out.printf("Ingest: %9d cnt, %6d secs, %8d entries/sec%n", count, (stopTime - origStartTime)/1000,
                  Math.round(count / ((stopTime - origStartTime)/1000.0)));
              startTime = stopTime;
            }
          }
        }
      }
    } catch (IOException e) {
      log.warn(" Check rowfile - "+ rowFile.getName() + " , or  colfile - "+ colFile.getName() ,e);
      throw new RuntimeException(e);
    } finally {
      if (valScanner != null)
        valScanner.close();
    }
    return count;
  }

  public long writeFromAdjacency_Incidence(String baseName,
                                           boolean deleteExistingTables, boolean trackTime, long estimateNumEntries) {
    int numBits = (int) (Math.log10(estimateNumEntries) + 1);

    long count = 0, startTime, origStartTime;


    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(baseName, Authorizations.EMPTY, 2);
    } catch (TableNotFoundException e) {
      throw new RuntimeException("Table " + baseName + " does not exist", e);
    }
    bs.setRanges(Collections.singleton(new Range()));

    D4MTableWriter.D4MTableConfig config = new D4MTableWriter.D4MTableConfig();
    config.baseName = baseName;
    config.useTable = config.useTableT = false;
    config.sumTable = config.sumTableT = true;
    config.useTableDeg = config.useTableDegT = false;
    config.useTableField = config.useTableFieldT = false;
    config.useEdgeTable = config.useEdgeTableT = true;
    config.useEdgeTableDegT = true;
//      config.useSameDegTable = true;
    config.colDeg = new Text("out");
    config.colDegT = new Text("in");
    config.connector = connector;
    config.deleteExistingTables = deleteExistingTables;
    config.degreeUseValue = true;

    origStartTime = startTime = System.currentTimeMillis();
    try (D4MTableWriter tw = new D4MTableWriter(config)) {

      Text outNode = new Text(), inNode = new Text();
      for (Map.Entry<Key, Value> entry : bs) {

        Key k = entry.getKey();
        outNode = k.getRow(outNode);
        inNode = k.getColumnQualifier(inNode);

        count++;
        Text edgeID = new Text(StringUtils.rightPad(StringUtils.reverse(Long.toString(count)), numBits, '0'));

        tw.ingestRow(outNode, inNode, entry.getValue(), edgeID);


        if (trackTime && count % 100000 == 0) {
          long stopTime = System.currentTimeMillis();
          if (startTime - stopTime > 1000 * 60) {
            System.out.printf("Ingest: %9d cnt, %6d secs, %8d entries/sec%n", count, (stopTime - origStartTime) / 1000,
                Math.round(count / ((stopTime - origStartTime) / 1000.0)));
            startTime = stopTime;
          }
        }
      }
    } finally {
      bs.close();
    }
    return count;
  }

  /**
   * Writes triples from component files to a main table, transpose table and degree table.
   *
   * @param valFile Optional value file. Uses "1" if not given.
   * @param delimiter Delimiter that separates items.
   * @param baseName Name of tables is the base name plus "", "T", "Deg"
   * @param deleteExistingTables Delete tables if present.
   * @param trackTime Log the rate of ingest or not.
   * @return Number of triples written.
   */
  public long writeTripleFile_Single(File rowFile, File colFile, File valFile,
                                        String delimiter, String baseName,
                                        boolean deleteExistingTables, boolean trackTime) {
    long count = 0, startTime, origStartTime;
    Text text = new Text(), valText = null;
    Value val = D4MTableWriter.VALONE;

    Scanner valScanner = null;
    try (Scanner rowScanner = new Scanner( rowFile.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(rowFile)) : new FileInputStream(rowFile) );
         Scanner colScanner = new Scanner( colFile.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(colFile)) : new FileInputStream(colFile) )) {
      rowScanner.useDelimiter(delimiter);
      colScanner.useDelimiter(delimiter);
      if (valFile != null) {
        valScanner = new Scanner(valFile);
        valScanner.useDelimiter(delimiter);
        valText = new Text();
      }

      Text outCol = new Text("deg"), // changed to undirected version
        inCol = new Text("deg"),
        edgeCol = new Text("edge");
      String Stable = baseName+"Single";
      char edgeSep = '|';

      if (connector.tableOperations().exists(Stable))
        if (deleteExistingTables)
          connector.tableOperations().delete(Stable);
        else
          return -1; //throw new TableExistsException(Stable+" already exists");
      connector.tableOperations().create(Stable);
      D4MTableWriter.assignDegreeAccumulator(Stable, connector);

      BatchWriterConfig bwc = new BatchWriterConfig();
      BatchWriter bw = connector.createBatchWriter(Stable, bwc);

      origStartTime = startTime = System.currentTimeMillis();
      try {
        while (rowScanner.hasNext()) {
          if (!colScanner.hasNext() || (valScanner != null && !valScanner.hasNext())) {
            throw new IllegalArgumentException("row, col and val files do not have the same number of elements. " +
                " rowScanner.hasNext()=" + rowScanner.hasNext() +
                " colScanner.hasNext()=" + colScanner.hasNext() +
                (valScanner == null ? "" : " valScanner.hasNext()=" + valScanner.hasNext()));
          }
          String rowStr = rowScanner.next(), colStr = colScanner.next();
          if (valFile != null) {
            valText.set(valScanner.next());
            val.set(valText.copyBytes());
          }

          text.set(rowStr);
          Mutation mut = new Mutation(text);
          mut.put(D4MTableWriter.EMPTYCF, outCol, val);
          bw.addMutation(mut);

          text.set(colStr);
          mut = new Mutation(text);
          mut.put(D4MTableWriter.EMPTYCF, inCol, val);
          bw.addMutation(mut);

          text.set(rowStr+edgeSep+colStr);
          mut = new Mutation(text);
          mut.put(D4MTableWriter.EMPTYCF, edgeCol, val);
          bw.addMutation(mut);

          // Force undirected
          text.set(colStr+edgeSep+rowStr);
          mut = new Mutation(text);
          mut.put(D4MTableWriter.EMPTYCF, edgeCol, val);
          bw.addMutation(mut);

          count++;

          if (trackTime && count % 100000 == 0) {
            long stopTime = System.currentTimeMillis();
            if (startTime - stopTime > 1000*60) {
              System.out.printf("Ingest: %9d cnt, %6d secs, %8d entries/sec%n", count, (stopTime - origStartTime)/1000,
                  Math.round(count / ((stopTime - origStartTime)/1000.0)));
              startTime = stopTime;
            }
          }
        }
      } finally {
        bw.close();
      }
    } catch (IOException e) {
      log.warn("",e);
      throw new RuntimeException(e);
    } catch (TableExistsException | TableNotFoundException e) {
      log.error("crazy",e);
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException | AccumuloException e) {
      log.warn(" ", e);
      throw new RuntimeException(e);
    } finally {
      if (valScanner != null)
        valScanner.close();
    }
    return count;
  }


}
