package edu.mit.ll.graphulo.tricount;

import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.WritableComparator;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_BYTES;
import static edu.mit.ll.graphulo.util.GraphuloUtil.VALUE_ONE_STRING_BYTES;

public final class TriangleIngestor {
  private static final Logger log = LoggerFactory.getLogger(TriangleIngestor.class);
  private static final FixedIntegerLexicoder LEX = new FixedIntegerLexicoder();
  private static final byte[] DEG_BYTES = "deg".getBytes(StandardCharsets.UTF_8);
  private static final byte[] IN_DEG_BYTES = "indeg".getBytes(StandardCharsets.UTF_8);
  private static final byte[] OUT_DEG_BYTES = "outdeg".getBytes(StandardCharsets.UTF_8);

  private final Connector connector;
  private String countDegree = null;
  @SuppressWarnings("unused")
  public void doCountDegree(String countDegree) {
    this.countDegree = countDegree;
  }

  @SuppressWarnings("unused") // used in D4M
  public TriangleIngestor(final String instanceName, final String zookeepers, final String username, final String password) throws AccumuloSecurityException, AccumuloException {
    this(new ZooKeeperInstance(instanceName, zookeepers).getConnector(username, new PasswordToken(password)));
  }
  public TriangleIngestor(final Connector connector) {
    this.connector = connector;
  }

  @SuppressWarnings("unused") // used in D4M
  public long ingestDirectory(final String directory,
                              final String tableAdj, final String tableEdge,
                              final boolean reverse, final boolean stringRowCols) {
    // call ingestFile on all pairs of files that end in "XXr.txt", "XXc.txt"
    long count = 0L;

    final File dir = new File(directory);
    if( !dir.isDirectory() )
      throw new IllegalArgumentException(("expected a directry but got "+directory));
    final File[] files = dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith("r.txt") || name.endsWith("c.txt");
      }
    });
    Objects.requireNonNull(files, "problem with directory");

    final Map<String,File> prefixMap = new HashMap<>(files.length / 2 + 1);
    for (final File file : files) {
      final String name = file.getName();
      final String prefix = name.substring(0, name.length()-5);
      if( prefixMap.containsKey(prefix) ) {
        final File other = prefixMap.remove(prefix);
        if( name.endsWith("r.txt") )
          count += ingestFile(file, other, tableAdj, tableEdge, reverse, stringRowCols);
        else
          count += ingestFile(other, file, tableAdj, tableEdge, reverse, stringRowCols);
      } else {
        prefixMap.put(prefix, file);
      }
    }
    return count;
  }
  
  private interface GetRowCol extends AutoCloseable {
//    boolean init();
    /** @return null when no more values */
    int[] next(int[] prev);
    @Override void close();
  }
  
  private class RowColFiles implements GetRowCol {
//    final File rowFile, colFile;
    final Scanner rowScanner, colScanner;

    RowColFiles(final File rowFile, final File colFile) {
//      this.rowFile = rowFile;
//      this.colFile = colFile;
      try {
        rowScanner = new Scanner(rowFile.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(rowFile)) : new FileInputStream(rowFile));
        colScanner = new Scanner(colFile.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(colFile)) : new FileInputStream(colFile));
      } catch (IOException e) {
        log.error("problem opening scan on files "+rowFile+" and "+colFile, e);
        throw new RuntimeException(e);
      }
      rowScanner.useDelimiter(",");
      colScanner.useDelimiter(",");
    }
    
    @Override
    public int[] next(int[] prev) {
      if( rowScanner.hasNext() ){
        if( !colScanner.hasNext() )
          errorNotAligned();
        prev = prev == null ? new int[2] : prev;
        prev[0] = rowScanner.nextInt();
        prev[1] = colScanner.nextInt();
        return prev;
      }
      if( colScanner.hasNext() )
        errorNotAligned();
      return null;
    }

    private void errorNotAligned() {
      throw new IllegalArgumentException("row, col files do not have the same number of elements. " +
          " rowScanner.hasNext()=" + rowScanner.hasNext() +
          " colScanner.hasNext()=" + colScanner.hasNext());
    }

    @Override
    public void close() {
      rowScanner.close();
      colScanner.close();
    }
  }

  private class CombinedFile implements GetRowCol {
    //    final File file;
    final Scanner scanner;

    CombinedFile(final File file) {
//      this.file = file;
      try {
        scanner = new Scanner(file.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(file)) : new FileInputStream(file));
      } catch (IOException e) {
        log.error("problem opening scan on file "+file, e);
        throw new RuntimeException(e);
      }
    }

    @Override
    public int[] next(int[] prev) {
      if( scanner.hasNext() ){
        prev = prev == null ? new int[2] : prev;
        prev[0] = scanner.nextInt();
        prev[1] = scanner.nextInt();
        return prev;
      }
      return null;
    }

    @Override
    public void close() {
      scanner.close();
    }
  }

  public long ingestCombinedFile(final String file,
                                 final String tableAdj, final String tableEdge,
                                 final boolean reverse, final boolean stringRowCols) {
    return ingestCombinedFile(new File(file), tableAdj, tableEdge, reverse, stringRowCols);
  }

  public long ingestCombinedFile(final File file,
                                 final String tableAdj, final String tableEdge,
                                 final boolean reverse, final boolean stringRowCols) {
    try( GetRowCol getRowCol = new CombinedFile(file) ) {
      return ingestFile(getRowCol, tableAdj, tableEdge, reverse, stringRowCols);
    }
  }

  public long ingestFile(final String rowFile, final String colFile,
                         final String tableAdj, final String tableEdge,
                         final boolean reverse, final boolean stringRowCols) {
    return ingestFile(new File(rowFile), new File(colFile), tableAdj, tableEdge, reverse, stringRowCols);
  }

  /**
   *
   * @param rowFile File with a big comma-seperated string of ints, aligned with colFile.
   * @param colFile File with a big comma-seperated string of ints, aligned with rowFile.
   * @param tableAdj Adjacency table to ingest into. Can be null.
   * @param tableEdge Incidence tablw to ingest into. Can be null.
   * @param reverse If false, insert adjacency lower triangle and incidence with column labels in ascending order.
   *                If true,  insert adjacency upper triangle and incidence with column labels in descending order.
   * @param stringRowCols If false, encode with 4-bytes. If true, encode with Integer.toString(x).getBytes().
   * @return Number of entries ingested.
   */
  public long ingestFile(final File rowFile, final File colFile,
                         final String tableAdj, final String tableEdge,
                         final boolean reverse, final boolean stringRowCols) {
    try( GetRowCol getRowCol = new RowColFiles(rowFile, colFile) ) {
      return ingestFile(getRowCol, tableAdj, tableEdge, reverse, stringRowCols);
    }
  }

  private long ingestFile(final GetRowCol getRowCol,
                          final String tableAdj, final String tableEdge,
                          final boolean reverse, final boolean stringRowCols) {
    if( tableAdj == null && tableEdge == null && countDegree == null )
      throw new IllegalArgumentException("need to specify either tableAdj or tableEdge or countDegree");

    long count = 0, startTime;
    byte[] t1b = new byte[4], t2b = new byte[4], rowcol = new byte[8];

    final BatchWriterConfig bwc = new BatchWriterConfig()
        .setMaxWriteThreads(25).setMaxMemory(1024000).setMaxLatency(100, TimeUnit.MILLISECONDS);
    MultiTableBatchWriter multiBatch = null;

    try {
      GraphuloUtil.createTables(connector, false, tableAdj, tableEdge, countDegree);
      if( countDegree != null ) {
        if( !stringRowCols )
          throw new IllegalArgumentException("countDegrees must use string encoding");
        IteratorSetting is = new IteratorSetting(1, SummingCombiner.class);
        SummingCombiner.setCombineAllColumns(is, true);
        SummingCombiner.setEncodingType(is, Type.STRING);
        GraphuloUtil.applyIteratorSoft(is, connector.tableOperations(), countDegree);
      }

      multiBatch = connector.createMultiTableBatchWriter(bwc);
      final BatchWriter bwAdj, bwEdge, bwDeg;
      if(tableAdj != null) bwAdj = multiBatch.getBatchWriter(tableAdj);
      else bwAdj = null;
      if(tableEdge != null) bwEdge = multiBatch.getBatchWriter(tableEdge);
      else bwEdge = null;
      if(countDegree != null) bwDeg = multiBatch.getBatchWriter(countDegree);
      else bwDeg = null;
//      final Map<Integer,Integer> map = new HashMap<>(2500000);

      final long origStartTime = startTime = System.currentTimeMillis();
      int[] prev = new int[2];
      byte[] pa = new byte[0], pe1 = new byte[0], pe2 = new byte[0], pd1 = new byte[0], pd2 = new byte[0];
      int pal = 0, pe1l = 0, pe2l = 0, pd1l = 0, pd2l = 0;
      Mutation ma = new Mutation(), me1 = new Mutation(), me2 = new Mutation(), md1 = new Mutation(), md2 = new Mutation();

     while( (prev = getRowCol.next(prev)) != null ) {
//        final int row, col;
        final byte[] rowb, colb;
        {
          if( prev[0] == prev[1] ) // No Diag.
            continue;
          if( stringRowCols ) {
            t1b = Integer.toString(prev[0]).getBytes(StandardCharsets.UTF_8);
            t2b = Integer.toString(prev[1]).getBytes(StandardCharsets.UTF_8);
          } else {
            LEX.encode(prev[0], t1b);
            LEX.encode(prev[1], t2b);
          }
          final int cmp = WritableComparator.compareBytes(t1b, 0, t1b.length, t2b, 0, t2b.length);
          if ((cmp > 0) ^ reverse) { // Lower triangle only, unless reversed.
//            row = t1; col = t2;
            rowb = t1b; colb = t2b;
          } else {
//            row = t2; col = t1;
            rowb = t2b; colb = t1b;
          }
        }
        // consider caching here to remove not insert the same entries twice

        if( tableAdj != null ) {
          // try to reuse the previous mutation for increased insert efficiency
          if (pal != rowb.length || WritableComparator.compareBytes(pa, 0, rowb.length, rowb, 0, rowb.length) != 0) {
            if( ma.size() > 0 )
              bwAdj.addMutation(ma);
            ma = new Mutation(rowb, 0, rowb.length);
            if( pa.length < rowb.length ) pa = Arrays.copyOf(rowb, rowb.length);
            else System.arraycopy(rowb, 0, pa, 0, rowb.length);
            pal = rowb.length;
          }
          ma.put(EMPTY_BYTES, colb, EMPTY_BYTES); // empty family, empty value
          count++;
        }

        if( tableEdge != null ) {
          if( pe1l != rowb.length || WritableComparator.compareBytes(pe1, 0, rowb.length, rowb, 0, rowb.length) != 0) {
            if( me1.size() > 0 ) bwEdge.addMutation(me1);
            me1 = new Mutation(rowb);
            if( pe1.length < rowb.length ) pe1 = Arrays.copyOf(rowb, rowb.length);
            else System.arraycopy(rowb, 0, pe1, 0, rowb.length);
            pe1l = rowb.length;
          }
          if( pe2l != colb.length || WritableComparator.compareBytes(pe2, 0, colb.length, colb, 0, colb.length) != 0) {
            if( me2.size() > 0 ) bwEdge.addMutation(me2);
            me2 = new Mutation(colb);
            if( pe2.length < colb.length ) pe2 = Arrays.copyOf(colb, colb.length);
            else System.arraycopy(colb, 0, pe2, 0, colb.length);
            pe2l = colb.length;
          }
          bothBytes(colb, rowb, rowcol);
          me1.put(EMPTY_BYTES, rowcol, EMPTY_BYTES);
          me2.put(EMPTY_BYTES, rowcol, EMPTY_BYTES);
          count += 2;
        }

        if( countDegree != null ) {
          if( pd1l != rowb.length || WritableComparator.compareBytes(pd1, 0, rowb.length, rowb, 0, rowb.length) != 0) {
            if( md1.size() > 0 ) bwDeg.addMutation(md1);
            md1 = new Mutation(rowb);
            if( pd1.length < rowb.length ) pd1 = Arrays.copyOf(rowb, rowb.length);
            else System.arraycopy(rowb, 0, pd1, 0, rowb.length);
            pd1l = rowb.length;
          }
          if( pd2l != colb.length || WritableComparator.compareBytes(pd2, 0, colb.length, colb, 0, colb.length) != 0) {
            if( md2.size() > 0 ) bwDeg.addMutation(md2);
            md2 = new Mutation(colb);
            if( pd2.length < colb.length ) pd2 = Arrays.copyOf(colb, colb.length);
            else System.arraycopy(colb, 0, pd2, 0, colb.length);
            pd2l = colb.length;
          }
          md1.put(EMPTY_BYTES, DEG_BYTES, VALUE_ONE_STRING_BYTES);
          md2.put(EMPTY_BYTES, DEG_BYTES, VALUE_ONE_STRING_BYTES);
          count += 2;
        }

        if (count % 100000 <= 1) {
          final long stopTime = System.currentTimeMillis();
          if (startTime - stopTime > 1000*60) {
            log.info(String.format("Ingest: %9d cnt, %6d secs, %8d entries/sec on %s, %s%n", count, (stopTime - origStartTime)/1000,
                Math.round(count / ((stopTime - origStartTime)/1000.0)), tableAdj, tableEdge));
            startTime = stopTime;
          }
        }
      }

      if( ma.size() > 0 )
        bwAdj.addMutation(ma);
      if( me1.size() > 0 )
        bwEdge.addMutation(me1);
      if( me2.size() > 0 )
        bwEdge.addMutation(me2);
      if( md1.size() > 0 )
        bwDeg.addMutation(md1);
      if( md2.size() > 0 )
        bwDeg.addMutation(md2);

    } catch(TableNotFoundException | AccumuloSecurityException | AccumuloException e){
      log.warn("", e);
      throw new RuntimeException(e);
    } finally {
      if (multiBatch != null)
        try {
          multiBatch.close();
        } catch (MutationsRejectedException e) {
          log.error("mutations rejected on close", e);
        }
    }
    return count;
  }

  private void bothBytes(final byte[] a, final byte[] b, final byte[] r) {
    System.arraycopy(a, 0, r, 0, 4);
    System.arraycopy(b, 0, r, 4, 4);
  }

}
