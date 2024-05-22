package edu.mit.ll.graphulo_ocean;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.skvi.LongLexicoderTemp;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.StatusLogger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_BYTES;
import static edu.mit.ll.graphulo_ocean.GenomicEncoder.bytesToInt;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Takes a generic action to run on each csv file. Parses the csv file into kmers and runs the action when the file is completed.
 * The IntegerMap variant stores all kmer counts in one big int[].
 * The VariableMap variant uses a Map data structure.
 * Prefer the IntegerMap for performance unless K is too large.
 */
public abstract class CSVIngesterKmer<T> {
  private static final Logger log = LoggerFactory.getLogger(CSVIngesterKmer.class);


  /** An action to take on all the kmers collected from a sample file. */
  public interface KmerAction<T> {
    void run(String sampleid, T map);
  }

  /** An action that ingests into Accumulo.
   * The Accumulo column qualifier is the file name.
   * The Accumulo row is the kmer.
   * */
  public static abstract class IngestIntoAccumulo<T> implements KmerAction<T> {
    private final static Lexicoder<Long> LEX = new LongLexicoder();

    private final boolean alsoIngestReverseComplement;
    private final boolean alsoIngestSmallerLex;
    private final String Atable, oTsampleDegree, AtableRC, AtableSmaller;
    private final Connector connector;
    protected final GenomicEncoder G;

    public IngestIntoAccumulo(Connector connector, String atable, String oTsampleDegree, boolean alsoIngestReverseComplement, boolean alsoIngestSmallerLex, int k) {
      this.alsoIngestReverseComplement = alsoIngestReverseComplement;
      this.alsoIngestSmallerLex = alsoIngestSmallerLex;
      Atable = atable;
      this.connector = connector;
      this.oTsampleDegree = oTsampleDegree;
      this.G = new GenomicEncoder(k);

      createSeqTable(Atable);
      createDegreeTable(oTsampleDegree);
      AtableRC = Atable+"_RC";
      AtableSmaller = Atable+"_Smaller";
      if (alsoIngestReverseComplement)
        createSeqTable(AtableRC);
      if (alsoIngestSmallerLex)
        createSeqTable(AtableSmaller);
    }

    private SortedSet<Text> getSplitPoints(int power4splits) {
      SortedSet<Text> set = new TreeSet<>();
      Preconditions.checkArgument(power4splits < 8);
      for (byte i = 1; i < 1 << power4splits; i++) {
        byte[] ba = new byte[1];
        ba[0] = (byte) (i << (8 - power4splits));
        set.add(new Text( ba ));
      }
      return set;
    }

    private void createSeqTable(String table) {
      // create tables if they don't exist
      if (!connector.tableOperations().exists(table)) {

        IteratorSetting longCombiner = new IteratorSetting(1, SummingCombiner.class);
        SummingCombiner.setCombineAllColumns(longCombiner, true);
        SummingCombiner.setEncodingType(longCombiner, LongLexicoderTemp.class);

        try {
          connector.tableOperations().create(table);
          connector.tableOperations().addSplits(table, getSplitPoints(3));
          GraphuloUtil.applyIteratorSoft(longCombiner, connector.tableOperations(), table);

        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | TableExistsException e) {
          log.warn("", e);
        }
      }
    }

    private void createDegreeTable(String table) {
      // create tables if they don't exist
      if (!connector.tableOperations().exists(table)) {

        IteratorSetting longCombiner = Graphulo.PLUS_ITERATOR_LONG;

        try {
          connector.tableOperations().create(table);
          GraphuloUtil.applyIteratorSoft(longCombiner, connector.tableOperations(), table);

        } catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
          log.warn("", e);
        }
      }
    }

    @Override
    public void run(String sampleid, T map) {
      BatchWriterConfig config = new BatchWriterConfig();
      MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(config);
      BatchWriter bw, bwRC, bwSmaller;

      try {
        byte[] sampleidb = sampleid.getBytes(UTF_8);
        bw = mtbw.getBatchWriter(Atable);
        bwRC = alsoIngestReverseComplement ? mtbw.getBatchWriter(AtableRC) : null;
        bwSmaller = alsoIngestSmallerLex ? mtbw.getBatchWriter(AtableSmaller) : null;

        Pair<Long, Long> p = ingestMap(map, bw, sampleidb, null);
        log.info("wrote "+p.getSecond()+" forward entries to "+Atable);
        long ingested = p.getFirst();
        long totalsum = p.getSecond();

        if (alsoIngestReverseComplement) {
          p = ingestMap(map, bwRC, sampleidb,
              new SpecificKmerAction() {
                @Override
                public byte[] transformKmer(byte[] mer) {
                  byte[] b2 = Arrays.copyOf(mer, mer.length);
                  G.reverseComplement(b2);
                  return b2;
                }
              });
          log.info("wrote " + p.getSecond() + " reverse complement entries to " + AtableRC);
          if (ingested != p.getFirst() || p.getSecond() != totalsum)
            log.warn("RC " + p.getFirst() + " not equal to ingested " + ingested + " or totalsum " + p.getSecond() + " not equal to totalsum " + totalsum);
        }

        if (alsoIngestSmallerLex) {
          p = ingestMap(map, bwSmaller, sampleidb,
              new SpecificKmerAction() {
                @Override
                public byte[] transformKmer(byte[] e) {
                  byte[] eo = Arrays.copyOf(e, e.length);
                  G.reverseComplement(eo);
                  return WritableComparator.compareBytes(e, 0, e.length, eo, 0, eo.length) <= 0
                      ? e : eo;
                }
              });
          log.info("wrote " + p.getSecond() + " smaller lex entries to " + AtableSmaller);
          if (ingested != p.getFirst() || p.getSecond() != totalsum)
            log.warn("SMALLER " + p.getFirst() + " not equal to ingested " + ingested + " or totalsum " + p.getSecond() + " not equal to totalsum " + totalsum);
        }

        // write degree
        BatchWriter bwd = mtbw.getBatchWriter(oTsampleDegree);
        Mutation m = new Mutation(sampleidb);
        m.put(EMPTY_BYTES, ("degree").getBytes(UTF_8), (""+totalsum).getBytes(UTF_8));
        bwd.addMutation(m);
        log.info("wrote "+totalsum+" as degree of "+sampleid+" to "+oTsampleDegree);


      } catch (Exception e) {
        log.warn("",e);
      } finally {
        if (mtbw != null)
          try {
            mtbw.close();
          } catch (MutationsRejectedException e) {
            log.warn("Mutation rejected at close()", e);
          }
      }
    }

    private interface SpecificKmerAction {
      byte[] transformKmer(byte[] mer);
    }

    public abstract Pair<Long,Long> ingestMap(T map, BatchWriter bw, byte[] sampleidb,
                                       SpecificKmerAction specificKmerAction) throws MutationsRejectedException;

    public static class VariableMap extends IngestIntoAccumulo<SortedMap<ArrayHolder, Integer>> {

      public VariableMap(Connector connector, String atable, String oTsampleDegree, boolean alsoIngestReverseComplement, boolean alsoIngestSmallerLex, int k) {
        super(connector, atable, oTsampleDegree, alsoIngestReverseComplement, alsoIngestSmallerLex, k);
      }

      public Pair<Long, Long> ingestMap(SortedMap<ArrayHolder, Integer> map, BatchWriter bw, byte[] sampleidb,
                                        SpecificKmerAction specificKmerAction) throws MutationsRejectedException {
        long ingested = 0, totalsum = 0;
        for (Map.Entry<ArrayHolder, Integer> entry : map.entrySet()) {
          Mutation m = new Mutation(specificKmerAction == null ? entry.getKey().b : specificKmerAction.transformKmer(entry.getKey().b));
          long lv = entry.getValue().longValue();
          totalsum += lv;
          m.put(EMPTY_BYTES, sampleidb, LEX.encode(lv));
          bw.addMutation(m);
          ingested++;
        }
        return new Pair<>(ingested, totalsum);
      }
    }

    public static class IntegerMap extends IngestIntoAccumulo<int[]> {

      public IntegerMap(Connector connector, String atable, String oTsampleDegree, boolean alsoIngestReverseComplement, boolean alsoIngestSmallerLex, int k) {
        super(connector, atable, oTsampleDegree, alsoIngestReverseComplement, alsoIngestSmallerLex, k);
      }

      public Pair<Long, Long> ingestMap(int[] map, BatchWriter bw, byte[] sampleidb,
                                        SpecificKmerAction specificKmerAction) throws MutationsRejectedException {
        long ingested = 0, totalsum = 0;
        for (int idx = 0; idx < map.length; idx++) {
          int ival = map[idx];
          if (ival == 0)
            continue;
          byte[] idxBytes = G.intToBytes(Integer.reverse(idx));
          Mutation m = new Mutation(specificKmerAction == null ? idxBytes : specificKmerAction.transformKmer(idxBytes));
          totalsum += ival;
          m.put(EMPTY_BYTES, sampleidb, LEX.encode((long) ival));
          bw.addMutation(m);
          ingested++;
        }
        return new Pair<>(ingested, totalsum);
      }
    }
  }


  protected final KmerAction<T> action;
  protected final GenomicEncoder G;
  public final int K;


  public CSVIngesterKmer(int K, KmerAction<T> action) {
    this.K = K;
    G = new GenomicEncoder(K);
    this.action = action;
    map = allocateMap();
  }

  //  static final Text EMPTY_TEXT = new Text();

  public static final class ArrayHolder implements Comparable<ArrayHolder> {
    public final byte[] b;

    ArrayHolder(byte[] b) {
      this.b = b;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ArrayHolder that = (ArrayHolder) o;
      return Arrays.equals(b, that.b);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(b);
    }

    @Override
    public int compareTo(ArrayHolder o) {
      return WritableComparator.compareBytes(b, 0, b.length, o.b, 0, o.b.length);
    }
  }

  private T map;

  private long ingestLine(String line) {
//    String[] parts = line.split(",");
    int comma = line.indexOf(',');
    if (comma == -1) {
      log.warn("Bad CSV line: "+line);
      return 0;
    }

    // Find the '/' character before the comma.
    // If the character after it is '2', then take the reverse complement.
    String header = line.substring(0, comma-1);
    int slash = header.indexOf('/');
    boolean reverse = slash != -1 && slash != header.length()-1 && header.charAt(slash+1) == '2';

    String seq = line.substring(comma+1);
    char[] seqb = seq.toCharArray();

    // split seq into kmers
    long num = 0;
    int i = 0;
    // skip 'N' logic
    outer: while (true) {
      for (int j = i; j < i + K; j++)
        if (seqb[j] == 'N') {
          i = j + 1;
          continue outer;
        }
      break;
    }

    for (; i < seqb.length-K; i++) {
      if (seqb[i + K - 1] == 'N') {
        i += K;
        outer: while (true) {
          for (int j = i; j < Math.min(i + K, seqb.length); j++)
            if (seqb[j] == 'N') {
              i = j + 1;
              continue outer;
            }
          break;
        }
        i--; continue;
      }

      byte[] e = G.encode(seqb, i);
      if (reverse)
        G.reverseComplement(e);
      putInMap(map, e);
      num++;
    }
    return num;
  }

  public abstract void putInMap(T map, byte[] e);

  public abstract T allocateMap();

  public abstract void clearMap(T map);


  public long ingestFile(File file) throws IOException {
    return ingestFile(file, 1, 0);
  }


  public long ingestFile(File file, int everyXLines, int startOffset) throws IOException {
    Preconditions.checkArgument(everyXLines >= 1 && startOffset >= 0, "bad params ", everyXLines, startOffset);

    String sampleid0 = file.getName();
    if (sampleid0.endsWith(".csv"))
      sampleid0 = sampleid0.substring(0, sampleid0.length()-4);


    String line;
    long entriesProcessed = 0, ingested = 0, totalsum = 0;

    try (BufferedReader fo = new BufferedReader(new FileReader(file))) {
      // Skip header line
      fo.readLine();
      // Offset
      for (int i = 0; i < startOffset; i++)
        fo.readLine();

      // log every 2 minutes
      StatusLogger slog = new StatusLogger();
      String partialMsg = file.getName()+": entries processed: ";

      clearMap(map);

      long linecnt = 0;
      while ((line = fo.readLine()) != null)
        if (!line.isEmpty() && linecnt++ % everyXLines == 0) {
          entriesProcessed += ingestLine(line);
          if (linecnt % 5 == 0)
            slog.logPeriodic(log, partialMsg+entriesProcessed);
        }

      log.info("Finished putting "+sampleid0+" into an in-memory map");

      action.run(sampleid0, map);

    }
    return ingested;
  }


  public static class VariableMap extends CSVIngesterKmer<SortedMap<ArrayHolder, Integer>> {

    public VariableMap(int K, KmerAction<SortedMap<ArrayHolder, Integer>> action) {
      super(K, action);
    }

    @Override
    public void putInMap(SortedMap<ArrayHolder, Integer> map, byte[] e) {
        ArrayHolder ah = new ArrayHolder(e);
        Integer curval = map.get(ah);
        int newval = curval == null ? 1 : curval + 1;
        map.put(ah, newval);
    }

    @Override
    public SortedMap<ArrayHolder, Integer> allocateMap() {
      return new TreeMap<>();
    }

    @Override
    public void clearMap(SortedMap<ArrayHolder, Integer> map) {
      map.clear();
    }
  }

  public static class IntegerMap extends CSVIngesterKmer<int[]> {

    public IntegerMap(int K, KmerAction<int[]> action) {
      super(K, action);
    }

    @Override
    public void putInMap(int[] map, byte[] e) {
      map[Integer.reverse(bytesToInt(e))]++;
    }

    @Override
    public int[] allocateMap() {
      return new int[1 << (K << 1)];
    }

    @Override
    public void clearMap(int[] map) {
      Arrays.fill(map, 0);
    }
  }

}
