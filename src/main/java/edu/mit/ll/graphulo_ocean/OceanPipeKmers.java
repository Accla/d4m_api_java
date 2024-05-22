package edu.mit.ll.graphulo_ocean;

import com.beust.jcommander.Parameter;
import edu.mit.ll.graphulo.util.StatusLogger;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.ConsoleAppender;

//import org.apache.log4j.Level;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//import org.apache.log4j.SimpleLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

/**
 * Ex: java -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanPipeKmers -myriaHost node-109 -K 11 -numthreads 1 -lockDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt_upload_claim" -outputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt_upload" < "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt"
 * Ex: java -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanIngestKMers_csvtoMyria -myriaHost node-109 -inputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_11_cnt" -K 11 -numthreads 1 -lockDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_11_cnt_upload_claim" -outputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_11_cnt_upload"
 */
public class OceanPipeKmers {
  private static final Logger log = LoggerFactory.getLogger(OceanPipeKmers.class);
  private final StatusLogger slog = new StatusLogger();

  public static void main(String[] args) {
    //LogManager.resetConfiguration();
    //log.removeAllAppenders();
    //log.addAppender(new ConsoleAppender(new SimpleLayout(), ConsoleAppender.SYSTEM_ERR));
    //log.setLevel(Level.INFO);
    executeNew(args);
  }

  public static int executeNew(String[] args) { return new OceanPipeKmers().execute(args, System.in, System.out); }

  private static class Opts extends Help {
//    @Parameter(names = {"-listOfSamplesFile"}, required = true)
//    public String listOfSamplesFile;

//    @Parameter(names = {"-everyXLines"})
//    public int everyXLines = 1;

//    @Parameter(names = {"-startOffset"})
//    public int startOffset = 0;

    @Parameter(names = {"-K"}, required = true)
    public int K;

    @Parameter(names = {"-rc"}, description = "reverseComplement")
    public boolean rc = false;

    @Parameter(names = {"-lex"}, description = "lesserLex; overrides -rc")
    public boolean lex = false;

    // Myria does not support the binary format with variable-length strings
    // Myria could be modified to support this; see BinaryFileScan.java in Myria
    @Parameter(names = {"-binary"})
    public boolean binary = false;

    @Override
    public String toString() {
      return "Opts{" +
          "K=" + K +
          ", rc=" + rc +
          ", lex=" + lex +
          ", binary=" + binary +
          '}';
    }
  }

  enum ReadMode { FORWARD, RC, LEX }

  /** @return Number of lines processed */
  @SuppressWarnings("ConstantConditions")
  public int execute(final String[] args, final InputStream in, final PrintStream out) {
    final Opts opts = new Opts();
    opts.parseArgs(OceanPipeKmers.class.getName(), args);
    log.info(OceanPipeKmers.class.getName() + " " + opts);
    final ReadMode readMode;
    if (opts.lex)
      readMode = ReadMode.LEX;
    else if (opts.rc)
      readMode = ReadMode.RC;
    else
      readMode = ReadMode.FORWARD;
    final GenomicEncoder G = new GenomicEncoder(opts.K);

    int linesProcessed = 0;
    try (
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        PrintStream writer = out
    ) {
      String line;
      byte[] enc = new byte[G.NB], encrc = new byte[G.NB];
      char[] dec = new char[G.K];
      final DataOutputStream dos = opts.binary ? new DataOutputStream(writer) : null;

      while ((line = reader.readLine()) != null) {

        switch (readMode) {
          case FORWARD:
//            System.err.println("WRITE: "+line.substring(0,opts.K)+" with long: "+Long.parseLong(line.substring(opts.K+1)));
            if (opts.binary) {
              dos.writeUTF(line.substring(0,opts.K));
              dos.writeLong(Long.parseLong(line.substring(opts.K+1)));
            } else
              writer.println(line);
            break;
          default:
            char[] linearr = line.toCharArray();

            enc = G.encode(linearr, 0, enc);
            switch (readMode) {
              case RC:
                enc = G.reverseComplement(enc);
                dec = G.decode(enc, dec);
                break;
              case LEX:
                System.arraycopy(enc, 0, encrc, 0, G.NB);
                encrc = G.reverseComplement(encrc);
                dec = G.decode(WritableComparator.compareBytes(enc, 0, G.NB, encrc, 0, G.NB) <= 0
                    ? enc : encrc, dec);
                break;
              default: throw new AssertionError();
            }
            if (opts.binary) {
              dos.writeUTF(String.valueOf(dec));
              dos.writeLong(Long.parseLong(line.substring(opts.K+1)));
            } else {
              System.arraycopy(dec, 0, linearr, 0, dec.length);
              writer.println(linearr);
            }
        }
        linesProcessed++;
        if (linesProcessed % 1_000 == 0)
          slog.logPeriodic(log, "Processed lines: "+linesProcessed);
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
    return linesProcessed;
  }

  private static int findComma(char[] arr, int start) {
    for (int i = start == -1 ? 0 : start; i < arr.length; i++)
      if (arr[i] == ',')
        return i;
    return -1;
  }

}
