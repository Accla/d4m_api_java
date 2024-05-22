package edu.mit.ll.graphulo_ocean;

import com.beust.jcommander.Parameter;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Execute this in a directory that can see all the sample files.
 * Ex: java -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanIngestKMers -listOfSamplesFile "/home/gridsan/dhutchison/gits/istc_oceanography/metadata/test_one_sample_filename.csv"
 * cd /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed
 * Ex: java -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanIngestKMers -listOfSamplesFile "/home/gridsan/dhutchison/gits/istc_oceanography/metadata/valid_samples_GA02_filenames_perm.csv" -everyXLines 2 -startOffset 0 -K 11 -oTsampleDegree oTsampleDegree | tee "$HOME/node-043-ingest.log"
 * createtable oTsampleSeqRaw
 * addsplits C T G
 */
public class OceanIngestKMers {
  private static final Logger log = LoggerFactory.getLogger(OceanIngestKMers.class);

  public static void main(String[] args) {
    executeNew(args);
  }

  public static int executeNew(String[] args) { return new OceanIngestKMers().execute(args); }

  private static class Opts extends Help {
    @Parameter(names = {"-listOfSamplesFile"}, required = true)
    public String listOfSamplesFile;

    @Parameter(names = {"-oTsampleSeqRaw"})
    public String oTsampleSeqRaw = "oTsampleSeqRaw";

    @Parameter(names = {"-everyXLines"})
    public int everyXLines = 1;

    @Parameter(names = {"-startOffset"})
    public int startOffset = 0;

    @Parameter(names = {"-txe1"})
    public String txe1 = "classdb54";

    @Parameter(names = {"-K"}, required = true)
    public int K;

    @Parameter(names = {"-oTsampleDegree"})
    public String oTsampleDegree = "oTsampleDegree";

    @Parameter(names = {"-alsoIngestReverseComplement"})
    public boolean alsoIngestReverseComplement = false;

    @Parameter(names = {"-alsoIngestSmallerLex"},
        description = "For example, store for ACG but not CGT, since ACG < CGT.")
    public boolean alsoIngestSmallerLex = false;

    @Override
    public String toString() {
      return "Opts{" +
          "listOfSamplesFile='" + listOfSamplesFile + '\'' +
          ", oTsampleSeqRaw='" + oTsampleSeqRaw + '\'' +
          ", everyXLines=" + everyXLines +
          ", startOffset=" + startOffset +
          ", txe1='" + txe1 + '\'' +
          ", K=" + K +
          ", oTsampleDegree='" + oTsampleDegree + '\'' +
          ", alsoIngestReverseComplement=" + alsoIngestReverseComplement +
          ", alsoIngestSmallerLex=" + alsoIngestSmallerLex +
          '}';
    }
  }

  /** @return Number of files processed */
  public int execute(final String[] args) {
    Opts opts = new Opts();
    opts.parseArgs(OceanIngestKMers.class.getName(), args);
    log.info(OceanIngestKMers.class.getName() + " " + opts);

    Connector conn = setupTXE1Connector(opts.txe1);

    return ingestFileList(conn, opts);
  }

  static Connector setupTXE1Connector(String txe1) {
    return setupTXE1Connector(txe1, getTXE1Authentication(txe1));
  }

  static Connector setupTXE1Connector(String txe1, AuthenticationToken auth) {
    String instanceName = txe1;
    String zookeeperHost = txe1+".cloud.llgrid.txe1.mit.edu:2181";
    ClientConfiguration cc = new ClientConfiguration().withInstance(instanceName).withZkHosts(zookeeperHost);// .withZkTimeout(timeout)
    Instance instance = new ZooKeeperInstance(cc);
    try {
      return instance.getConnector("AccumuloUser", auth);
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new RuntimeException("Trouble authenticating to database "+txe1,e);
    }
  }


  static AuthenticationToken getTXE1Authentication(String txe1) {
    File file = new File("/home/gridsan/groups/databases/"+txe1+"/accumulo_user_password.txt");
    PasswordToken token;
    try (BufferedReader is = new BufferedReader(new FileReader(file))) {
      token = new PasswordToken(is.readLine());
    } catch (FileNotFoundException e) {
      log.error("Cannot find accumulo_user_password.txt for instance "+txe1, e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      log.error("Problem reading accumulo_user_password.txt for instance " + txe1, e);
      throw new RuntimeException(e);
    }
    return token;
  }

  /** @return Number of files processed */
  private int ingestFileList(Connector conn, Opts opts) {
    try (BufferedReader fo = new BufferedReader(new FileReader(opts.listOfSamplesFile))) {
      for (int i = 0; i < opts.startOffset; i++)
        fo.readLine();

      String line;
      long linecnt = 0;
      int filesprocessed = 0;
      long entriesProcessed = 0;
      CSVIngesterKmer<?> ingester =
          opts.K <= 15 ?
              new CSVIngesterKmer.IntegerMap(opts.K, new CSVIngesterKmer.IngestIntoAccumulo.IntegerMap(
                  conn, opts.oTsampleSeqRaw, opts.oTsampleDegree, opts.alsoIngestReverseComplement, opts.alsoIngestSmallerLex, opts.K
              )) :
              new CSVIngesterKmer.VariableMap(opts.K, new CSVIngesterKmer.IngestIntoAccumulo.VariableMap(
                  conn, opts.oTsampleSeqRaw, opts.oTsampleDegree, opts.alsoIngestReverseComplement, opts.alsoIngestSmallerLex, opts.K
              ));
      while ((line = fo.readLine()) != null)
        if (!line.isEmpty() && linecnt++ % opts.everyXLines == 0) {
          log.info("Starting file: "+line);
          File file = new File(line);
          if (!file.exists() || !file.canRead()) {
            log.warn("Problem with file: "+file.getName());
            continue;
          }

          long ep = ingester.ingestFile(file);
          entriesProcessed += ep;
          log.info("Finished file: "+line+"; entries ingested: "+ep+"; cummulative: "+entriesProcessed);
          filesprocessed++;
        }
      log.info("Finished all "+filesprocessed+" files! (every "+opts.everyXLines+" lines; offset "+opts.startOffset+")");
      return filesprocessed;
    } catch (IOException e) {
      throw new RuntimeException("",e);
    }
  }

}
