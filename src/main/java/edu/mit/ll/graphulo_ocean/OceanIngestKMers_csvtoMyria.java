package edu.mit.ll.graphulo_ocean;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import com.google.common.base.Preconditions;
import com.google.common.io.PatternFilenameFilter;
import edu.mit.ll.graphulo_ocean.parfile.ParallelFileMapper;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

/**
 * Ex: java -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanIngestKMers_csvtoMyria -myriaHost node-109 -inputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt" -K 11 -numthreads 1 -lockDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt_upload_claim" -outputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt_upload"
 * Ex: java -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanIngestKMers_csvtoMyria -myriaHost node-109 -inputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_11_cnt" -K 11 -numthreads 1 -lockDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_11_cnt_upload_claim" -outputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_11_cnt_upload"
 */
public class OceanIngestKMers_csvtoMyria {
  private static final Logger log = LoggerFactory.getLogger(OceanIngestKMers_csvtoMyria.class);

  public static void main(String[] args) {
    executeNew(args);
  }

  public static int executeNew(String[] args) { return new OceanIngestKMers_csvtoMyria().execute(args); }

  private static class Opts extends Help {
//    @Parameter(names = {"-listOfSamplesFile"}, required = true)
//    public String listOfSamplesFile;

//    @Parameter(names = {"-everyXLines"})
//    public int everyXLines = 1;

//    @Parameter(names = {"-startOffset"})
//    public int startOffset = 0;

    @Parameter(names = {"-K"}, required = true)
    public int K;

    @Parameter(names = {"-inputDir"}, required = true, converter = FileConverter.class)
    public File inputDir;

    @Parameter(names = {"-lockDir"}, converter = FileConverter.class)
    public File lockDir;

    @Parameter(names = {"-outputDir"}, converter = FileConverter.class)
    public File outputDir;

    @Parameter(names = {"-numthreads"})
    public int numthreads = 1;

    @Parameter(names = {"-myriaHost"}, description = "example: node-109", required = true)
    public String myriaHost;

    @Parameter(names = {"-myriaPort"}, description = "example: 8753")
    public int port = 8753;
  }

  /** @return Number of files processed */
  public int execute(final String[] args) {
    final Opts opts = new Opts();
    opts.parseArgs(OceanIngestKMers_csvtoMyria.class.getName(), args);
    log.info(OceanIngestKMers_csvtoMyria.class.getName() + " " + opts);
    Preconditions.checkArgument(opts.inputDir.exists() && opts.inputDir.isDirectory(), "input dir does not exist");
    if (opts.lockDir == null)
      opts.lockDir = new File(opts.inputDir, "lockDir_upload");
    if (opts.outputDir == null)
      opts.outputDir = new File(opts.inputDir, "outputDir_upload");
    if (!opts.outputDir.exists()) {
      try {
        Thread.sleep((long)(Math.random() * 1000));
      } catch (InterruptedException e) {
        log.warn("",e);
      }
      if (!opts.outputDir.exists())
        //noinspection ResultOfMethodCallIgnored
        opts.outputDir.mkdirs();
    }
    final GenomicEncoder G = new GenomicEncoder(opts.K);

//    final String url = opts.myriaHostAndPort+"/dataset";

    final ParallelFileMapper.FileAction fileAction = new ParallelFileMapper.FileAction() {
      @Override
      public void run(File f) {
        log.info(Thread.currentThread().getName()+": Processing "+f.getName());
        final String sampleid = f.getName().substring(0,5);
        final String json =
            "{" +
                "  \"relationKey\" : {" +
                "    \"userName\" : \"public\"," +
                "    \"programName\" : \"adhoc\"," +
                "    \"relationName\" : \"kmercnt_"+opts.K+"_forward_"+sampleid+"\"" +
                "  }," +
                "  \"schema\" : {" +
                "    \"columnTypes\" : [\"STRING_TYPE\", \"LONG_TYPE\"]," +
                "    \"columnNames\" : [\"kmer\", \"cnt\"]" +
                "  }," +
                "  \"source\" : {" +
                "    \"dataType\" : \"File\"," +
                "    \"filename\" : \""+f.getAbsolutePath()+"\"" +
                "  }," +
                "  \"overwrite\" : false," +
                "  \"delimiter\": \",\"" +
                "}";

        HttpURLConnection conn = null;
        File outFile = new File(opts.outputDir, f.getName()+"_upload_response");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile))) {

          conn = (HttpURLConnection) new URL("http", opts.myriaHost, opts.port, "/dataset").openConnection();
          conn.setRequestMethod("POST");
          conn.addRequestProperty("Content-type", "application/json");
          conn.setDoInput(true);
          conn.setDoOutput(true);

          try (OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream())) {
            wr.write(json);
            wr.flush();
          }

          java.io.InputStream is = conn.getResponseCode() >= 400 ? conn.getErrorStream() : conn.getInputStream();

          try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = reader.readLine()) != null) {
              writer.write(line+'\n');
            }
          }

        } catch (IOException e) {
          throw new RuntimeException(e);
        } finally {
          if (conn != null)
            conn.disconnect();
        }
        log.info(Thread.currentThread().getName()+": Finished "+f.getName());
      }
    };

    @SuppressWarnings("ConstantConditions")
    List<File> inputFiles = Arrays.asList(opts.inputDir.listFiles(new PatternFilenameFilter("S\\d\\d\\d\\d.*\\.csv$")));

    Thread[] threads = new Thread[opts.numthreads];
    for (int i = 0; i < threads.length; i++)
      threads[i] = new Thread(new ParallelFileMapper(inputFiles, opts.lockDir, fileAction), "t" + i);
    for (Thread t : threads)
      t.start();
    for (Thread t : threads)
      try {
        t.join();
      } catch (InterruptedException e) {
        log.warn("while waiting for thread "+t, e);
      }

    return inputFiles.size();
  }

}
