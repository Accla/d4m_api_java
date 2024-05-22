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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 java -cp "/home/dhutchis/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanIngestKMers_partocsv -inputDir "/home/dhutchis/gits/istc_oceanography/parse_fastq" -K 11
 java -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanIngestKMers_partocsv -inputDir "/home/gridsan/dhutchison/gits/istc_oceanography/parse_fastq" -K 11
 java -Xms512m -Xmx4g -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanIngestKMers_partocsv -inputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed" -K 11 -numthreads 10 -lockDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt_claim" -outputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt"
 java -Xms512m -Xmx4g -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanIngestKMers_partocsv -inputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped" -K 11 -numthreads 10 -lockDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_11_cnt_claim" -outputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_11_cnt"
 java -Xms10g -Xmx20g -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanIngestKMers_partocsv -inputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed" -K 13 -numthreads 8 -lockDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_13_cnt_claim" -outputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_13_cnt"
 java -Xms10g -Xmx20g -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanIngestKMers_partocsv -inputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped" -K 13 -numthreads 8 -lockDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_13_cnt_claim" -outputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_13_cnt"
 java -Xms17g -Xmx22g -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanIngestKMers_partocsv -inputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed" -K 15 -numthreads 3 -lockDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_15_cnt_claim" -outputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_15_cnt"
 java -Xms17g -Xmx22g -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanIngestKMers_partocsv -inputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped" -K 15 -numthreads 3 -lockDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_15_cnt_claim" -outputDir "/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_15_cnt"
 To remove claim files for missing files:
 export K=13
 export NOL=""
 diff <(ls -1 "parsed${NOL}_${K}_cnt/" | awk -F'_' '{print $1}') <(ls -1 "parsed${NOL}_${K}_cnt_claim/" | awk -F'.csv' '{print $1}') | awk -F' ' '{print $2}' | sed '/^$/d' | while read i; do rm "parsed${NOL}_${K}_cnt_claim/${i}.csv.claim"; done
 */
public class OceanIngestKMers_partocsv {
  private static final Logger log = LoggerFactory.getLogger(OceanIngestKMers_partocsv.class);

  public static void main(String[] args) {
    executeNew(args);
  }

  public static int executeNew(String[] args) { return new OceanIngestKMers_partocsv().execute(args); }

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

    @Override
    public String toString() {
      return "Opts{" +
          "K=" + K +
          ", inputDir=" + inputDir +
          ", lockDir=" + lockDir +
          ", outputDir=" + outputDir +
          ", numthreads=" + numthreads +
          '}';
    }
  }

  /** @return Number of files processed */
  public int execute(final String[] args) {
    final Opts opts = new Opts();
    opts.parseArgs(OceanIngestKMers_partocsv.class.getName(), args);
    log.info(OceanIngestKMers_partocsv.class.getName() + " " + opts);
    Preconditions.checkArgument(opts.inputDir.exists() && opts.inputDir.isDirectory(), "input dir does not exist");
    if (opts.lockDir == null)
      opts.lockDir = new File(opts.inputDir, "lockDir_cnt");
    if (opts.outputDir == null)
      opts.outputDir = new File(opts.inputDir, "outputDir_cnt");
    //noinspection Duplicates
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

    final CSVIngesterKmer.KmerAction<SortedMap<CSVIngesterKmer.ArrayHolder, Integer>> kmerActionBigK =
        new CSVIngesterKmer.KmerAction<SortedMap<CSVIngesterKmer.ArrayHolder, Integer>>() {
          private char[] charBuffer = new char[G.K];
          @Override
          public void run(String sampleid, SortedMap<CSVIngesterKmer.ArrayHolder, Integer> map) {
            File outFile = new File(opts.outputDir, sampleid+"_"+opts.K+"_cnt.csv");

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile))) {
              for (Map.Entry<CSVIngesterKmer.ArrayHolder, Integer> entry : map.entrySet()) {
                writer.write(G.decode(entry.getKey().b, charBuffer));
                writer.write(','+entry.getValue().toString()+'\n');
              }
            } catch (IOException e) {
              log.error("error writing to file "+outFile, e);
            }
          }
        };

    final CSVIngesterKmer.KmerAction<int[]> kmerActionLittleK =
        new CSVIngesterKmer.KmerAction<int[]>() {
          private byte[] idxBytes = new byte[G.NB];
          private char[] charBuffer = new char[G.K];

          @Override
          public void run(String sampleid, int[] map) {
            File outFile = new File(opts.outputDir, sampleid+"_"+opts.K+"_cnt.csv");

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile))) {
              for (int idx = 0; idx < map.length; idx++) {
                int ival = map[idx];
                if (ival == 0)
                  continue;
                writer.write(G.decode(G.intToBytes(Integer.reverse(idx), idxBytes), charBuffer));
                writer.write(","+Integer.toString(ival)+"\n");
              }
            } catch (IOException e) {
              log.error("error writing to file "+outFile, e);
            }
          }
        };


    @SuppressWarnings("ConstantConditions")
    List<File> inputFiles = Arrays.asList(opts.inputDir.listFiles(new PatternFilenameFilter(".*\\.csv$")));

    Thread[] threads = new Thread[opts.numthreads];
    for (int i = 0; i < threads.length; i++)
      threads[i] = new Thread(new ParallelFileMapper(inputFiles, opts.lockDir,
          new ParallelFileMapper.FileAction() {
            private CSVIngesterKmer<?> ingester =
                opts.K <= 15 ?
                    new CSVIngesterKmer.IntegerMap(opts.K, kmerActionLittleK) :
                    new CSVIngesterKmer.VariableMap(opts.K, kmerActionBigK);

            @Override
            public void run(File f) {
              try {
                try {
                  Thread t = Thread.currentThread();
                  t.setName(t.getName().substring(0,t.getName().indexOf(' ')+1)+f.getName());
                } catch (SecurityException ignored) {}
                ingester.ingestFile(f);
              } catch (Exception e) {
                log.error("error while processing file "+f, e);
              }
            }
          }),
          "t" + i+" ");
    final long startTime = System.currentTimeMillis();
    for (Thread t : threads) {
      t.start();
      try {
        Thread.sleep((long) (Math.random()*200));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    for (Thread t : threads)
      try {
        t.join();
        log.info("Thread "+t.getName()+" finished. Duration: "+(System.currentTimeMillis()-startTime)/1000+" sec");
      } catch (InterruptedException e) {
        log.warn("while waiting for thread "+t, e);
      }

    return inputFiles.size();
  }

}
