package edu.mit.ll.graphulo_ocean;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.examples.ExampleUtil;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

/**
 * Test the ocean genomics pipeline on a small file.
 */
public class OceanTest extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(OceanTest.class);

  public static final int kmer = 11;

  @Test
  public void runPipeline() throws Exception {
    String tSampleIDSeqID = "oceantest_TsampleSeq",
        tSampleID = "oceantest_Tsample",
        tSampleDistance = "oceantest_TsampleDis";

    ingestFiles(tSampleIDSeqID);
    sumToSample(tSampleIDSeqID, tSampleID);
    doBrayCurtis(tSampleID, tSampleDistance);

    GraphuloUtil.deleteTables(tester.getConnector(), tSampleIDSeqID, tSampleID, tSampleDistance);
  }

  private void ingestFiles(String tSampleIDSeqID) throws Exception {
    Connector conn = tester.getConnector();
    GraphuloUtil.deleteTables(conn, tSampleIDSeqID);
//    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
//        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    CSVIngester ingester = new CSVIngester(conn);
    long numSeqs = ingester.ingestFile(ExampleUtil.getDataFile("S0001_n1000.csv"), tSampleIDSeqID, true);
    numSeqs += ingester.ingestFile(ExampleUtil.getDataFile("S0002_n1000.csv"), tSampleIDSeqID, false);
    log.info("number of sequences ingested: "+numSeqs);

//    Assert.assertEquals(expect, actual);

//    conn.tableOperations().delete(tSampleIDSeqID);
//    conn.tableOperations().delete(tR);
  }


  private void sumToSample(String tSampleIDSeqID, String tSampleID) {
    Connector conn = tester.getConnector();
    GraphuloUtil.deleteTables(conn, tSampleID);
    DynamicIteratorSetting dis = new DynamicIteratorSetting(1, null)
        .append(ValToColApply.iteratorSetting(1))
        .append(KMerColQApply.iteratorSetting(1, kmer));

    Graphulo g = new Graphulo(conn, tester.getPassword());
    long numUniqueKMersPerSample = g.OneTable(tSampleIDSeqID, tSampleID, null, null, -1, null, null, Graphulo.PLUS_ITERATOR_LONG,
        null, null, dis.getIteratorSettingList(), null, null);
    log.info("numUniqueKMersPerSample = "+numUniqueKMersPerSample);
  }

  private void doBrayCurtis(String tSampleID, String tSampleDistance) {
    Connector conn = tester.getConnector();
    GraphuloUtil.deleteTables(conn, tSampleDistance);
    Graphulo g = new Graphulo(conn, tester.getPassword());
    long numSamplePairings = g.cartesianProductBrayCurtis(tSampleID, tSampleDistance,
        CartesianDissimilarityIterator.DistanceType.BRAY_CURTIS);
    log.info("numSamplePairings = "+numSamplePairings);
  }

  @Test
  public void ingestKmersAndDist() throws Exception {
    final Connector conn = tester.getConnector();
    final String tKmer = "oceantest_Tkmer";
    final String tKmerDeg = "oceantest_TkmerDeg";
    String tDist = "oceantest_TsampleDist";
    GraphuloUtil.deleteTables(conn, tKmer, tKmerDeg, tDist);

    ingestKmers(conn, tKmer, tKmerDeg, new Creator<SortedMap<CSVIngesterKmer.ArrayHolder, Integer>>() {
      @Override
      public CSVIngesterKmer<SortedMap<CSVIngesterKmer.ArrayHolder, Integer>> create(int kmer) {
        return new CSVIngesterKmer.VariableMap(kmer, new CSVIngesterKmer.IngestIntoAccumulo.VariableMap(
            conn, tKmer, tKmerDeg, false, false, kmer
        ));
      }
    });
    doDist(conn, tKmer, tKmerDeg, tDist);

    // test repeated add
    final String tKmer2 = tKmer+"2";
    final String tKmerDeg2 = tKmerDeg+"2";
//    String tDist2 = tDist+"2";
    GraphuloUtil.deleteTables(conn, tKmer2, tKmerDeg2);
    conn.tableOperations().clone(tKmer, tKmer2, true, null, null);

    ingestKmers(conn, tKmer2, tKmerDeg2, new Creator<int[]>() {
      @Override
      public CSVIngesterKmer<int[]> create(int kmer) {
        return new CSVIngesterKmer.IntegerMap(kmer, new CSVIngesterKmer.IngestIntoAccumulo.IntegerMap(
            conn, tKmer, tKmerDeg, false, false, kmer
        ));
      }
    });

    GraphuloUtil.deleteTables(tester.getConnector(), tKmer, tKmerDeg, tDist, tKmer2); //, tKmerDeg2);
  }

  private interface Creator<T> {
    CSVIngesterKmer<T> create(int kmer);
  }

  private void ingestKmers(Connector conn, String tKmer, String tKmerDeg, Creator<?> creator) throws Exception {
//    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
//        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    CSVIngesterKmer ingester = creator.create(kmer);
    long numSeqs = ingester.ingestFile(ExampleUtil.getDataFile("S0001_n1000.csv"));
    numSeqs += ingester.ingestFile(ExampleUtil.getDataFile("S0002_n1000.csv"));
    log.info("number of sequences ingested: "+numSeqs);
  }


  private void doDist(Connector conn, String tKmer, String tKmerDeg, String tDist) throws Exception {
    OceanDistanceCalc odc = new OceanDistanceCalc();
    OceanDistanceCalc.Opts opts = new OceanDistanceCalc.Opts();
    opts.oTsampleDist = tDist;
    opts.oTsampleDegree = tKmerDeg;
    opts.oTsampleSeqRaw = tKmer;
    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    odc.executeGraphulo(graphulo, opts);

    Map<Key,Value> map = new HashMap<>();
    TestUtil.scanTableToMap(conn, opts.oTsampleDist, map);
    Map.Entry<Key, Value> actual = Iterators.getOnlyElement(map.entrySet().iterator());
    Assert.assertTrue(actual.getKey().equals(new Key("S0001_n1000", "", "S0002_n1000"), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
    Assert.assertEquals(0.9473744643185487, Double.parseDouble(actual.getValue().toString()), 1e-11);
  }

}