package edu.mit.ll.graphulo_ocean;

import com.beust.jcommander.Parameter;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.skvi.DoubleCombiner;
import edu.mit.ll.graphulo.skvi.DoubleDecodeIterator;
import edu.mit.ll.graphulo.skvi.DoubleSummingCombiner;
import edu.mit.ll.graphulo.skvi.LruCacheIterator;
import edu.mit.ll.graphulo.skvi.TwoTableIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.mit.ll.graphulo_ocean.OceanIngestKMers.getTXE1Authentication;
import static edu.mit.ll.graphulo_ocean.OceanIngestKMers.setupTXE1Connector;

/**
 * Executable.
 * Ex: java -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanDistanceCalc -oTsampleDegree oTsampleDegree -oTsampleDist oTsampleDist -oTsampleSeqRaw oTsampleSeqRaw | tee "$HOME/node-043-dist.log"
 * createtable oTsampleDist
 * addsplits S0004 S0010 S0027
 * <p>
 * This performs a min.+ matrix multiply on the kmer table with itself, restricting output to the strict upper triangle.
 * Because the resulting table is small, I added an optimization to hold the whole output table in memory
 * for complete pre-summing.
 */
public class OceanDistanceCalc {
  private static final Logger log = LoggerFactory.getLogger(OceanDistanceCalc.class);

  public static void main(String[] args) {
    new OceanDistanceCalc().execute(args);
  }

  static class Opts extends Help {
    @Parameter(names = {"-oTsampleSeqRaw"})
    public String oTsampleSeqRaw = "oTsampleSeqRaw";

    @Parameter(names = {"-txe1"})
    public String txe1 = "classdb54";

    @Parameter(names = {"-oTsampleDegree"})
    public String oTsampleDegree = "oTsampleDegree";

    @Parameter(names = {"-oTsampleDist"})
    public String oTsampleDist = "oTsampleDist";

    /** In D4M syntax. For example: S0001,:,S0002, */
    @Parameter(names = {"-sampleFilter"})
    public String sampleFilter = null;

    @Override
    public String toString() {
      return "Opts{" +
          "oTsampleSeqRaw='" + oTsampleSeqRaw + '\'' +
          ", txe1='" + txe1 + '\'' +
          ", oTsampleDegree='" + oTsampleDegree + '\'' +
          ", oTsampleDist='" + oTsampleDist + '\'' +
          ", sampleFilter='" + sampleFilter + '\'' +
          '}';
    }
  }

  public void execute(final String[] args) {
    Opts opts = new Opts();
    opts.parseArgs(OceanDistanceCalc.class.getName(), args);
    log.info(OceanDistanceCalc.class.getName() + " " + opts);

    AuthenticationToken auth = getTXE1Authentication(opts.txe1);
    Connector conn = setupTXE1Connector(opts.txe1, auth);
    Graphulo graphulo = new Graphulo(conn, auth);
    executeGraphulo(graphulo, opts);
  }

  void executeGraphulo(Graphulo graphulo, Opts opts) {
    Map<String,String> opt = new HashMap<>();
    opt.put("rowMultiplyOp", DistanceRowMult.class.getName());
    opt.putAll(graphulo.basicRemoteOpts("rowMultiplyOp.opt.", opts.oTsampleDegree, null, null));
//    opt.put("rowMultiplyOp.opt.multiplyOp", multOp.getName()); // treated same as multiplyOp
//    if (multOpOptions != null)
//      for (Map.Entry<String, String> entry : multOpOptions.entrySet()) {
//        opt.put("rowMultiplyOp.opt.multiplyOp.opt."+entry.getKey(), entry.getValue()); // treated same as multiplyOp
//      }
//    opt.put("rowMultiplyOp.opt.rowmode", rowmode.name());

    // double combiner
    IteratorSetting dc = new IteratorSetting(1, DoubleSummingCombiner.class);
    DoubleCombiner.setEncodingType(dc, DoubleCombiner.Type.BYTE);
    Combiner.setCombineAllColumns(dc, true);

    List<IteratorSetting> itersAfterTT = new ArrayList<>();
    itersAfterTT.add(LruCacheIterator.combinerSetting(1, null, 50_000,
        DoubleSummingCombiner.class, dc.getOptions(), true)); // do a byte-wise combining inside the cache
    itersAfterTT.add(new IteratorSetting(1, DoubleDecodeIterator.class));


    long l = graphulo.TwoTable(TwoTableIterator.CLONESOURCE_TABLENAME, opts.oTsampleSeqRaw, opts.oTsampleDist, null,
        -1, TwoTableIterator.DOTMODE.ROW, opt, Graphulo.PLUS_ITERATOR_DOUBLE,
        null, opts.sampleFilter, opts.sampleFilter,
        false, false, null, null, itersAfterTT,
        null, null,
        -1, null, null);
    log.info("Number of entries written to distance table "+opts.oTsampleDist+": "+l);

//    IteratorSetting isDistFinish = DistanceApply.iteratorSetting(Graphulo.DEFAULT_COMBINER_PRIORITY+1,
//        graphulo.basicRemoteOpts(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX, opts.oTsampleDegree, null, null));
    IteratorSetting isDistFinish = MathTwoScalar.applyOpDouble(Graphulo.DEFAULT_COMBINER_PRIORITY+1,
        false, MathTwoScalar.ScalarOp.MINUS, 1, true); // keep zero
    GraphuloUtil.addOnScopeOption(isDistFinish, EnumSet.of(IteratorUtil.IteratorScope.scan));
    GraphuloUtil.applyIteratorSoft(isDistFinish, graphulo.getConnector().tableOperations(), opts.oTsampleDist);
  }

}
