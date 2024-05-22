package edu.mit.ll.graphulo.skvi;

import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.ewise.EWiseOp;
import edu.mit.ll.graphulo.rowmult.CartesianRowMultiply;
import edu.mit.ll.graphulo.rowmult.RowMultiplyOp;
import edu.mit.ll.graphulo.simplemult.ConstantTwoScalar;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.PeekingIterator2;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Performs operations on two tables.
 * When <tt>dotmode == ROW</tt>, acts as multiply step of outer product, emitting partial products.
 * When <tt>dotmode == EWISE</tt>, acts as element-wise multiply.
 * When <tt>dotmode == NONE</tt>, no multiplication.
 * Set <tt>AT.emitNoMatch = B.emitNoMatch = true</tt> for sum of tables.
 * <p/>
 * Configure two remote sources for tables AT and B,
 * or configure one remote source and use the parent source as the other one.
 * <p/>
 * Table of behavior given options for AT, options for B and a parent source iterator.
 * <table>
 * <tr><th>AT</th><th>B</th><th>source</th><th>Behavior</th></tr>
 * <tr><td>y</td><td>y</td><td>y</td><td>Warn. AT * B</td></tr>
 * <tr><td>y</td><td>y</td><td>n</td><td>AT * B</td></tr>
 * <tr><td>y</td><td>n</td><td>n</td><td>AT * AT</td></tr>
 * <tr><td>y</td><td>n</td><td>y</td><td>AT * source</td></tr>
 * <tr><td>n</td><td>y</td><td>y</td><td>source * B</td></tr>
 * <tr><td>n</td><td>y</td><td>n</td><td>B * B</td></tr>
 * <tr><td>n</td><td>n</td><td>y</td><td>source * source</td></tr>
 * <tr><td>n</td><td>n</td><td>n</td><td>Error.</td></tr>
 * </table>
 *
 * If either the AT or B tableName is "{@value #CLONESOURCE_TABLENAME}",
 * then TwoTableIterator will initalize that table as a deepCopy of the source skvi,
 * setting up rowRanges and colFilters as in the options.
 *
 */
public class TwoTableIterator implements SaveStateIterator {
  private static final Logger log = LoggerFactory.getLogger(TwoTableIterator.class);

  private RowMultiplyOp rowMultiplyOp = null;
  private final Map<String, String> rowMultiplyOpOptions = new HashMap<>();
  private DOTMODE dotmode;
  private boolean emitNoMatchA = false, emitNoMatchB = false;
//  private MultiplyOp multiplyOp = null;
  private EWiseOp eWiseOp = null;
  private Map<String, String> multiplyOpOptions = new HashMap<>();

  private SortedKeyValueIterator<Key, Value> remoteAT, remoteB;
  private PeekingIterator2<? extends Map.Entry<Key, Value>> bottomIter;

  private Range seekRange;
  private Collection<ByteSequence> seekColumnFamilies;
  private boolean seekInclusive;
  /**
   * Track the row of AT and B emitted. For monitoring.
   */
  private Key emitted = new Key();

  public static final String CLONESOURCE_TABLENAME = "*CLONESOURCE*";
  public static final String PREFIX_AT = "AT";
  public static final String PREFIX_B = "B";

  public enum DOTMODE {
    /** Not yet implemented.  Plan is to emit nothing from collisions.
     * Only emit entries that do not match from the two tables. Set emitNoMatch to true for the two tables. */
    NONE,
    /** Perform a function on matching rows of A and B.  Provide rowMultiplyOp option. */
    ROW,
    /** Match on row and column. Nothing extra read into memory. */
    EWISE
  }

  public static final String
  EMITNOMATCH = "emitNoMatch",
  MULTIPLYOP = "multiplyOp",
  OPT_SUFFIX = ".opt.",
  ROWMULTIPLYOP = "rowMultiplyOp";

  /**
   *
   * @param map Map to reuse. Pass null to create a new HashMap.
   * @param emitNoMatch Whether to emit non-colliding entries. False for intersection/multiplication, True for union/sum.
   * @param tableName See {@link RemoteSourceIterator#optionMap}
   * @param zookeeperHost See {@link RemoteSourceIterator#optionMap}
   * @param timeout See {@link RemoteSourceIterator#optionMap}
   * @param instanceName See {@link RemoteSourceIterator#optionMap}
   * @param username See {@link RemoteSourceIterator#optionMap}
   * @param password See {@link RemoteSourceIterator#optionMap}
   * @param authorizations See {@link RemoteSourceIterator#optionMap}
   * @param rowRanges See {@link RemoteSourceIterator#optionMap}
   * @param colFilter See {@link RemoteSourceIterator#optionMap}
   * @param doClientSideIterators See {@link RemoteSourceIterator#optionMap}
   * @param remoteIterators See {@link RemoteSourceIterator#optionMap}
   * @return map with options filled in.
   */
  public static Map<String, String> tableREMOTEOptionMap(
      Map<String, String> map, boolean emitNoMatch, String tableName, String zookeeperHost, int timeout, String instanceName,
      String username, String password,
      Authorizations authorizations, String rowRanges, String colFilter, Boolean doClientSideIterators,
      DynamicIteratorSetting remoteIterators) {
    map = RemoteSourceIterator.optionMap(map, tableName, zookeeperHost, timeout, instanceName,
        username, password, authorizations, rowRanges, colFilter, doClientSideIterators, remoteIterators);
    map.put(EMITNOMATCH, Boolean.toString(emitNoMatch));
    return map;
  }

  /**
   * For either table A or table B. This option indicates that the table should
   * {@link SortedKeyValueIterator#deepCopy} the other table iterator.
   * @param map Map to reuse. Pass null to create a new HashMap.
   * @param emitNoMatch Whether to emit non-colliding entries. False for intersection/multiplication, True for union/sum.
   * @param rowRanges Row range filter, implemented with {@link SeekFilterIterator}.
   * @param colFilter Column filter, see {@link GraphuloUtil#applyGeneralColumnFilter(String, SortedKeyValueIterator, IteratorEnvironment)}.
   * @param remoteIterators Iterators to apply after the deepCopied iterator and before TwoTableIterator.
   * @return map with options for this table filled in.
   */
  public static Map<String, String> tableCLONESOURCEOptionMap(
      Map<String, String> map, boolean emitNoMatch, String rowRanges, String colFilter,
      DynamicIteratorSetting remoteIterators) {
    map = RemoteSourceIterator.optionMap(map, CLONESOURCE_TABLENAME, null, -1, null,
        null, (String)null, null, rowRanges, colFilter, false, remoteIterators);
    map.put(EMITNOMATCH, Boolean.toString(emitNoMatch));
    return map;
  }

  /**
   * Create a map of options that properly configures TwoTableIterator
   * for ROW mode.
   * <p>
   * One of either AtableMap or BtableMap must be non-null.
   * Use either {@link #tableREMOTEOptionMap} or {@link #tableCLONESOURCEOptionMap}
   * for the AtableMap and BtableMap arguments.
   *
   * @param map Map to reuse. Pass null to create a new HashMap.
   * @param AtableMap Options for table A. Pass null to use the source iterator for table A.
   * @param BtableMap Options for table B. Pass null to use the source iterator for table B.
   * @param rowMultiplyOp Null uses default {@link CartesianRowMultiply}.
   * @param rowMultiplyOpOptions Null means no options passed to rowMultiplyOp.
   * @return map with TwoTableIterator's options filled in.
   */
  public static Map<String, String> optionMapROW(
      Map<String, String> map, Map<String, String> AtableMap, Map<String, String> BtableMap,
      Class<? extends RowMultiplyOp> rowMultiplyOp, Map<String, String> rowMultiplyOpOptions) {
    map = optionMapCommon(map, AtableMap, BtableMap, DOTMODE.ROW);
    if (rowMultiplyOp != null)
      map.put(ROWMULTIPLYOP, rowMultiplyOp.getName());
    if (rowMultiplyOpOptions != null)
      for (Map.Entry<String, String> entry : rowMultiplyOpOptions.entrySet())
        map.put(ROWMULTIPLYOP + OPT_SUFFIX + entry.getKey(), entry.getValue());
    return map;
  }

  /**
   * Create a map of options that properly configures TwoTableIterator
   * for EWISE mode.
   * <p>
   * One of either AtableMap or BtableMap must be non-null.
   * Use either {@link #tableREMOTEOptionMap} or {@link #tableCLONESOURCEOptionMap}
   * for the AtableMap and BtableMap arguments.
   *
   * @param map Map to reuse. Pass null to create a new HashMap.
   * @param AtableMap Options for table A. Pass null to use the source iterator for table A.
   * @param BtableMap Options for table B. Pass null to use the source iterator for table B.
   * @param multiplyOp Null uses default {@link CartesianRowMultiply}.
   * @param multiplyOpOptions Null means no options passed to rowMultiplyOp.
   * @return map with TwoTableIterator's options filled in.
   */
  public static Map<String, String> optionMapEWISE(
      Map<String, String> map, Map<String, String> AtableMap, Map<String, String> BtableMap,
      Class<? extends EWiseOp> multiplyOp, Map<String, String> multiplyOpOptions) {
    map = optionMapCommon(map, AtableMap, BtableMap, DOTMODE.EWISE);
    if (multiplyOp != null)
      map.put(MULTIPLYOP, multiplyOp.getName());
    if (multiplyOpOptions != null)
      for (Map.Entry<String, String> entry : multiplyOpOptions.entrySet())
        map.put(MULTIPLYOP + OPT_SUFFIX + entry.getKey(), entry.getValue());
    return map;
  }

  /**
   * Create a map of options that properly configures TwoTableIterator
   * for NONE mode.
   * <p>
   * One of either AtableMap or BtableMap must be non-null.
   * Use either {@link #tableREMOTEOptionMap} or {@link #tableCLONESOURCEOptionMap}
   * for the AtableMap and BtableMap arguments.
   *
   * @param map Map to reuse. Pass null to create a new HashMap.
   * @param AtableMap Options for table A. Pass null to use the source iterator for table A.
   * @param BtableMap Options for table B. Pass null to use the source iterator for table B.
   * @return map with TwoTableIterator's options filled in.
   */
  public static Map<String, String> optionMapNONE(
      Map<String, String> map, Map<String, String> AtableMap, Map<String, String> BtableMap) {
    return optionMapCommon(map, AtableMap, BtableMap, DOTMODE.NONE);
  }

  private static Map<String,String> optionMapCommon(
      Map<String, String> map, Map<String, String> AtableMap, Map<String, String> BtableMap,
      DOTMODE dotmode) {
    if (map == null)
      map = new HashMap<>();
    if (AtableMap != null)
      for (Map.Entry<String, String> entry : AtableMap.entrySet())
        map.put(PREFIX_AT + '.' + entry.getKey(), entry.getValue());
    if (BtableMap != null)
      for (Map.Entry<String, String> entry : BtableMap.entrySet())
        map.put(PREFIX_B + '.' + entry.getKey(), entry.getValue());
    map.put("dotmode", dotmode.name());
    return map;
  }

  private void parseOptions(Map<String, String> options, final Map<String, String> optAT, final Map<String, String> optB) {
    log.debug("options: "+options);
    String dm = options.get("dotmode");
    if (dm == null)
      throw new IllegalArgumentException("Must specify dotmode. Given: " + options);
    dotmode = DOTMODE.valueOf(dm);

    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
      String optionKey = optionEntry.getKey(), optionValue = optionEntry.getValue();
      if (optionKey.startsWith(PREFIX_AT + '.')) {
        String keyAfterPrefix = optionKey.substring(PREFIX_AT.length() + 1);
        switch (keyAfterPrefix) {
          case EMITNOMATCH:
            emitNoMatchA = Boolean.parseBoolean(optionValue);
            break;
          case "doWholeRow":
            if (Boolean.parseBoolean(optionValue))
              log.warn("Forcing doWholeRow option on table A to FALSE. Given: " + optionValue);
            continue;
          default:
            optAT.put(keyAfterPrefix, optionValue);
            break;
        }
      } else if (optionKey.startsWith(PREFIX_B + '.')) {
        String keyAfterPrefix = optionKey.substring(PREFIX_B.length() + 1);
        switch (keyAfterPrefix) {
          case EMITNOMATCH:
            emitNoMatchB = Boolean.parseBoolean(optionValue);
            break;
          case "doWholeRow":
            if (Boolean.parseBoolean(optionValue))
              log.warn("Forcing doWholeRow option on table A to FALSE. Given: " + optionValue);
            continue;
          default:
            optB.put(keyAfterPrefix, optionValue);
            break;
        }
      } else if (optionKey.startsWith("rowMultiplyOp.opt.")) {
        String keyAfterPrefix = optionKey.substring("rowMultiplyOp.opt.".length());
        rowMultiplyOpOptions.put(keyAfterPrefix, optionValue);
      } else if (optionKey.startsWith("multiplyOp.opt.")) {
        switch (dotmode) {
          case ROW:
            rowMultiplyOpOptions.put(optionKey, optionValue);
            break;
          case EWISE:
            String keyAfterPrefix = optionKey.substring("multiplyOp.opt.".length());
            multiplyOpOptions.put(keyAfterPrefix, optionValue);
            break;
          case NONE:
            break;
        }
      } else {
        switch (optionKey) {
          case ROWMULTIPLYOP:
            if (dotmode == DOTMODE.ROW)
              rowMultiplyOp = GraphuloUtil.subclassNewInstance(optionValue, RowMultiplyOp.class);
            else
              log.warn(dotmode+" mode: Ignoring rowMultiplyOp " + optionValue);
            break;
          case MULTIPLYOP:
            switch (dotmode) {
              case ROW:
                rowMultiplyOpOptions.put(MULTIPLYOP, optionValue);
                break;
              case EWISE:
                eWiseOp = GraphuloUtil.subclassNewInstance(optionValue, EWiseOp.class);
                break;
              case NONE:
                log.warn("NONE mode: Ignoring multiplyOp " + optionValue);
                break;
            }
            break;
          case "dotmode":
            break;
//          case "trace":
//            Watch.enableTrace = Boolean.parseBoolean(optionValue);
//            break;
          default:
            log.warn("Unrecognized option: " + optionEntry);
            break;
        }
      }
    }
    switch (dotmode) {
      case ROW:
        if (rowMultiplyOp == null)
//          throw new IllegalArgumentException("ROW mode: Must specify rowMultiplyOp");
          rowMultiplyOp = new CartesianRowMultiply(); // default
        break;
      case EWISE:
        if (eWiseOp == null)
//          throw new IllegalArgumentException("EWISE mode: Must specify rowMultiplyOp");
          eWiseOp = new ConstantTwoScalar(); // default: 1 on collision
        break;
      case NONE:
        break;
    }
  }

  private Map<String,String> origOptions;
  private SortedKeyValueIterator<Key,Value> origSource;

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    origOptions = new HashMap<>(options);
    origSource = source;
    // parse options, pass correct options to RemoteSourceIterator init()
    Map<String, String> optAT = new HashMap<>(), optB = new HashMap<>();
    parseOptions(options, optAT, optB);
    // The tableName option is the key signal for use of RemoteSourceIterator.
    boolean dorAT = optAT.containsKey(RemoteSourceIterator.TABLENAME) && !optAT.get(RemoteSourceIterator.TABLENAME).isEmpty(),
        dorB = optB.containsKey(RemoteSourceIterator.TABLENAME) && !optB.get(RemoteSourceIterator.TABLENAME).isEmpty();


    if (!dorAT && !dorB && source == null)      // ~A ~B ~S
      throw new IllegalArgumentException("optAT, optB, and source cannot all be missing");
    if (source == null) {
      if (dorAT) {
        remoteAT = new RemoteSourceIterator();
        remoteAT.init(null, optAT, env);
        if (dorB) {
          remoteB = new RemoteSourceIterator(); // A B ~S
          remoteB.init(null, optB, env);
        } else                                  // A ~B ~S
          remoteB = remoteAT.deepCopy(env);
      } else {
        remoteB = new RemoteSourceIterator();   // ~A B ~S
        remoteB.init(null, optB, env);
        remoteAT = remoteB.deepCopy(env);
      }
    } else { // source != null
      if (!dorAT && !dorB) {                    // ~A ~B S
        remoteAT = source;
        remoteB = source.deepCopy(env);
        remoteAT = setupRemoteSourceOptionsSKVI(remoteAT, optAT, env);
        remoteB = setupRemoteSourceOptionsSKVI(remoteB, optB, env);
      } else if (dorAT) {
        remoteAT = setup(optAT, source, env);
        if (!dorB) {
          remoteB = source;                     // A ~B S
          remoteB = setupRemoteSourceOptionsSKVI(remoteB, optB, env);
        } else {
          remoteB = setup(optB, source, env);   // A B S
        }
      } else {
        remoteB = setup(optB, source, env);
        remoteAT = source;                      // ~A B S
        remoteAT = setupRemoteSourceOptionsSKVI(remoteAT, optAT, env);
      }
    }

    assert !(rowMultiplyOp != null && eWiseOp != null);
    log.debug("rowMultiplyOp=" + rowMultiplyOp + "  rowMultiplyOpOptions: " + rowMultiplyOpOptions);
    if (rowMultiplyOp != null)
      rowMultiplyOp.init(rowMultiplyOpOptions, env);
    if (eWiseOp != null)
      eWiseOp.init(multiplyOpOptions, env);
  }

  private SortedKeyValueIterator<Key, Value> setup(
      Map<String,String> opts, SortedKeyValueIterator<Key, Value> source, IteratorEnvironment env) throws IOException {
    assert opts != null && source != null;
    String tableName = opts.get(RemoteSourceIterator.TABLENAME);
    SortedKeyValueIterator<Key, Value> ret;
    if (tableName != null && tableName.equals(CLONESOURCE_TABLENAME)) {
      if (source.getClass().equals(DynamicIterator.class)) {
        log.warn("BAD OPTS: " + opts);
      }
      ret = source.deepCopy(env);
      ret = setupRemoteSourceOptionsSKVI(ret, opts, env);
      log.debug("Setting up "+CLONESOURCE_TABLENAME+": "+ret);
    } else {
      ret = new RemoteSourceIterator();
      ret.init(null, opts, env);
    }
    return ret;
  }

  /** This corresponds with the setup for RemoteSourceIterator, except this is applied to a local SKVI instead
   * of a Scanner. */
  private SortedKeyValueIterator<Key, Value> setupRemoteSourceOptionsSKVI(
      SortedKeyValueIterator<Key, Value> ret, Map<String, String> opts, IteratorEnvironment env) throws IOException {
    boolean doWholeRow = false;
    Map<String,String> diterMap = new HashMap<>();
    for (Map.Entry<String, String> optionEntry : opts.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
      if (optionEntry.getValue().isEmpty())
        continue;
      if (optionKey.startsWith(RemoteSourceIterator.ITER_PREFIX)) {
        diterMap.put(optionKey.substring(RemoteSourceIterator.ITER_PREFIX.length()), optionValue);
      } else {
        switch (optionKey) {
          case RemoteSourceIterator.ZOOKEEPERHOST:
          case RemoteSourceIterator.TIMEOUT:
          case RemoteSourceIterator.INSTANCENAME:
          case RemoteSourceIterator.USERNAME:
          case RemoteSourceIterator.PASSWORD:
          case RemoteSourceIterator.AUTHENTICATION_TOKEN:
          case RemoteSourceIterator.AUTHENTICATION_TOKEN_CLASS:
          case RemoteSourceIterator.DOCLIENTSIDEITERATORS:
          case RemoteSourceIterator.AUTHORIZATIONS: // <-- not sure about this one
            // these are ok to ignore
            break;
          case RemoteSourceIterator.TABLENAME:
            assert optionEntry.getValue().equals(CLONESOURCE_TABLENAME);
            break;
          case "doWholeRow":
            doWholeRow = Boolean.parseBoolean(optionEntry.getValue());
            break;
          case RemoteSourceIterator.ROWRANGES:
            SortedKeyValueIterator<Key, Value> filter = new SeekFilterIterator();
            filter.init(ret, Collections.singletonMap(RemoteSourceIterator.ROWRANGES, optionEntry.getValue()), env);
            ret = filter;
            break;
          case RemoteSourceIterator.COLFILTER:
//            byte[] by = optionValue.getBytes(StandardCharsets.UTF_8);
//            log.debug("Printing characters of string: "+ Key.toPrintableString(by, 0, by.length, 100));
            ret = GraphuloUtil.applyGeneralColumnFilter(optionValue, ret, env);
            break;
          default:
            log.warn("Unrecognized option: " + optionEntry);
            break;
        }
      }
    }

    if (!diterMap.isEmpty()) {
      DynamicIteratorSetting dynamicIteratorSetting = DynamicIteratorSetting.fromMap(diterMap);
//      System.out.println("about to load:\n"+dynamicIteratorSetting);
      ret = dynamicIteratorSetting.loadIteratorStack(ret, env);
    }

    if (doWholeRow) {
      SortedKeyValueIterator<Key, Value> filter = new WholeRowIterator();
      filter.init(ret, Collections.<String, String>emptyMap(), env);
      ret = filter;
    }
    return ret;
  }


  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
//    System.out.println("DM ori range: "+range);
//    System.out.println("DM colFamili: "+columnFamilies);
//    System.out.println("DM inclusive: " + inclusive);

    Key sk = range.getStartKey();
    // BAD: put range at beginning of row, no matter what
//    if (sk != null)
//      range = new Range(new Key(sk.getRow()), true, range.getEndKey(), range.isEndKeyInclusive());

    // if range is not infinite, see if there is a clear sign we want to restore state:
    if (sk != null && sk.getColumnFamilyData().length() == 0 && sk.getColumnQualifierData().length() == 0 && sk.getColumnVisibilityData().length() == 0
        && sk.getTimestamp() == Long.MAX_VALUE && !range.isStartKeyInclusive()) {
      // assuming that we are seeking using a key previously returned by this iterator
      // therefore go to the next row
      Key followingRowKey = sk.followingKey(PartialKey.ROW);
      if (range.getEndKey() != null && followingRowKey.compareTo(range.getEndKey()) > 0) {
        seekRange = range;
        seekColumnFamilies = columnFamilies;
        seekInclusive = inclusive;
        bottomIter = null;
        log.debug("Weird range; aborting seek. Range is "+range);
        return;
      }

      range = new Range(followingRowKey, true, range.getEndKey(), range.isEndKeyInclusive());
    }

    seekRange = range;
    seekColumnFamilies = columnFamilies;
    seekInclusive = inclusive;
//    System.out.println("DM adj range: " + range);

    // Weird results if we start in the middle of a row. Not handling.
//    Watch<Watch.PerfSpan> watch = Watch.getInstance();
//    watch.start(Watch.PerfSpan.ATnext);
//    try {
      remoteAT.seek(seekRange, columnFamilies, inclusive);
//    } finally {
//      watch.stop(Watch.PerfSpan.ATnext);
//    }

//    watch.start(Watch.PerfSpan.Bnext);
//    try {
      remoteB.seek(seekRange, columnFamilies, inclusive);
//    } finally {
//      watch.stop(Watch.PerfSpan.Bnext);
//    }

//    log.debug("remoteAT.hasTop()="+remoteAT.hasTop()+" remoteB.hasTop()="+remoteB.hasTop());
    prepNextRowMatch(/*false*/);
//    log.debug("remoteAT.hasTop()="+remoteAT.hasTop()+" remoteB.hasTop()="+remoteB.hasTop());
//    if (remoteAT.hasTop())
//      log.debug("remoteAT.getTopKey()="+remoteAT.getTopKey());
//    if (remoteB.hasTop())
//      log.debug("remoteB.getTopKey()="+remoteB.getTopKey());
  }

  private void prepNextRowMatch(/*boolean doNext*/) throws IOException {

    /*if (doNext) {
      watch.start(Watch.PerfSpan.ATnext);
      try {
        remoteAT.next();
      } finally {
        watch.stop(Watch.PerfSpan.ATnext);
      }
      if (!remoteAT.hasTop()) {
        bottomIter = null;
        return;
      }
      watch.start(Watch.PerfSpan.Bnext);
      try {
        remoteB.next();
      } finally {
        watch.stop(Watch.PerfSpan.Bnext);
      }
      if (!remoteB.hasTop()) {
        bottomIter = null;
        return;
      }
    }*/


    PartialKey pk = null;
    switch (dotmode) {
      case ROW:
        pk = PartialKey.ROW;
        break;
      case EWISE:
      case NONE:
        pk = PartialKey.ROW_COLFAM_COLQUAL_COLVIS;
        break;
    }


    Watch<Watch.PerfSpan> watch = null; //Watch.getInstance();
    TOPLOOP:
    do {
      if ((!remoteAT.hasTop() && !remoteB.hasTop())
          || (!emitNoMatchA && remoteAT.hasTop() && !remoteB.hasTop())
          || (!emitNoMatchB && !remoteAT.hasTop() && remoteB.hasTop())) {
        bottomIter = null;
        return;
      }

      while (true) {
        int cmp;
        if (emitNoMatchA && remoteAT.hasTop() && !remoteB.hasTop())
          cmp = -1;
        else if (emitNoMatchB && !remoteAT.hasTop() && remoteB.hasTop())
          cmp = 1;
        else
          cmp = remoteAT.getTopKey().compareTo(remoteB.getTopKey(), pk);

//        if (remoteAT.hasTop() && remoteB.hasTop())
//          log.debug("remoteAT "+remoteAT.getTopKey().toStringNoTime()+" "+remoteAT.getTopValue()
//            +" remoteB "+remoteB.getTopKey().toStringNoTime()+" "+remoteB.getTopValue());
//        else if (remoteAT.hasTop())
//          log.debug("remoteAT "+remoteAT.getTopKey().toStringNoTime()+" "+remoteAT.getTopValue());
//        else if (remoteB.hasTop())
//          log.debug(" remoteB "+remoteB.getTopKey().toStringNoTime()+" "+remoteB.getTopValue());
//        else
//          log.debug("no hasTop() for remoteAT or remoteB");

        if (cmp < 0) {
          if (emitNoMatchA) {
            switch (dotmode) {
              case ROW:
                emitted = GraphuloUtil.keyCopy(remoteAT.getTopKey(), PartialKey.ROW);
                bottomIter = new PeekingIterator2<>(rowMultiplyOp.multiplyRow(remoteAT, null));
                continue TOPLOOP;
              case EWISE:
                emitted = remoteAT.getTopKey();
                bottomIter = new PeekingIterator2<>(
                    eWiseOp.multiply(emitted.getRowData(), emitted.getColumnFamilyData(),
                        emitted.getColumnQualifierData(), emitted.getColumnVisibilityData(),
                        emitted.getTimestamp(), Long.MAX_VALUE,
                        remoteAT.getTopValue(), null));
                remoteAT.next();
                continue TOPLOOP;
              case NONE:
                bottomIter = new PeekingIterator2<>(GraphuloUtil.copyTopEntry(remoteAT));
                remoteAT.next();
                return;
            }
          }
          boolean success = skipUntil(remoteAT, remoteB.getTopKey(), pk, seekRange, seekColumnFamilies, seekInclusive, watch, Watch.PerfSpan.ATnext);
          if (!success) {
            bottomIter = null;
            return;
          }
//          continue;
        } else if (cmp > 0) {
          if (emitNoMatchB) {
            switch (dotmode) {
              case ROW:
                emitted = GraphuloUtil.keyCopy(remoteB.getTopKey(), PartialKey.ROW);
                bottomIter = new PeekingIterator2<>(rowMultiplyOp.multiplyRow(null, remoteB));
                continue TOPLOOP;
              case EWISE:
                emitted = remoteB.getTopKey();
                bottomIter = new PeekingIterator2<>(
                    eWiseOp.multiply(emitted.getRowData(), emitted.getColumnFamilyData(),
                        emitted.getColumnQualifierData(), emitted.getColumnVisibilityData(),
                        Long.MAX_VALUE, emitted.getTimestamp(),
                        null, remoteB.getTopValue()));
                remoteB.next();
                continue TOPLOOP;
              case NONE:
                bottomIter = new PeekingIterator2<>(GraphuloUtil.copyTopEntry(remoteB));
                remoteB.next();
                return;
            }
          }
          boolean success = skipUntil(remoteB, remoteAT.getTopKey(), pk, seekRange, seekColumnFamilies, seekInclusive, watch, Watch.PerfSpan.Bnext);
          if (!success) {
            bottomIter = null;
            return;
          }
//          continue;
        } else { // cmp == 0
          // collision = matching row
          switch (dotmode) {
            case ROW: {
              emitted = GraphuloUtil.keyCopy(remoteAT.getTopKey(), PartialKey.ROW);
              bottomIter = new PeekingIterator2<>(rowMultiplyOp.multiplyRow(remoteAT, remoteB));
              continue TOPLOOP;
            }
            case EWISE: {
              emitted = remoteAT.getTopKey();
              bottomIter = new PeekingIterator2<>(
                  eWiseOp.multiply(emitted.getRowData(), emitted.getColumnFamilyData(),
                      emitted.getColumnQualifierData(), emitted.getColumnVisibilityData(),
                      emitted.getTimestamp(), remoteB.getTopKey().getTimestamp(),
                      remoteAT.getTopValue(), remoteB.getTopValue()));
              remoteAT.next();
              remoteB.next();
              continue TOPLOOP;
            }
            case NONE: // emit nothing on collision
              bottomIter = PeekingIterator2.emptyIterator();
              remoteAT.next();
              remoteB.next();
              continue TOPLOOP;
          }
        }
      }
    } while (bottomIter != null && !bottomIter.hasNext());
  }

  /**
   * Call next() on skvi until getTopKey() advances >= keyToSkipTo (in terms of pk), or until !hasTop().
   * Calls seek() if this takes a while, say greater than 10 next() calls.
   *
   * @return True if advanced to a new key; false if !hasTop().
   */
  static boolean skipUntil(SortedKeyValueIterator<Key, Value> skvi, Key keyToSkipTo, PartialKey pk,
                           Range seekRange, Collection<ByteSequence> columnFamilies, boolean inclusive,
                           Watch<Watch.PerfSpan> watch, Watch.PerfSpan watchtype) throws IOException {
    assert keyToSkipTo != null;
    /** Call seek() if using this many next() calls does not get us to rowToSkipTo */
    final int MAX_NEXT_ATTEMPT = 10;
    int cnt;
    for (cnt = 0;
         cnt < MAX_NEXT_ATTEMPT && skvi.hasTop() && keyToSkipTo.compareTo(skvi.getTopKey(), pk) > 0;
         cnt++) {
//      watch.start(watchtype);
//      try {
        skvi.next();
//      } finally {
//        watch.stop(watchtype);
//      }
    }
    if (skvi.hasTop() && keyToSkipTo.compareTo(skvi.getTopKey(), pk) > 0) {
      // set target range to beginning of pk
      Key seekKey = GraphuloUtil.keyCopy(keyToSkipTo, pk);
      Range skipToRange = new Range(seekKey, true, null, false)
          .clip(seekRange, true);
      if (skipToRange == null) // row we want to get to does not exist, and it is out of our range
        return false;
      skvi.seek(skipToRange, columnFamilies, inclusive);
    }

//    watch.increment(Watch.PerfSpan.RowSkipNum, cnt);
    return skvi.hasTop();
  }


  @Override
  public void next() throws IOException {
    bottomIter.next();
    if (!bottomIter.hasNext())
      prepNextRowMatch(/*false*/);
  }

  @Override
  public Key safeState() {
    if (bottomIter == null || bottomIter.peekSecond() != null) {
      // either we have no entries left to emit, or we need to
      // finish the row's cartesian product first (until bottomIter has one left)
      return null;
    } else {
      // the current top entry of bottomIter is the last in this cartesian product (bottomIter)
      // TWOROW/ONEROWA/ONEROWB: Save state at this row.  If reseek'd to this row, go to the next row (assume exclusive).
      // EWISE/NONE: emit the exact Key accessed.
      assert bottomIter.peekFirst() != null;
      return emitted;
    }
  }


  @Override
  public boolean hasTop() {
    assert bottomIter == null || bottomIter.hasNext();
    return bottomIter != null;
  }

  @Override
  public Key getTopKey() {
    return bottomIter.peekFirst().getKey();
  }

  @Override
  public Value getTopValue() {
    return bottomIter.peekFirst().getValue();
  }

  @Override
  public TwoTableIterator deepCopy(IteratorEnvironment env) {
    TwoTableIterator copy = new TwoTableIterator();
    try {
      copy.init(origSource == null ? null : origSource.deepCopy(env), origOptions, env);
    } catch (IOException e) {
      log.error("Problem creating deepCopy", e);
      throw new RuntimeException(e);
    }
    return copy;
  }
}
