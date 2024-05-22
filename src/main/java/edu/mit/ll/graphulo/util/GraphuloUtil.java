package edu.mit.ll.graphulo.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.RangeSet;
import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.skvi.D4mRangeFilter;
import edu.mit.ll.graphulo.skvi.RemoteWriteIterator;
import edu.mit.ll.graphulo.skvi.TwoTableIterator;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.ColumnQualifierFilter;
import org.apache.accumulo.core.iterators.user.ColumnSliceFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utility functions
 */
public class GraphuloUtil {
  private static final Logger log = LoggerFactory.getLogger(GraphuloUtil.class);

  public static final char DEFAULT_SEP_D4M_STRING = '\t';
  public static final String OPT_SUFFIX = ".opt.";

  public static final Text EMPTY_TEXT = new Text();
  public static final Value EMPTY_VALUE = new Value();
  public static final Key EMPTY_KEY = new Key();
  public static final byte[] EMPTY_BYTES = new byte[0];

  public static final Value VALUE_ONE_STRING = new Value("1".getBytes(StandardCharsets.UTF_8));
  public static final byte[] VALUE_ONE_STRING_BYTES = "1".getBytes(StandardCharsets.UTF_8);
  public static final Value VALUE_ONE_VLONG = new Value(GraphuloUtil.writeVUnsignedLong(1));

  private GraphuloUtil() {
  }


  /* Motivation for using -1 argument in String.split() call:
System.out.println(",".split(",",-1 ).length + Arrays.toString(",".split(",",-1 )));
System.out.println(",".split(",",0  ).length + Arrays.toString(",".split(",",0  )));
System.out.println("a,".split(",",-1).length + Arrays.toString("a,".split(",",-1)));
System.out.println("a,".split(",",0 ).length + Arrays.toString("a,".split(",",0 )));
System.out.println();
System.out.println(",".split(",",-1 )[1].length);
System.out.println("a,,".split(",",0 ).length + Arrays.toString("a,,".split(",",0 )));
System.out.println("a,,".split(",",-1).length + Arrays.toString("a,,".split(",",-1)));
System.out.println(",a,,".split(",",0 ).length + Arrays.toString(",a,,".split(",",0 )));
System.out.println(",a,,".split(",",-1).length + Arrays.toString(",a,,".split(",",-1)));
   */

  /**
   * Split a D4M String into each component. Does nothing special with ranges, i.e. the ':' character.
   */
  public static String[] splitD4mString(String str) {
    // maybe optimize away since this is a lower-level function
    Preconditions.checkArgument(str != null && !str.isEmpty(), "%s must be length at least 1", str);
    return str.substring(0,str.length()-1).split(
        Character.toString(str.charAt(str.length() - 1)), -1
    );
  }

  /**
   * Split options on period characters.
   * "" holds entries without a period.
   *
   * @return Map Prefix -> (Map entryWithoutPrefix -> value)
   */
  public static Map<String, Map<String, String>> splitMapPrefix(Map<String, String> options) {
    Map<String, Map<String, String>> prefixMap = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      int periodIdx = key.indexOf(".");
      if (periodIdx == 0)
        throw new IllegalArgumentException("not allowed to have option that starts with period: " + entry);
      String prefix = periodIdx == -1
          ? "" : key.substring(0, periodIdx);
      String afterPrefix = periodIdx == -1
          ? key : key.substring(periodIdx + 1); // ok if empty

      Map<String, String> mapInside = prefixMap.get(prefix);
      if (mapInside == null) {
        mapInside = new HashMap<>();
      }
      mapInside.put(afterPrefix, entry.getValue());
      prefixMap.put(prefix, mapInside);
    }
    return prefixMap;
  }

  public static Map<String, String> preprendPrefixToKey(String prefix, Map<String, String> options) {
    Map<String, String> res = new HashMap<>(options.size());
    for (Map.Entry<String, String> entry : options.entrySet()) {
      res.put(prefix + entry.getKey(), entry.getValue());
    }
    return res;
  }

  private static final SortedSet<Range> SETINFRNG;
  static {
    SortedSet<Range> tmp = new TreeSet<>();
    tmp.add(new Range());
    SETINFRNG = Collections.unmodifiableSortedSet(tmp);
  }

  /**
   * Convert D4M string representation of rows to Ranges.
   * Last character in the string is an arbitrary separator char
   * that must not appear in the rows. The ':' cannot appear in rows either.
   * See UtilTest for more test cases.
   * Does not merge overlapping ranges.
   *
   * @param rowStr Ex: ':,r1,r3,r5,:,r7,r9,:,'
   * @return Ex: (-Inf,r1] [r3,r3) [r5,r7] [r9,+Inf)
   */
  public static SortedSet<Range> d4mRowToRanges(String rowStr) {
    return d4mRowToRanges(rowStr, false);
  }

  /**
   * @see #d4mRowToRanges(String)
   * @param singletonsArePrefix If true, then singleton entries in the D4M string are
   *                            made into prefix ranges instead of single row ranges.
   */
  public static SortedSet<Range> d4mRowToRanges(String rowStr, boolean singletonsArePrefix) {
    if (rowStr == null || rowStr.isEmpty())
      return new TreeSet<>();
    // could write my own version that does not do regex, but probably not worth optimizing
    String[] rowStrSplit = splitD4mString(rowStr);
    //if (rowStrSplit.length == 1)
    List<String> rowStrList = Arrays.asList(rowStrSplit);
    PeekingIterator3<String> pi = new PeekingIterator3<>(rowStrList.iterator());
    SortedSet<Range> rngset = new TreeSet<>();

    if (pi.peekFirst().equals(":")) { // (-Inf,
      if (pi.peekSecond() == null) {
        return SETINFRNG; // (-Inf,+Inf)
      } else {
        if (pi.peekSecond().equals(":") || (pi.peekThird() != null && pi.peekThird().equals(":")))
          throw new IllegalArgumentException("Bad D4M rowStr: " + rowStr);
        rngset.add(new Range(null, false, pi.peekSecond(), true)); // (-Inf,2]
        pi.next();
        pi.next();
      }
    }

    while (pi.hasNext()) {
      if (pi.peekSecond() == null) { // last singleton row [1,1~)
        if (singletonsArePrefix)
          rngset.add(Range.prefix(pi.peekFirst()));
        else
          rngset.add(Range.exact(pi.peekFirst()));
        return rngset;
      } else if (pi.peekSecond().equals(":")) {
        if (pi.peekThird() == null) { // [1,+Inf)
          rngset.add(new Range(pi.peekFirst(), true, null, false));
          return rngset;
        } else { // [1,3]
          if (pi.peekThird().equals(":"))
            throw new IllegalArgumentException("Bad D4M rowStr: " + rowStr);
          rngset.add(new Range(pi.peekFirst(), true, pi.peekThird(), true));
          pi.next();
          pi.next();
          pi.next();
        }
      } else { // [1,1~)
        if (singletonsArePrefix)
          rngset.add(Range.prefix(pi.peekFirst()));
        else
          rngset.add(Range.exact(pi.peekFirst()));
        pi.next();
      }
    }
    return rngset;
  }

  public static ByteSequence stringToByteSequence(String s) {
//    try {
//      ByteBuffer bb = Text.encode(s);
//      return new ArrayByteSequence(bb.array(), bb.arrayOffset(), bb.limit());
//    } catch (CharacterCodingException e) {
//      log.error("problem encoding string "+s, e);
//      throw new RuntimeException(e);
//    }
    return new ArrayByteSequence(s);
  }

  /**
   * @see #d4mRowToRanges(String)
   * @param singletonsArePrefix If true, then singleton entries in the D4M string are
   *                            made into prefix ranges instead of single row ranges.
   * @return {@link ImmutableRangeSet}
   */
  public static RangeSet<ByteSequence> d4mRowToGuavaRangeSet(String rowStr, boolean singletonsArePrefix) {
    if (rowStr == null || rowStr.isEmpty())
      return ImmutableRangeSet.of();
    // could write my own version that does not do regex, but probably not worth optimizing
    String[] rowStrSplit = splitD4mString(rowStr);
    //if (rowStrSplit.length == 1)
    List<String> rowStrList = Arrays.asList(rowStrSplit);
    PeekingIterator3<String> pi = new PeekingIterator3<>(rowStrList.iterator());
    ImmutableRangeSet.Builder<ByteSequence> rngset = ImmutableRangeSet.builder();

    if (pi.peekFirst().equals(":")) { // (-Inf,
      if (pi.peekSecond() == null) {
        return ImmutableRangeSet.of(com.google.common.collect.Range.<ByteSequence>all()); // (-Inf,+Inf)
      } else {
        if (pi.peekSecond().equals(":") || (pi.peekThird() != null && pi.peekThird().equals(":")))
          throw new IllegalArgumentException("Bad D4M rowStr: " + rowStr);
        rngset.add(com.google.common.collect.Range.atMost(stringToByteSequence(pi.peekSecond()))); // (-Inf,2]
        pi.next();
        pi.next();
      }
    }

    while (pi.hasNext()) {
      if (pi.peekSecond() == null) { // last singleton row [1,1~)
        if (singletonsArePrefix)
          rngset.add(com.google.common.collect.Range.closedOpen(
            stringToByteSequence(pi.peekFirst()), stringToByteSequence(Range.followingPrefix(new Text(pi.peekFirst())).toString())
          ));
        else
          rngset.add(com.google.common.collect.Range.singleton(stringToByteSequence(pi.peekFirst())));
        return rngset.build();
      } else if (pi.peekSecond().equals(":")) {
        if (pi.peekThird() == null) { // [1,+Inf)
          rngset.add(com.google.common.collect.Range.atLeast(stringToByteSequence(pi.peekFirst())));
          return rngset.build();
        } else { // [1,3]
          if (pi.peekThird().equals(":"))
            throw new IllegalArgumentException("Bad D4M rowStr: " + rowStr);
          rngset.add(com.google.common.collect.Range.closed(stringToByteSequence(pi.peekFirst()), stringToByteSequence(pi.peekThird())));
          pi.next();
          pi.next();
          pi.next();
        }
      } else { // [1,1~)
        if (singletonsArePrefix)
          rngset.add(com.google.common.collect.Range.closedOpen(
              stringToByteSequence(pi.peekFirst()), stringToByteSequence(Range.followingPrefix(new Text(pi.peekFirst())).toString())
          ));
        else
          rngset.add(com.google.common.collect.Range.singleton(stringToByteSequence(pi.peekFirst())));
        pi.next();
      }
    }
    return rngset.build();
  }

  public static String rangesToD4MString(Collection<Range> ranges) {
    return rangesToD4MString(ranges, DEFAULT_SEP_D4M_STRING);
  }

  public static String rangesToD4MString(Collection<Range> ranges, char sep) {
    if (ranges == null || ranges.isEmpty())
      return "";
    ranges = Range.mergeOverlapping(ranges);
    StringBuilder sb = new StringBuilder();
    String infEnd = null;
    for (Range range : ranges) {
      if (range.isInfiniteStartKey() && range.isInfiniteStopKey())
        return sb.append(':').append(sep).toString();
      else if (range.isInfiniteStartKey()) {
        String endRow = normalizeEndRow(range); assert endRow != null;
        sb.insert(0, ":" + sep+endRow+sep);
      } else if (range.isInfiniteStopKey()) {
        infEnd = normalizeStartRow(range); assert infEnd != null;
      } else {
        String startRow = normalizeStartRow(range),
            endRow = normalizeEndRow(range);
        assert startRow != null && endRow != null;
        if (startRow.equals(endRow))
          sb.append(startRow).append(sep);
        else
          sb.append(startRow).append(sep).append(':').append(sep).append(endRow).append(sep);
      }
    }
    if (infEnd != null)
      sb.append(infEnd).append(sep).append(':').append(sep);
    return sb.toString();
  }

  private static String normalizeStartRow(Range range) {
    Key startKey = range.getStartKey();
    if (startKey == null)
      return null;
    String startRow = new String(startKey.getRowData().toArray(), StandardCharsets.UTF_8);
    if (!range.isStartKeyInclusive())
      return startRow+'\0';
    else
      return startRow;
  }

  private static String normalizeEndRow(Range range) {
    Key endKey = range.getEndKey();
    if (endKey == null)
      return null;
    String endRow = new String(endKey.getRowData().toArray(), StandardCharsets.UTF_8);
    if (!range.isEndKeyInclusive())
      return prevRow(endRow);
    else
      return endRow;
  }

  public static final char LAST_ONE_BYTE_CHAR = Character.toChars(Byte.MAX_VALUE)[0];

  private static String prevRow(String row) {
    if (row.charAt(row.length()-1) == '\0')
      return row.substring(0, row.length() - 1);
    else
      return row.substring(0,row.length()-1)
          + (char)((int)row.charAt(row.length()-1)-1)
          + LAST_ONE_BYTE_CHAR;
  }

  /**
   * Convert D4M string representation of individual rows/columns to Text objects.
   * No ':' allowed as an entire row name!
   * Last character in the string is an arbitrary separator char
   * that must not appear in the rows.
   * See UtilTest for more test cases.
   *
   * @param rowStr Ex: 'a,b,c,d,'
   * @return A Text object for each one.
   */
  public static Collection<Text> d4mRowToTexts(String rowStr) {
    if (rowStr == null || rowStr.isEmpty())
      return Collections.emptySet();
    String[] rowStrSplit = splitD4mString(rowStr);
    Collection<Text> ts = new HashSet<>(rowStrSplit.length);
    for (String row : rowStrSplit) {
      if (row.equals(":"))
        throw new IllegalArgumentException("rowStr cannot contain the label \":\" ---- " + rowStr);
      ts.add(new Text(row));
    }
    return ts;
  }

  public static String textsToD4mString(Collection<Text> texts) {
    return textsToD4mString(texts, DEFAULT_SEP_D4M_STRING);
  }

  public static String textsToD4mString(Collection<Text> texts, char sep) {
    if (texts == null)
      return "";
    StringBuilder sb = new StringBuilder();
    for (Text text : texts)
      sb.append(text).append(sep);
    return sb.toString();
  }

  public static String stringsToD4mString(Collection<String> texts) {
    return stringsToD4mString(texts, DEFAULT_SEP_D4M_STRING);
  }

  public static String stringsToD4mString(Collection<String> texts, char sep) {
    if (texts == null)
      return "";
    StringBuilder sb = new StringBuilder();
    for (String text : texts)
      sb.append(text).append(sep);
    return sb.toString();
  }

  public static Collection<Range> textsToRanges(Collection<Text> texts) {
    Collection<Range> ranges = new HashSet<>();
    for (Text text : texts)
      ranges.add(new Range(text));
    return ranges;
  }

  public static Collection<Range> stringsToRanges(Collection<String> texts) {
    Collection<Range> ranges = new HashSet<>();
    for (String text : texts)
      ranges.add(new Range(text));
    return ranges;
  }

  public static final String ONSCOPE_OPTION = "_ONSCOPE_";

  /**
   * Helper method for adding an option to an iterator
   * which OneTable and TwoTable Graphulo operations will interpret to limit the scopes an iterator applies to.
   * @param scopes Scopes to limit the iterator to. Only meaningful if not null, not empty, and not all.
   *               Choices are from {@link org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope}: scan, minc, majc.
   * @return The same IteratorSetting as the one passed in.
   */
  public static IteratorSetting addOnScopeOption(IteratorSetting itset, EnumSet<IteratorUtil.IteratorScope> scopes) {
    if (scopes == null || scopes.isEmpty() || scopes.size() == IteratorUtil.IteratorScope.values().length)
      return itset;
    String s = "";
    for (IteratorUtil.IteratorScope scope : scopes) {
      s += scope.name() + ',';
    }
    itset.addOption(ONSCOPE_OPTION, s);
    return itset;
  }

  /**
   * Add the given Iterator to a table on scan, minc, majc scopes.
   * If already present on a scope, does not re-add the iterator to that scope.
   * Call it "plus".
   * <p>
   * Respects {@link #ONSCOPE_OPTION} if present from {@link #addOnScopeOption}.
   */
  public static void applyIteratorSoft(IteratorSetting itset, TableOperations tops, String table) {
    // check for special option that limits iterator scope
    String scopeStrs = itset.getOptions().get(ONSCOPE_OPTION);
    EnumSet<IteratorUtil.IteratorScope> scopesToConsider;
    if (scopeStrs != null && !scopeStrs.isEmpty()) {
      scopesToConsider = EnumSet.noneOf(IteratorUtil.IteratorScope.class);
      for (String scope : splitD4mString(scopeStrs))
        scopesToConsider.add(
            IteratorUtil.IteratorScope.valueOf(scope.toLowerCase()));
    } else
      scopesToConsider = EnumSet.allOf(IteratorUtil.IteratorScope.class);

    // checking if iterator already exists. Not checking for conflicts.
    try {
      IteratorSetting existing;
      EnumSet<IteratorUtil.IteratorScope> enumSet = EnumSet.noneOf(IteratorUtil.IteratorScope.class);
      for (IteratorUtil.IteratorScope scope : scopesToConsider) {
        existing = tops.getIteratorSetting(table, itset.getName(), scope);
        if (existing == null)
          enumSet.add(scope);
      }
      tops.attachIterator(table, itset, enumSet);
    } catch (AccumuloSecurityException | AccumuloException e) {
      log.error("error trying to add "+itset+" to " + table, e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("no table: "+table, e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a copy of key with all fields except the ones specified cleared.
   * @param key The key to copy
   * @param pk What fields to retain from the key
   * @return A new Key object pointing to new copies of fields specified by pk; other fields are empty/default.
   */
  public static Key keyCopy(Key key, PartialKey pk) {
    if (key == null || pk == null)
      return null;
    switch (pk) {
      case ROW:
        return new Key(key.getRowData().toArray(), EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES, Long.MAX_VALUE, false, true);
      case ROW_COLFAM:
        return new Key(key.getRowData().toArray(), key.getColumnFamilyData().toArray(), EMPTY_BYTES, EMPTY_BYTES, Long.MAX_VALUE, false, true);
      case ROW_COLFAM_COLQUAL:
        return new Key(key.getRowData().toArray(), key.getColumnFamilyData().toArray(), key.getColumnQualifierData().toArray(), EMPTY_BYTES, Long.MAX_VALUE, false, true);
      case ROW_COLFAM_COLQUAL_COLVIS:
        return new Key(key.getRowData().toArray(), key.getColumnFamilyData().toArray(), key.getColumnQualifierData().toArray(), key.getColumnVisibilityData().toArray(), Long.MAX_VALUE, false, true);
      case ROW_COLFAM_COLQUAL_COLVIS_TIME:
        return new Key(key.getRowData().toArray(), key.getColumnFamilyData().toArray(), key.getColumnQualifierData().toArray(), key.getColumnVisibilityData().toArray(), key.getTimestamp(), false, true);
      case ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL:
        return new Key(key.getRowData().toArray(), key.getColumnFamilyData().toArray(), key.getColumnQualifierData().toArray(), key.getColumnVisibilityData().toArray(), key.getTimestamp(), key.isDeleted(), true);
      default:
        throw new AssertionError("unknown pk: "+pk);
    }
  }

//  public static void addIteratorDynamically(TableOperations tops, String tableName,
//                                     IteratorSetting setting, EnumSet<IteratorUtil.IteratorScope> scopes,
//                                     boolean beforeVers)
//      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
//    int maxPriority = Integer.MAX_VALUE;
//    if (beforeVers) {
//      IteratorSetting versSetting = tops.getIteratorSetting(tableName, "vers", scopes.iterator().next());
//      if (versSetting != null)
//        maxPriority = versSetting.getPriority();
//    }
//    tops.checkIteratorConflicts(tableName, setting, scopes);
//  }

//  * @param permitFetchColumns True allows use of {@link ScannerBase#fetchColumn}. False forces use of {@link ColumnQualifierFilter} explicitly.
//  *                           Set to false if filtering columns upstream would create large difficulty.

  /**
   * Apply an appropriate column filter based on the input string.
   * Four modes of operation:
   1. Null or blank ("") `colFilter`: do nothing.
   2. No ranges `colFilter`: use scanner.fetchColumn() which invokes an Accumulo system ColumnQualifierFilter.
   3. Singleton range `colFilter`: use Accumulo user ColumnSliceFilter.
   4. Multi-range `colFilter`: use Graphulo D4mRangeFilter.
   * @param colFilter column filter string
   * @param scanner to call fetchColumn() on, for case #2; and addScanIterator(), for cases #3 and #4
   * @param priority to use for scan iterator setting, for case #3 and #4
   */
//  @Deprecated
  //   * @deprecated Use {@link #applyGeneralColumnFilter(String, ScannerBase, DynamicIteratorSetting)} for more robust filter setting.
  public static void applyGeneralColumnFilter(String colFilter, ScannerBase scanner, int priority) {
//    System.err.println("colFilter: "+colFilter);
    if (colFilter != null && !colFilter.isEmpty()) {
      int pos1 = colFilter.indexOf(':');
      if (pos1 == -1) { // no ranges - collection of singleton texts
        for (Text text : GraphuloUtil.d4mRowToTexts(colFilter)) {
          scanner.fetchColumn(GraphuloUtil.EMPTY_TEXT, text);
        }
      } else {
        SortedSet<Range> ranges = GraphuloUtil.d4mRowToRanges(colFilter);
        assert ranges.size() > 0;
        IteratorSetting s;
        if (ranges.size() == 1) { // single range - use ColumnSliceFilter
          Range r = ranges.first();
          if (r.isInfiniteStartKey() && r.isInfiniteStopKey())
            return;               // Infinite case: no filtering.
          s = new IteratorSetting(priority, ColumnSliceFilter.class);
//          System.err.println("start: "+(r.isInfiniteStartKey() ? null : r.getStartKey().getRow().toString())
//              +"end: "+(r.isInfiniteStopKey() ? null : r.getEndKey().getRow().toString()));
          ColumnSliceFilter.setSlice(s, r.isInfiniteStartKey() ? null : r.getStartKey().getRow().toString(),
              true, r.isInfiniteStopKey() ? null : r.getEndKey().getRow().toString(), true);
//          System.err.println("!ok "+GraphuloUtil.d4mRowToRanges(colFilter));
        } else { // multiple ranges
//          System.err.println("ok "+GraphuloUtil.d4mRowToRanges(colFilter));
          s = D4mRangeFilter.iteratorSetting(1, D4mRangeFilter.KeyPart.COLQ, colFilter);
        }
        scanner.addScanIterator(s);
      }
    }
  }

  /**
   * Apply an appropriate column filter based on the input string.
   * Four modes of operation:
   * <ol>
   <li>1. Null or blank ("") `colFilter`: do nothing.</li>
   <li>2. No ranges `colFilter`: use scanner.fetchColumn() which invokes an Accumulo system {@link ColumnQualifierFilter}.</li>
   <li>3. Singleton range `colFilter`: use Accumulo user {@link ColumnSliceFilter}.</li>
   <li>4. Multi-range `colFilter`: use Graphulo {@link D4mRangeFilter}.</li>
   </ol>
   * @param colFilter column filter string
   * @param scanner to call fetchColumn() on, for case #2
   * @param dis to call append()/prepend() on, for cases #3 and #4
   * @param append True means call {@link DynamicIteratorSetting#append}. False means call {@link DynamicIteratorSetting#prepend}
   */
  public static void applyGeneralColumnFilter(String colFilter, ScannerBase scanner, DynamicIteratorSetting dis, boolean append) {
//    System.err.println("colFilter: "+colFilter);
    if (colFilter != null && !colFilter.isEmpty()) {
      int pos1 = colFilter.indexOf(':');
      if (pos1 == -1) { // no ranges - collection of singleton texts
        // todo - the order this filter applies is different. Ensure no logical bugs when we have case 2.
        for (Text text : GraphuloUtil.d4mRowToTexts(colFilter)) {
          scanner.fetchColumn(GraphuloUtil.EMPTY_TEXT, text);
        }
      } else {
        SortedSet<Range> ranges = GraphuloUtil.d4mRowToRanges(colFilter);
        assert ranges.size() > 0;
        IteratorSetting s;
        if (ranges.size() == 1) { // single range - use ColumnSliceFilter
          Range r = ranges.first();
          s = new IteratorSetting(1, ColumnSliceFilter.class);
//          System.err.println("start: "+(r.isInfiniteStartKey() ? null : r.getStartKey().getRow().toString())
//              +"end: "+(r.isInfiniteStopKey() ? null : r.getEndKey().getRow().toString()));
          ColumnSliceFilter.setSlice(s, r.isInfiniteStartKey() ? null : r.getStartKey().getRow().toString(),
              true, r.isInfiniteStopKey() ? null : r.getEndKey().getRow().toString(), true);
//          System.err.println("!ok "+GraphuloUtil.d4mRowToRanges(colFilter));
        } else { // multiple ranges
//          System.err.println("ok "+GraphuloUtil.d4mRowToRanges(colFilter));
          s = D4mRangeFilter.iteratorSetting(1, D4mRangeFilter.KeyPart.COLQ, colFilter);
        }
        if (append)
          dis.append(s);
        else
          dis.prepend(s);
      }
    }
  }

  /**
   * For use within an iterator stack.
   * Apply an appropriate column filter based on the input string.
   * Four modes of operation:
   1. Null or blank ("") `colFilter`: do nothing.
   2. No ranges `colFilter`: use Accumulo system ColumnQualifierFilter.
   3. Singleton range `colFilter`: use Accumulo user ColumnSliceFilter.
   4. Multi-range `colFilter`: use Graphulo D4mRangeFilter.
   * @param colFilter column filter string
   * @param skvi Parent / source iterator
   * @return SKVI with appropriate filter iterators placed in front of it.
   */
  public static SortedKeyValueIterator<Key,Value> applyGeneralColumnFilter(
      String colFilter, SortedKeyValueIterator<Key,Value> skvi, IteratorEnvironment env) throws IOException {
    if (colFilter == null || colFilter.isEmpty())
      return skvi;

    int pos1 = colFilter.indexOf(':');
    if (pos1 == -1) { // no ranges - collection of singleton texts
      Set<Column> colset = new HashSet<>();
      for (Text text : GraphuloUtil.d4mRowToTexts(colFilter)) {
        byte[] by = text.copyBytes();
//        log.debug("Printing characters of string TEXT LIM: "+ Key.toPrintableString(by, 0, text.getLength(), 100));
//        log.debug("Printing characters of string TEXT    : "+ Key.toPrintableString(by, 0, by.length, 100));
        colset.add(new Column(EMPTY_BYTES, text.copyBytes(), EMPTY_BYTES));
      }
      return new ColumnQualifierFilter(skvi, colset);

    } else {
      SortedSet<Range> ranges = GraphuloUtil.d4mRowToRanges(colFilter);
      assert ranges.size() > 0;

      if (ranges.size() == 1) { // single range - use ColumnSliceFilter
        Range r = ranges.first();
        Map<String,String> map = new HashMap<>();

        String start = r.isInfiniteStartKey() ? null : r.getStartKey().getRow().toString(),
            end = r.isInfiniteStopKey() ? null : r.getEndKey().getRow().toString();
        boolean startInclusive = true, endInclusive = true;

        if (start != null)
          map.put(ColumnSliceFilter.START_BOUND, start);
        if (end != null)
          map.put(ColumnSliceFilter.END_BOUND, end);
        map.put(ColumnSliceFilter.START_INCLUSIVE, String.valueOf(startInclusive));
        map.put(ColumnSliceFilter.END_INCLUSIVE, String.valueOf(endInclusive));

        SortedKeyValueIterator<Key,Value> filter = new ColumnSliceFilter();
        filter.init(skvi, map, env);
        return filter;

      } else { // multiple ranges
        SortedKeyValueIterator<Key,Value> filter = new D4mRangeFilter();
        filter.init(skvi, D4mRangeFilter.iteratorSetting(1, D4mRangeFilter.KeyPart.COLQ, colFilter).getOptions(), env);
        return filter;
      }
    }
  }

  /**
   * Create a new instance of a class whose name is given, as a descendent of a given subclass.
   */
  public static <E> E subclassNewInstance(String classname, Class<E> parentClass) {
    Class<?> c;
    try {
      c = Class.forName(classname);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Can't find class: " + classname, e);
    }
    Class<? extends E> cm;
    try {
      cm = c.asSubclass(parentClass);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(classname+" is not a subclass of "+parentClass.getName(), e);
    }
    try {
      return cm.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException("can't instantiate new instance of " + cm.getName(), e);
    }
  }


  /** If str begins with prefix, return a String containing the characters after the prefix. Otherwise return null. */
  public static String stringAfter(byte[] prefix, byte[] str) {
    return 0 == WritableComparator.compareBytes(str, 0, prefix.length, prefix, 0, prefix.length)
        ? new String(str, prefix.length, str.length - prefix.length, UTF_8)
        : null;
  }

  /**
   * Add Cartesian product of prefixes and suffixes to a string, each give as a D4M String.
   * @see #padD4mString_Single(String, String, String)
   */
  public static String padD4mString(String prefixes, String suffixes, String str) {
    if (prefixes == null || prefixes.isEmpty())
      prefixes = ",";
    if (suffixes == null || suffixes.isEmpty())
      suffixes = ",";
    if (prefixes.length()<=1 && suffixes.length()<=1)
      return str;

    if (d4mStringContainsRange(str)) {
//      if (suffixes.length()>1)
//        throw new UnsupportedOperationException("No way to append the suffixes "+suffixes+
//            " to a D4M String containing a Range: "+str);
      // add prefix to v0 Ranges. Goto full Range Objects because ':' is complicated.
      SortedSet<Range> tmp = GraphuloUtil.d4mRowToRanges(str), tmp2 = new TreeSet<>();
      for (String startPre : GraphuloUtil.splitD4mString(prefixes))
        for (Range range : tmp)
          tmp2.add(GraphuloUtil.prependPrefixToRange(startPre, range));
      str = GraphuloUtil.rangesToD4MString(tmp2, str.charAt(str.length()-1));
      prefixes = ",";
    }

    String s = "";
    for (String pre : GraphuloUtil.splitD4mString(prefixes)) {
      for (String suf : GraphuloUtil.splitD4mString(suffixes)) {
        s += padD4mString_Single(pre, suf, str);
      }
    }
    return s;
  }

  /**
   * Does the str contain the colon+separator, meaning it has a range?
   */
  public static boolean d4mStringContainsRange(String str) {
    String cont = ":"+str.charAt(str.length()-1);
    return str.contains(cont);
  }

  /** Add prefix and/or suffix to every part of a D4M string.
   * prependStartPrefix("pre|","X","a,b,:,v,:,") ==>
   *  "pre|aX,pre|bX,:,pre|vX,:,"
   * */
  public static String padD4mString_Single(String prefix, String suffix, String str) {
    if (prefix == null)
      prefix = "";
    if (suffix == null)
      suffix = "";
    if (prefix.isEmpty() && suffix.isEmpty())
      return str;
    char sep = str.charAt(str.length() - 1);
    StringBuilder sb = new StringBuilder();
    for (String part : GraphuloUtil.splitD4mString(str)) {
      if (part.equals(":"))
        sb.append(part).append(sep);
      else
        sb.append(prefix).append(part).append(suffix).append(sep);
    }
    return sb.toString();
  }

  /**
   * Count the number of terms in a D4M String that do NOT have any ranges.
   * @param s D4M String
   * @return Number of terms in the D4M String.
   */
  public static int numD4mStr(String s) {
    if (s == null || s.isEmpty())
      return 0;
    if (s.contains(":"))
      throw new IllegalArgumentException("Cannot count number of terms in a D4M String with a range: "+s);
    int cnt = 0, pos = -1;
    char sep = s.charAt(s.length()-1);
    while ((pos = s.indexOf(sep,++pos)) != -1)
      cnt++;
    return cnt;
  }


  /**
   * Pad a range with a prefix, so the new range points to entries
   * that begin with the prefix and then satisfy the original range.
   * Only uses the Row field of the original Range; discards the rest.
   * @param pre The prefix
   * @param rold The original Range
   * @return New Range of the prefix plus the original
   */
  public static Range prependPrefixToRange(String pre, Range rold) {
    if (pre == null || pre.isEmpty())
      return rold;
    if (rold.isInfiniteStopKey() && rold.isInfiniteStartKey())
      return Range.prefix(pre);
    if (rold.isInfiniteStartKey())
      return new Range(pre, true, pre+normalizeEndRow(rold), true);
    if (rold.isInfiniteStopKey())
      return new Range(pre+normalizeStartRow(rold), true,
          Range.followingPrefix(new Text(pre)).toString(), false);
    return new Range(pre+normalizeStartRow(rold), true,
        pre+normalizeEndRow(rold), true);
  }

  public static boolean d4mStringContainsEmptyString(String str) {
    Preconditions.checkArgument(str != null && !str.isEmpty(), "%s is not a D4M String", str);
    if (str.length()==1)
      return true;
    String sep = Character.toString(str.charAt(str.length()-1));
    String sepsep = sep+sep;
    return str.contains(sepsep);

  }

  /**
   * Makes each input term into a prefix range.
   * <pre>
   *  "v1,v5," => "v1|,:,v1},v5|,:,v5},"
   *  "v1,:,v3,v5," => "v1,:,v3,v5|,:,v5},"
   * </pre>
   */
  public static String singletonsAsPrefix(String str) {
    Preconditions.checkNotNull(str);
    Preconditions.checkArgument(!str.isEmpty());
//    Preconditions.checkArgument(str.indexOf(':') != -1, "Cannot have the ':' character: "+str);
    char sep = str.charAt(str.length() - 1);
    if (d4mStringContainsEmptyString(str)) // empty prefix is full range.
      return ":"+sep;

    if (!d4mStringContainsRange(str)) {
      StringBuilder sb = new StringBuilder();
      for (String vktext : GraphuloUtil.splitD4mString(str)) {
        sb.append(vktext).append(sep)
            .append(':').append(sep)
            .append(prevRow(Range.followingPrefix(new Text(vktext)).toString()))
            .append(sep);
      }
      return sb.toString();
    }

//    Collection<Range> origRngs = d4mRowToRanges(str);
//    for (Range rng : origRngs) {
//      // if a singleton row, then make into a prefix row
//
//    }

//    String[] strSplit = str.substring(0, str.length() - 1)
//        .split(String.valueOf(sep));
//    List<String> strList = Arrays.asList(strSplit);
//    PeekingIterator3<String> pi = new PeekingIterator3<>(strList.iterator());
//    SortedSet<Range> rngset = new TreeSet<>();
//
//    if (pi.peekFirst().equals(":")) { // (-Inf,
//      if (pi.peekSecond() == null) {
//        return str; // (-Inf,+Inf)
//      } else {
//        if (pi.peekSecond().equals(":") || (pi.peekThird() != null && pi.peekThird().equals(":")))
//          throw new IllegalArgumentException("Bad D4M rowStr: " + str);
////        sb.append(':').append(sep).append(pi.peekSecond()).append(sep); // (-Inf,2]
//        rngset.add(new Range(null, false, pi.peekSecond(), true)); // (-Inf,2]
//        pi.next();
//        pi.next();
//      }
//    }
//
//    while (pi.hasNext()) {
//      if (pi.peekSecond() == null) { // last singleton row [1,1~)
////        sb.append(pi.peekFirst()).append(sep)
////            .append(':').append(sep)
////            .append(Range.followingPrefix(new Text(pi.peekFirst())).toString()).append(sep);
//        rngset.add(Range.prefix(pi.peekFirst()));
//
//      } else if (pi.peekSecond().equals(":")) {
//        if (pi.peekThird() == null) { // [1,+Inf)
////          sb.append(pi.peekFirst()).append(sep).append(':').append(sep);
//          rngset.add(new Range(pi.peekFirst(), true, null, false));
//
//        } else { String s = GraphuloUtil.singletonsAsPrefix(vktexts, sep);// [1,3]
//          if (pi.peekThird().equals(":"))
//            throw new IllegalArgumentException("Bad D4M rowStr: " + str);
////          sb.append(pi.peekFirst()).append(sep)
////              .append(':').append(sep)
////              .append(pi.peekThird()).append(sep);
//          rngset.add(new Range(pi.peekFirst(), true, pi.peekThird(), true));
//          pi.next();
//          pi.next();
//          pi.next();
//        }
//      } else { // [1,1~)
////        sb.append(pi.peekFirst()).append(sep)
////            .append(':').append(sep)
////            .append(Range.followingPrefix(new Text(pi.peekFirst())).toString()).append(sep);
//        rngset.add(Range.prefix(pi.peekFirst()));
//        pi.next();
//      }
//    }

    Collection<Range> prefixRngs = d4mRowToRanges(str, true);
//    log.info(prefixRngs);
    return rangesToD4MString(prefixRngs, sep);
  }

  /**
   * Makes each input term into a prefix range.
   * "v1,v5," => "v1,:,v1\255,v5,:,v5\255,"
   */
  public static String singletonsAsPrefix(Collection<Text> vktexts, char sep) {
    StringBuilder sb = new StringBuilder();
    for (Text vktext : vktexts) {
      sb.append(vktext.toString()).append(sep)
          .append(':').append(sep)
          .append(prevRow(Range.followingPrefix(new Text(vktext)).toString()))
          .append(sep);
    }
    return sb.toString();
  }


  public static Map.Entry<Key, Value> copyTopEntry(SortedKeyValueIterator<Key, Value> skvi) {
    final Key k = keyCopy(skvi.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL);
    final Value v = new Value(skvi.getTopValue());
    return new Map.Entry<Key, Value>() {
      @Override
      public Key getKey() {
        return k;
      }

      @Override
      public Value getValue() {
        return v;
      }

      @Override
      public Value setValue(Value value) {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Write entries to a table.
   */
  public static void writeEntries(Connector connector, Map<Key, Value> map, String table, boolean createIfNotExist) {
    if (createIfNotExist && !connector.tableOperations().exists(table))
      try {
        connector.tableOperations().create(table);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("trouble creating "+table, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }

    BatchWriterConfig bwc = new BatchWriterConfig();
    bwc.setMaxWriteThreads(25);
    BatchWriter bw;
    try {
      bw = connector.createBatchWriter(table, bwc);
    } catch (TableNotFoundException e) {
      log.error("tried to write to a non-existant table "+table, e);
      throw new RuntimeException(e);
    }

    try {
      for (Map.Entry<Key, Value> entry : map.entrySet()) {
        Key k = entry.getKey();
        ByteSequence rowData = k.getRowData(),
            cfData = k.getColumnFamilyData(),
            cqData = k.getColumnQualifierData();
        Mutation m = new Mutation(rowData.toArray(), rowData.offset(), rowData.length());
        m.put(cfData.toArray(), cqData.toArray(), k.getColumnVisibilityParsed(), entry.getValue().get());
        bw.addMutation(m);
      }

    } catch (MutationsRejectedException e) {
      log.error("mutations rejected", e);
      throw new RuntimeException(e);
    } finally {
      try {
        bw.close();
      } catch (MutationsRejectedException e) {
        log.error("mutations rejected while trying to close BatchWriter", e);
      }
    }
  }

  /** Copy the splits placed on table t1 to table t2. */
  public static void copySplits(TableOperations tops, String t1, String t2) throws AccumuloException {
    try {
      Collection<Text> splits = tops.listSplits(t1);
      SortedSet<Text> ss = new TreeSet<>(splits);
      tops.addSplits(t2, ss);
    } catch (TableNotFoundException | AccumuloSecurityException e) {
      log.error("cannot handle splits copying from "+t1+" to "+t2, e);
      throw new RuntimeException(e);
    }
  }

  /** Delete tables. If they already exist, delete and re-create them if forceDelete==true,
   * otherwise throw an IllegalStateException. */
  public static void deleteTables(Connector connector, String... tns) {
    TableOperations tops = connector.tableOperations();
    for (String tn : tns) {
      if (tn != null && tops.exists(tn)) {
        try {
          tops.delete(tn);
        } catch (AccumuloException | AccumuloSecurityException e) {
          log.error("Problem deleing temporary table " + tn, e);
          throw new RuntimeException(e);
        } catch (TableNotFoundException ignored) {
        }
      }
    }
  }

  /** Switches row and column qualifier. Returns HashMap. */
  public static <V> Map<Key,V> transposeMap(Map<Key, V> mapOrig) {
      Map<Key, V> m = new HashMap<>(mapOrig.size());
      return transposeMapHelp(mapOrig, m);
  }

  /** Switches row and column qualifier. Use same comparator as the given map. Returns TreeMap. */
  public static <V> SortedMap<Key,V> transposeMap(SortedMap<Key, V> mapOrig) {
    SortedMap<Key, V> m = new TreeMap<>(mapOrig.comparator());
    return transposeMapHelp(mapOrig, m);
  }

  private static <M extends Map<Key,V>, V> M transposeMapHelp(Map<Key,V> orig, M neww) {
    for (Map.Entry<Key, V> entry : orig.entrySet()) {
      Key k0 = entry.getKey();
      Key k = new Key(k0.getColumnQualifier(), k0.getColumnFamily(),
          k0.getRow(), k0.getColumnVisibilityParsed(), k0.getTimestamp());
      neww.put(k, entry.getValue());
    }
    return neww;
  }

  /**
   * Generates RemoteSourceIterator (possibly x2), TwoTableIterator, RemoteWriteIterator
   * configuration through a DynamicIteratorSetting.
   * @param map Map of all options.
   * @param priority Priority to use for the IteratorSetting of the whole stack
   * @param name Null means use the default name "TableMultIterator"
   */
  public static IteratorSetting tableMultIterator(
      Map<String, String> map,
      int priority, String name) {
    Map<String, String> optDM = new HashMap<>(), optC = new HashMap<>();
    {
      Map<String, Map<String, String>> prefixMap = GraphuloUtil.splitMapPrefix(map);
      for (Map.Entry<String, Map<String, String>> prefixEntry : prefixMap.entrySet()) {
        final String prefix = prefixEntry.getKey();
        Map<String, String> entryMap = prefixEntry.getValue();

        switch (prefix) {
          case TwoTableIterator.PREFIX_AT:
          case TwoTableIterator.PREFIX_B:
            optDM.putAll(GraphuloUtil.preprendPrefixToKey(prefix + '.', entryMap));
            break;
          case "C":
            optC.putAll(entryMap);
            break;
          default:
            for (Map.Entry<String, String> entry : entryMap.entrySet()) {
//              switch (entry.getKey()) {
//                case "dotmode":
//                case "multiplyOp":
//                  optDM.put(entry.getKey(), entry.getValue());
//                  break;
//                default:
//                  log.warn("Unrecognized option: " + prefix + '.' + entry);
//                  break;
//              }
              if (prefix.isEmpty())
                optDM.put(entry.getKey(), entry.getValue());
              else
                optDM.put(prefix + '.'+entry.getKey(), entry.getValue());
            }
            break;
        }
      }
    }
    DynamicIteratorSetting dis = new DynamicIteratorSetting(priority, name == null ? "TableMultIterator" : name)
        .append(new IteratorSetting(1, TwoTableIterator.class, optDM));
    if (!optC.isEmpty())
      dis.append(new IteratorSetting(1, RemoteWriteIterator.class, optC));
    return dis.toIteratorSetting();
  }

  public static void createTables(Connector connector, boolean deleteIfExists, String... tables) {
    TableOperations tops = connector.tableOperations();
    for (String tn : tables) {
      if (tn == null)
        continue;
      if (deleteIfExists && tops.exists(tn))
        try {
          tops.delete(tn);
        } catch (AccumuloException | AccumuloSecurityException e) {
          log.warn("trouble deleting table "+tn, e);
          throw new RuntimeException(e);
        } catch (TableNotFoundException ignored) {
        }
      if (!tops.exists(tn))
        try {
          tops.create(tn);
        } catch (AccumuloException | AccumuloSecurityException e) {
          log.warn("trouble creating table " + tn, e);
          throw new RuntimeException(e);
        } catch (TableExistsException ignored) {
        }
    }
  }

  /**
   * Copy out the private method from Key.
   * @see Key
   */
  public static byte[] followingArray(byte ba[]) {
    byte[] fba = new byte[ba.length + 1];
    System.arraycopy(ba, 0, fba, 0, ba.length);
    fba[ba.length] = (byte) 0x00;
    return fba;
  }



  /**
   * Write the bytes of i, beginning with the first non-zero one.
   */
  public static byte[] writeVUnsignedLong(long i) {
    boolean pos = i >= 0;
    if( pos ) {
      if (i == 0)
        return EMPTY_BYTES;
      if (i <= 0xFF)
        return new byte[]{(byte) i};
      if (i <= 0xFFFF)
        return new byte[]{(byte) (i >> 8), (byte) i};
      if (i <= 0xFFFFFF)
        return new byte[]{(byte) (i >> 16), (byte) (i >> 8), (byte) i};
      if (i <= 0xFFFFFFFF)
        return new byte[]{(byte) (i >> 24), (byte) (i >> 16), (byte) (i >> 8), (byte) i};
      if (i <= 0xFFFFFFFFFFL)
        return new byte[]{(byte) (i >> 32), (byte) (i >> 24), (byte) (i >> 16), (byte) (i >> 8), (byte) i};
      if (i <= 0xFFFFFFFFFFFFL)
        return new byte[]{(byte) (i >> 40), (byte) (i >> 32), (byte) (i >> 24), (byte) (i >> 16), (byte) (i >> 8), (byte) i};
      if (i <= 0xFFFFFFFFFFFFFFL)
        return new byte[]{(byte) (i >> 48), (byte) (i >> 40), (byte) (i >> 32), (byte) (i >> 24), (byte) (i >> 16), (byte) (i >> 8), (byte) i};
    }
    return new byte[] {(byte) (i >> 56), (byte) (i >> 48), (byte) (i >> 40), (byte) (i >> 32), (byte) (i >> 24), (byte) (i >> 16), (byte) (i >> 8), (byte) i};
  }


  /**
   * Read a long encoded with writeVUnsignedLong.
   */
  public static long readUnsignedVLong(byte[] b) {
    long l = 0;
    for (byte aB : b) {
      l <<= 8;
      l |= aB & 0xFF;
    }
    return l;
  }

}
