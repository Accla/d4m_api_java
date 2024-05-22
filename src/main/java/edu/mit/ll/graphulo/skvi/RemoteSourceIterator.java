package edu.mit.ll.graphulo.skvi;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.PeekingIterator1;
import edu.mit.ll.graphulo.util.SerializationUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
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
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Reads from a remote Accumulo table.
 */
public class RemoteSourceIterator implements SortedKeyValueIterator<Key, Value>/*, OptionDescriber*/ {
  private static final Logger log = LoggerFactory.getLogger(RemoteSourceIterator.class);

  /** The original options passed to init. Retaining this makes deepCopy much easier-- call init again and done! */
  private Map<String,String> origOptions;

  private String instanceName;
  private String tableName;
  private String zookeeperHost;
  private String username;
  private AuthenticationToken auth = null;
  private Authorizations authorizations = Authorizations.EMPTY;
  /**
   * Zookeeper timeout in milliseconds
   */
  private int timeout = -1;

  private boolean doWholeRow = false,
      doClientSideIterators = false;
  private DynamicIteratorSetting dynamicIteratorSetting;
  private SortedSet<Range> rowRanges = new TreeSet<>(Collections.singleton(new Range()));
  /**
   * The range given by seek. Clip to this range.
   */
  private Range seekRange;

  /**
   * Holds the current range we are scanning.
   * Goes through the part of ranges after seeking to the beginning of the seek() clip.
   */
  private Iterator<Range> rowRangeIterator;
  private String colFilter = "";

  /**
   * Created in init().
   */
  private Scanner scanner;
  /**
   * Buffers one entry from the remote table.
   */
  private PeekingIterator1<Map.Entry<Key, Value>> remoteIterator;

  public static final String
      ZOOKEEPERHOST = "zookeeperHost",
      TIMEOUT = "timeout",
      INSTANCENAME = "instanceName",
      TABLENAME = "tableName",
      USERNAME = "username";

  public static final String
      PASSWORD = "password"; // alternative to AUTHENTICATION_TOKEN that passes password directly
  public static final String
      AUTHENTICATION_TOKEN = "authenticationToken", // base64 encoding of token
      AUTHENTICATION_TOKEN_CLASS = "authenticationTokenClass", // class of token
      AUTHORIZATIONS = "authorizations",
      ROWRANGES = "rowRanges",
      COLFILTER = "colFilter",
      DOCLIENTSIDEITERATORS = "doClientSideIterators",
      ITER_PREFIX = "diter.";

  public static IteratorSetting iteratorSetting(
      int priority, String zookeeperHost, int timeout, String instanceName, String tableName, String username,
      String password,
      Authorizations authorizations, String rowRanges, String colFilter, boolean doClientSideIterators,
      DynamicIteratorSetting remoteIterators) {
    Preconditions.checkNotNull(tableName, "Param %s is required", TABLENAME);
    return new IteratorSetting(priority, RemoteSourceIterator.class, optionMap(null, tableName, zookeeperHost, timeout, instanceName,
        username, password, authorizations, rowRanges, colFilter, doClientSideIterators, remoteIterators));
  }

  public static IteratorSetting iteratorSetting(
      int priority, String zookeeperHost, int timeout, String instanceName, String tableName, String username,
      AuthenticationToken token,
      Authorizations authorizations, String rowRanges, String colFilter, boolean doClientSideIterators,
      DynamicIteratorSetting remoteIterators) {
    Preconditions.checkNotNull(tableName, "Param %s is required", TABLENAME);
    return new IteratorSetting(priority, RemoteSourceIterator.class, optionMap(null, tableName, zookeeperHost, timeout, instanceName,
        username, token, authorizations, rowRanges, colFilter, doClientSideIterators, remoteIterators));
  }

  /**
   *
   * @param map Map to reuse. Pass null to create a new HashMap.
   * @param tableName Name of table to read from.
   * @param zookeeperHost Zookeeper host for Connector to remote table.
   * @param timeout Timeout for Connector to remote table. <= 0 means use default timeout.
   * @param instanceName Instance name for Connector to remote table.
   * @param username Accumulo Username for Connector to remote table.
   * @param password Used in a PasswordToken for Connector to remote table. Passed in plaintext.
   * @param authorizations Authorizations to use while scanning. Null means {@link Authorizations#EMPTY}
   * @param rowRanges Applied to this class's Scanner. Null means all rows. TODO: Would using a SeekFilterIterator lead to better performance?
   * @param colFilter Column filter, see {@link GraphuloUtil#applyGeneralColumnFilter(String, SortedKeyValueIterator, IteratorEnvironment)}.
   * @param doClientSideIterators Whether to apply remoteIterators at the remote table or locally. Meaningless if remoteIterators is null. Null means false.
   * @param remoteIterators If doClientSideIterators=false, these iterators are applied on the remote server.
   *                        If doClientSideIterators=true, these iterators are applied locally.
   *                        Null means no extra iterators.
   * @return map with options filled in.
   */
  public static Map<String,String> optionMap(
      Map<String, String> map, String tableName, String zookeeperHost, int timeout, String instanceName, String username,
      String password,
      Authorizations authorizations, String rowRanges, String colFilter, Boolean doClientSideIterators,
      DynamicIteratorSetting remoteIterators) {
    if (map == null)
      map = new HashMap<>();
    if (tableName != null)
      map.put(TABLENAME, tableName);
    if (zookeeperHost != null)
      map.put(ZOOKEEPERHOST, zookeeperHost);
    if (timeout > 0)
      map.put(TIMEOUT, Integer.toString(timeout));
    if (instanceName != null)
      map.put(INSTANCENAME, instanceName);
    if (username != null)
      map.put(USERNAME, username);
    if (password != null)
      map.put(PASSWORD, password);
    if (authorizations != null && !authorizations.equals(Authorizations.EMPTY))
      map.put(AUTHORIZATIONS, authorizations.serialize());
    if (rowRanges != null)
      map.put(ROWRANGES, rowRanges);
    if (colFilter != null)
      map.put(COLFILTER, colFilter);
    if (doClientSideIterators != null)
      map.put(DOCLIENTSIDEITERATORS, doClientSideIterators.toString());
    if (remoteIterators != null)
      map.putAll(remoteIterators.buildSettingMap(ITER_PREFIX));
    return map;
  }

  /**
   *
   * @param map Map to reuse. Pass null to create a new HashMap.
   * @param tableName Name of table to read from.
   * @param zookeeperHost Zookeeper host for Connector to remote table.
   * @param timeout Timeout for Connector to remote table. <= 0 means use default timeout.
   * @param instanceName Instance name for Connector to remote table.
   * @param username Accumulo Username for Connector to remote table.
   * @param token AuthenticationToken containing credentials for user. Passed via Base64 encoding (no encryption).
   * @param authorizations Authorizations to use while scanning. Null means {@link Authorizations#EMPTY}
   * @param rowRanges Applied to this class's Scanner. Null means all rows. TODO: Would using a SeekFilterIterator lead to better performance?
   * @param colFilter Column filter, see {@link GraphuloUtil#applyGeneralColumnFilter(String, SortedKeyValueIterator, IteratorEnvironment)}.
   * @param doClientSideIterators Whether to apply remoteIterators at the remote table or locally. Meaningless if remoteIterators is null. Null means false.
   * @param remoteIterators If doClientSideIterators=false, these iterators are applied on the remote server.
   *                        If doClientSideIterators=true, these iterators are applied locally.
   *                        Null means no extra iterators.
   * @return map with options filled in.
   */
  public static Map<String,String> optionMap(
      Map<String, String> map, String tableName, String zookeeperHost, int timeout, String instanceName, String username,
      AuthenticationToken token,
      Authorizations authorizations, String rowRanges, String colFilter, Boolean doClientSideIterators,
      DynamicIteratorSetting remoteIterators) {
    if (map == null)
      map = new HashMap<>();
    if (tableName != null)
      map.put(TABLENAME, tableName);
    if (zookeeperHost != null)
      map.put(ZOOKEEPERHOST, zookeeperHost);
    if (timeout > 0)
      map.put(TIMEOUT, Integer.toString(timeout));
    if (instanceName != null)
      map.put(INSTANCENAME, instanceName);
    if (username != null)
      map.put(USERNAME, username);
    if (token != null) {
      map.put(AUTHENTICATION_TOKEN_CLASS, token.getClass().getName());
      map.put(AUTHENTICATION_TOKEN, SerializationUtil.serializeWritableBase64(token));
    }
    if (authorizations != null && !authorizations.equals(Authorizations.EMPTY))
      map.put(AUTHORIZATIONS, authorizations.serialize());
    if (rowRanges != null)
      map.put(ROWRANGES, rowRanges);
    if (colFilter != null)
      map.put(COLFILTER, colFilter);
    if (doClientSideIterators != null)
      map.put(DOCLIENTSIDEITERATORS, doClientSideIterators.toString());
    if (remoteIterators != null)
      map.putAll(remoteIterators.buildSettingMap(ITER_PREFIX));
    return map;
  }

  private void parseOptions(Map<String, String> map) {
    Map<String,String> diterMap = new HashMap<>();
    String token = null, tokenClass = null;
    for (Map.Entry<String, String> optionEntry : map.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
      if (optionValue.isEmpty())
        continue;
      if (optionKey.startsWith(ITER_PREFIX)) {
        diterMap.put(optionKey.substring(ITER_PREFIX.length()), optionValue);
      } else {
        switch (optionKey) {
          case ZOOKEEPERHOST:
            zookeeperHost = optionValue;
            break;
          case TIMEOUT:
            timeout = Integer.parseInt(optionValue);
            break;
          case INSTANCENAME:
            instanceName = optionValue;
            break;
          case TABLENAME:
            tableName = optionValue;
            break;
          case USERNAME:
            username = optionValue;
            break;

          case PASSWORD:
            auth = new PasswordToken(optionValue);
            break;
          case AUTHENTICATION_TOKEN:
            token = optionValue;
            break;
          case AUTHENTICATION_TOKEN_CLASS:
            tokenClass = optionValue;
            break;

          case AUTHORIZATIONS: // passed value must be from Authorizations.serialize()
            authorizations = new Authorizations(optionValue.getBytes(StandardCharsets.UTF_8));
            break;
          case "doWholeRow":
            doWholeRow = Boolean.parseBoolean(optionValue);
            break;
          case ROWRANGES:
            rowRanges = parseRanges(optionValue);
            break;
          case COLFILTER:
            colFilter = optionValue; //GraphuloUtil.d4mRowToTexts(optionValue);
            break;
          case DOCLIENTSIDEITERATORS:
            doClientSideIterators = Boolean.parseBoolean(optionValue);
            break;
          default:
            log.warn("Unrecognized option: " + optionEntry);
            break;
        }
      }
//      log.trace("Option OK: " + optionEntry);
    }
    Preconditions.checkArgument((auth == null && token != null && tokenClass != null) ||
        (token == null && tokenClass == null && auth != null),
        "must specify only one kind of authentication: password=%s, token=%s, tokenClass=%s",
        auth, token, tokenClass);
    if (auth == null) {
      auth = GraphuloUtil.subclassNewInstance(tokenClass, AuthenticationToken.class);
      SerializationUtil.deserializeWritableBase64(auth, token);
    }

    if (!diterMap.isEmpty())
      dynamicIteratorSetting = DynamicIteratorSetting.fromMap(diterMap);
    // Required options
    if (zookeeperHost == null ||
        instanceName == null ||
        tableName == null ||
        username == null ||
        auth == null)
      throw new IllegalArgumentException("not enough options provided");
  }

  /**
   * Parse string s in the Matlab format "row1,row5,row7,:,row9,w,:,z,zz,:,"
   * Does not have to be ordered but cannot overlap.
   *
   * @param s -
   * @return a bunch of ranges
   */
  static SortedSet<Range> parseRanges(String s) {
    Collection<Range> rngs = GraphuloUtil.d4mRowToRanges(s);
    rngs = Range.mergeOverlapping(rngs);
    return new TreeSet<>(rngs);
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> map, IteratorEnvironment iteratorEnvironment) throws IOException {
    if (source != null)
      log.warn("RemoteSourceIterator ignores/replaces parent source passed in init(): " + source);
    origOptions = new HashMap<>(map); // defensive copy

    parseOptions(map);
    setupConnectorScanner();

    log.debug("RemoteSourceIterator on table " + tableName + ": init() succeeded");
  }

  private void setupConnectorScanner() {
    ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeeperHost);
    if (timeout != -1)
      cc = cc.withZkTimeout(timeout);
    Instance instance = new ZooKeeperInstance(cc);
    Connector connector;
    try {
      connector = instance.getConnector(username, auth);
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("failed to connect to Accumulo instance " + instanceName, e);
      throw new RuntimeException(e);
    }

    try {
      scanner = connector.createScanner(tableName, authorizations);
    } catch (TableNotFoundException e) {
      log.error(tableName + " does not exist in instance " + instanceName, e);
      throw new RuntimeException(e);
    }

    if (doClientSideIterators)
      scanner = new ClientSideIteratorScanner(scanner);

    if (dynamicIteratorSetting == null)
      GraphuloUtil.applyGeneralColumnFilter(colFilter,scanner,10);
    else {
      GraphuloUtil.applyGeneralColumnFilter(colFilter, scanner, dynamicIteratorSetting, false); // prepend
      scanner.addScanIterator(dynamicIteratorSetting.toIteratorSetting());
    }

    if (doWholeRow) { // This is a legacy setting.
      // TODO: make priority dynamic in case 25 is taken; make name dynamic in case iterator name already exists. Or buffer here.
      scanner.addScanIterator(new IteratorSetting(25, WholeRowIterator.class));
    }

  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    scanner.close();
  }

  /**
   * Advance to the first subset range whose end key >= the seek start key.
   */
  public static Iterator<Range> getFirstRangeStarting(PeekingIterator1<Range> iter, Range seekRange) {
    if (!seekRange.isInfiniteStartKey())
      while (iter.hasNext() && !iter.peek().isInfiniteStopKey()
          && ((iter.peek().getEndKey().equals(seekRange.getStartKey()) && !seekRange.isEndKeyInclusive())
          || iter.peek().getEndKey().compareTo(seekRange.getStartKey()) < 0)) {
        iter.next();
      }
    return iter;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    log.debug("RemoteSourceIterator on table " + tableName + ": seek(): " + range);
    /** configure Scanner to the first entry to inject after the start of the range.
     Range comparison: infinite start first, then inclusive start, then exclusive start
     {@link org.apache.accumulo.core.data.Range#compareTo(Range)} */
    seekRange = range;
    rowRangeIterator = getFirstRangeStarting(new PeekingIterator1<>(rowRanges.iterator()), range); //rowRanges.tailSet(range).iterator();
    remoteIterator = PeekingIterator1.emptyIterator();
    next();
  }

//  /**
//   * Restrict columns fetched to the ones given. Takes effect on next seek().
//   *
//   * @param columns Columns to fetch. Null or empty collection for all columns.
//   * @throws IOException
//   */
//  public void setFetchColumns(Collection<IteratorSetting.Column> columns) throws IOException {
//    scanner.clearColumns();
//    if (columns != null)
//      for (IteratorSetting.Column column : columns) {
//        if (column.getColumnQualifier() == null)    // fetch all columns in this column family
//          scanner.fetchColumnFamily(column.getColumnFamily());
//        else
//          scanner.fetchColumn(column.getColumnFamily(), column.getColumnQualifier());
//      }
//  }


  @Override
  public boolean hasTop() {
    return remoteIterator.hasNext();
  }

  @Override
  public void next() throws IOException {
    if (rowRangeIterator == null || remoteIterator == null)
      throw new IllegalStateException("next() called before seek() b/c rowRangeIterator or remoteIterator not set");
    remoteIterator.next(); // does nothing if there is no next (i.e. hasTop()==false)
    while (!remoteIterator.hasNext() && rowRangeIterator.hasNext()) {
      Range range = rowRangeIterator.next();
      range = range.clip(seekRange, true); // clip to the seek range
      if (range == null) // empty intersection - no more ranges by design
        return;
      scanner.setRange(range);
      remoteIterator = new PeekingIterator1<>(scanner.iterator());
    }
    // either no ranges left and we finished the current scan OR remoteIterator.hasNext()==true
//    if (hasTop())
//      log.trace(tableName + " prepared next entry " + getTopKey() + " ==> "
//          + (doWholeRow ? WholeRowIterator.decodeRow(getTopKey(), getTopValue()) : getTopValue()));
//    else
//      log.trace(tableName + " hasTop() == false");
  }

  @Override
  public Key getTopKey() {
    return remoteIterator.peek().getKey(); // returns null if hasTop()==false
  }

  @Override
  public Value getTopValue() {
    return remoteIterator.peek().getValue();
  }

  @Override
  public RemoteSourceIterator deepCopy(IteratorEnvironment iteratorEnvironment) {
    RemoteSourceIterator copy = new RemoteSourceIterator();
    try {
      copy.init(null, origOptions, iteratorEnvironment);
    } catch (IOException e) {
      log.error("Problem creating deepCopy of RemoteSourceIterator on table "+tableName, e);
      throw new RuntimeException(e);
    }
    return copy;
  }
}
