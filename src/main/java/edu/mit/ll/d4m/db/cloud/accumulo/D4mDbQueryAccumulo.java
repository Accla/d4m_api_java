package edu.mit.ll.d4m.db.cloud.accumulo;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.D4mConfig;
import edu.mit.ll.d4m.db.cloud.D4mDbResultSet;
import edu.mit.ll.d4m.db.cloud.D4mDbRow;
import edu.mit.ll.d4m.db.cloud.D4mException;
import edu.mit.ll.d4m.db.cloud.D4mParentQuery;
import edu.mit.ll.d4m.db.cloud.util.CompareUtil;
import edu.mit.ll.d4m.db.cloud.util.D4mDataObj;
import edu.mit.ll.d4m.db.cloud.util.RegExpUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author William Smith
 */
public class D4mDbQueryAccumulo extends D4mParentQuery {
	private static final Logger log = LoggerFactory.getLogger(D4mDbQueryAccumulo.class);
	//	private String tableName = "";
	private int numberOfThreads = 50;
	public String rowReturnString = "";
	public String columnReturnString = "";
	public String valueReturnString = "";
	public static final String NEWLINE =   "\n"; // "\n" is necessary for correct parsing. //System.getProperty("line.separator");
	public boolean doTest = false;
	private static final String KEY_RANGE = "KEY_RANGE";
	private static final String REGEX_RANGE = "REGEX_RANGE";
	private static final String POSITIVE_INFINITY_RANGE = "POSITIVE_INFINITY_RANGE";
	private static final String NEGATIVE_INFINITY_RANGE = "NEGATIVE_INFINITY_RANGE";

	//	private ConnectionProperties connProps = new ConnectionProperties();
	private String family = "";
	//	private int limit=0; // number of elements (column)
	private int numRows=0; //number of rows
	private int count=0;
	private long cumCount=0; //cumulative count of results retrieved
	private String rowsQuery=null;
	private String colsQuery=null;
	private ParsedInfo rowInfo =null;
	private HashMap<String, String> rowStringMap=null;
	private ParsedInfo colInfo =null;
	private ArrayList<Key> rowKeys=null;
	private StringBuilder sbRowReturn = new StringBuilder();
	private StringBuilder sbColumnReturn = new StringBuilder();
	private StringBuilder sbValueReturn = new StringBuilder();
	ArrayList<D4mDbRow> rowList = new ArrayList<>();  //for testing

	private boolean startRowInclusive=true;
	private boolean endRowInclusive=true;
	private boolean positiveInfinity=false;
	private boolean doAllRanges=false;

	private Iterator<Entry<Key, Value>> scannerIter =null;

	private Key startKey      = null;

	private Range startRange  = null;
	private Scanner scanner = null;
	private BatchScanner bscanner = null;
	public D4mDbResultSet testResultSet=null;
	public boolean hasNext=false;
	private boolean getAllData = false;
	private LinkedList<Range> rangesList= new LinkedList<>();
	private CompareUtil compareUtil=null;
	AccumuloConnection connection=null;
	//private ConcurrentLinkedQueue <Entry<Key, Value>> dataQue=new ConcurrentLinkedQueue<Entry<Key,Value>>();
	public D4mDbQueryAccumulo() {
		super();
		this.count=0;
		//this.limit=0;
		this.cumCount = 0;
	}
	/**
	 * Constructor that may use ZooKeeperInstance or MasterInstance to connect
	 * to Accumulo.
	 */
	public D4mDbQueryAccumulo(ConnectionProperties connProps, String table) {
		this();
		super.tableName = table;
		super.connProps = connProps;
		this.numberOfThreads = this.connProps.getMaxNumThreads();
	}

	/**
	 * Constructor that uses ZooKeeperInstance to connect to Accumulo.
	 */
	public D4mDbQueryAccumulo(String instanceName, String host, String table, String username, String password) {
		this();
		super.tableName = table;
		super.connProps.setHost(host);
		super.connProps.setInstanceName(instanceName);
		super.connProps.setUser(username);
		super.connProps.setPass(password);
	}

	public D4mDbResultSet getAllData() throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
		this.getAllData = true;
		D4mDbResultSet results = new D4mDbResultSet();
		if(this.scannerIter == null) {
			Scanner scanner = getScanner(); //connection.getScanner(tableName);
			if(this.startRange == null) {
				//			this.startKey = new Key();
				this.startRange = new Range(this.startKey,null);
			}
			else if (this.startKey != null) {
				//Text row = this.startKey.getRow();
				this.startRange = new Range(this.startKey,null);
			}
			scanner.setRange(startRange);
			scanner.fetchColumnFamily(new Text(this.family));
			this.scannerIter = scanner.iterator();
		}
		long start = System.currentTimeMillis();


		iterateOverEntries(this.scannerIter);

		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (double)(System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(this.rowList);
		this.testResultSet = results;
		return results;
	}

	/*
	 * This method build the results of row, column, and value strings
	 *   Return false if the count is less than the limit, or if the limit is infinite
	 *   Return true if the count has reach the limit.
	 * 
	 */
	private boolean buildReturnString(String rowKey,  String column, String value) {
		boolean isDone=false;

		if(super.limit == 0 || this.count < super.limit) {
			if (log.isDebugEnabled())
				log.debug("  +++ ROW="+rowKey+",COL="+column+",VAL="+value+" +++");
			this.sbRowReturn.append(rowKey).append(NEWLINE);
			this.sbColumnReturn.append(column.replace(this.family, "")).append(NEWLINE);
			this.sbValueReturn.append(value).append(NEWLINE);
			this.count++;
			this.cumCount++;
			if(super.limit > 0 && this.count == super.limit) {
				isDone=true;
			}
		} else  {
			isDone = true;
			//this.cumCount = this.cumCount + (long)this.count;
		}

		return isDone;
	}
	private boolean buildReturnString(Key theKey, String rowKey,  String column, String value) {
		boolean isDone = buildReturnString(rowKey, column, value);
		if(isDone) {
			//Key startkey = this.startRange.getStartKey();
			//startkey.set(theKey);
			this.startKey = theKey;
		}

		if (D4mConfig.DEBUG) {
			this.saveTestResults(rowKey, theKey.getColumnFamily().toString() ,column, value);
		}

		return isDone;
	}
	private boolean buildReturnString(Key theKey, Value val) {
		String rowKey = theKey.getRow().toString();
		String column = theKey.getColumnQualifier().toString();
		String value = new String(val.get(), StandardCharsets.UTF_8);
		return buildReturnString(theKey,rowKey, column, value);
	}

	public boolean hasNext() {
		if(this.scannerIter != null) {
			this.hasNext = this.scannerIter.hasNext();
		}
		return this.hasNext;
	}
	/*
	 * Next method will return the next chunk of data.
	 * Chunk of data returned is determined by the limit.
	 * For example, if the limit is set to 100, then it would return 100 results each time NEXT is called.
	 * 
	 */
	public void next() {
		this.count=0;
		clearBuffers();
		try {
			if(hasNext()) {
				if(this.getAllData) {
					getAllData();
				}
				else {
					doMatlabQuery(this.rowsQuery, this.colsQuery);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

    private static class ParsedInfo {
        private String delimiter;
        private String[] content;

        ParsedInfo(String delimiter, String raw_content){
            this.delimiter = delimiter;
            this.content = raw_content.split(delimiter);
        }

        public String getDelimiter() {
            return delimiter;
        }

        public void setDelimiter(String delimiter) {
            this.delimiter = delimiter;
        }

        public String[] getContent() {
            return content;
        }
    }

	public HashMap<String, String> assocColumnWithRow(String rows, String cols) {

		ParsedInfo rowInfo = this.processParam(rows);
		ParsedInfo columnInfo = this.processParam(cols);
		String[] rowArray = rowInfo.getContent();
		String[] columnArray = columnInfo.getContent();

		HashMap<String, String> resultMap = new HashMap<>();
		for (int i = 0; i < rowArray.length; i++) {
			resultMap.put(rowArray[i], columnArray[i]);
		}
		return resultMap;
	}

	/**
	 *  Parse queryString
     *  @param queryString a valid D4M query string
	 **/
	private List<String> loadMap(String queryString) {
		ParsedInfo parsedInfo = this.processParam(queryString);
		String[] contentArray = parsedInfo.getContent();
		return Arrays.asList(contentArray);
	}

	public boolean isRangeQuery(String[] paramContent) {
		boolean rangeQuery = false;
		/*
		 * Range Queries are the following 'a,:,b,',  'a,:,end,',  ',:,b,'.
		 *  Note: Negative infinity Range a*,
		 */

		if (paramContent.length == 1 && paramContent[0].contains("*")) {
			rangeQuery = true;
		}
		if (paramContent.length == 3 && paramContent[1].contains(":")) {
			rangeQuery = true;
		}
		return rangeQuery;
	}

	public String getRangeQueryType(String[] paramContent) {
		/*
		 * Range Queries are the following 'a,:,b,',  'a,:,end,',  ',:,b,'. 
		 * Note: Negative Infinity Range a*,
		 */
		String rangeQueryType = "";
		if (paramContent[0].contains("*")) {
			rangeQueryType = D4mDbQueryAccumulo.REGEX_RANGE;
		}
		if (paramContent.length == 3) {
			if (paramContent[1].contains(":")) {
				rangeQueryType = D4mDbQueryAccumulo.KEY_RANGE;
			}
			if (paramContent[1].contains(":") && paramContent[2].toLowerCase().contains("end")) {
				rangeQueryType = D4mDbQueryAccumulo.POSITIVE_INFINITY_RANGE;
			}
			if (paramContent[1].contains(":") && paramContent[0].equals("")) {
				rangeQueryType = D4mDbQueryAccumulo.NEGATIVE_INFINITY_RANGE;
			}
		}
		return rangeQueryType;
	}


	public D4mDbResultSet doMatlabQuery(String rows, String cols, String family, String authorizations) throws D4mException {
		//throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		this.family = super.columnFamily = family;
		super.setSecurity(authorizations);
		clearBuffers();
		reset();
		this.rowsQuery = rows;
		this.colsQuery = cols;
		try {
			this.testResultSet = doMatlabQuery(rows, cols);
		} catch (AccumuloSecurityException|TableNotFoundException|AccumuloException e) {
			e.printStackTrace();
			throw new D4mException(e);
		} catch (NullPointerException e) {
			throw new D4mException("NULLPOINTER =  rows ="+rows+", cols="+cols+"\n"+e.toString());
        }

		return this.testResultSet;
	}

	private D4mDbResultSet doMatlabQuery(String rows, String cols) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

		if (!rows.equals(":") && cols.equals(":")) {

			ParsedInfo rowInfo = this.processParam(rows);
			this.rowInfo = rowInfo;
			String[] paramContent = rowInfo.getContent();
			// System.out.println("this.isRangeQuery(paramContent)="+this.isRangeQuery(paramContent));
			if (this.isRangeQuery(paramContent)) {
				log.debug("MATLAB_RANGE_QUERY_ON_ROWS");
				return this.doMatlabRangeQueryOnRows(rows, cols);
			}
			else {
				log.debug("MATLAB_QUERY_ON_ROWS");
				return this.doMatlabQueryOnRows(rows, cols);
			}
		}
		if (rows.equals(":") && !cols.equals(":")) {
			log.debug("MATLAB_QUERY_ON_COLS");
			return this.doMatlabQueryOnColumns(rows, cols);
		}
		if (rows.equals(":") && cols.equals(":")) {
			log.debug("GET_ALL_DATA");
			return this.getAllData();
		}
		if( !rows.startsWith(":") && !rows.equals(":") && !cols.startsWith(":") && !cols.equals(":")) {
			log.debug("SEARCH_BY_ROW_&_COL");
			return this.searchByRowAndColumn(rows, cols, null,null);
		}

		//String [] rowsArray = null;
		log.debug("==== AssocColumnWithRow ====");
		if(this.rowStringMap == null) {
			this.rowStringMap = this.assocColumnWithRow(rows, cols);
			this.rowInfo = processParam(rows);
			String [] rowsArray = this.rowInfo.getContent();
			this.rowKeys = param2keys(rowsArray);
		}
		Map<String, String> rowMap = this.rowStringMap;

		D4mDbResultSet results = new D4mDbResultSet();
		long start = System.currentTimeMillis();

		if(this.scannerIter == null) {
			HashSet<Range> ranges = this.loadRanges(this.rowKeys);
			BatchScanner scanner =  getBatchScanner(); // connection.getBatchScanner(this.tableName, this.numberOfThreads);
			scanner.fetchColumnFamily(new Text(this.family));
			scanner.setRanges(ranges);
			scannerIter = scanner.iterator();
		}
		Entry<Key, Value> entry = null;
		String rowKey;

		while ((this.count < this.limit)||(this.hasNext=scannerIter.hasNext())) {
			entry = scannerIter.next();
			rowKey = entry.getKey().getRow().toString();
			String column = entry.getKey().getColumnQualifier().toString();//new String(entry.getKey().getColumnQualifier().toString());
			String value = new String(entry.getValue().get(), StandardCharsets.UTF_8);
			String finalColumn = column.replace(this.family, "");

			if (rowMap.containsKey(rowKey) && rowMap.containsValue(finalColumn)) {

				if(this.buildReturnString(entry.getKey(),rowKey, finalColumn, value)) {
					break;
				}
			}
		}

		//Set the Key to start the next search
		{
			this.rowKeys.add(0,entry.getKey());
		}

		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);
		return results;
	}

	public D4mDbResultSet doMatlabQueryOnRows(String rows, String cols, String family, String authorizations) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		super.columnFamily = family;
		this.family = super.columnFamily;
		connProps.setAuthorizations(authorizations.split(","));
                reset();
		return doMatlabQueryOnRows(rows, cols);
	}

	/*
	 *  use scanner to get data
	 */
	private D4mDbResultSet doMatlabQueryOnRows(String rows, String cols) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		boolean useBatch= true;
		if(useBatch) {
			return doBatchMatlabQueryOnRows(rows, cols);
		}
		else {
			//			return doScanMatlabQueryOnRows(rows,cols);
		}
		return null;
	}

	private D4mDbResultSet doBatchMatlabQueryOnRows(String rows, String cols) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

		List<String> rowMap = loadMap(rows);

		if( this.rowInfo == null) {
			this.rowInfo = processParam(rows);
			String [] rowsArray = this.rowInfo.getContent();
			this.rowKeys = param2keys(rowsArray);
		}
		else {
			if(this.rowKeys == null) {
				String [] rowsArray = this.rowInfo.getContent();
				this.rowKeys = param2keys(rowsArray);
			}
		}
		D4mDbResultSet results = new D4mDbResultSet();

		BatchScanner scanner =null;
		if(this.bscanner == null) {
			HashSet<Range> ranges = this.loadRanges(this.rowKeys);
			scanner = getBatchScanner();
			scanner.setRanges(ranges);
			scanner.fetchColumnFamily(new Text(this.family));

		}
		long start = System.currentTimeMillis();

		if(this.scannerIter == null) {
			this.scannerIter = scanner.iterator();
		}
		iterateMatlabQueryOnRows(scannerIter, rowMap);

		if(!this.hasNext) {
			close();
		}
		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);
		this.testResultSet = results;
		return results;
	}

	private Entry<Key, Value>  iterateMatlabQueryOnRows (Iterator<Entry<Key, Value>> scannerIter, List<String> rowMap) {
		String rowKey;
		Entry<Key, Value> entry = null;
		while (this.hasNext =scannerIter.hasNext()) {
			entry = scannerIter.next();
			rowKey = entry.getKey().getRow().toString();

			if (rowMap.contains(rowKey)) {
				String value = new String(entry.getValue().get(), StandardCharsets.UTF_8);
				if(this.buildReturnString(entry.getKey(),rowKey, entry.getKey().getColumnQualifier().toString(), value)) {
					break;
				}
			}

		}
		return entry;
	}
	private void setNewRowKeyInMap(String rowKey, ParsedInfo info) {
		String[] paramContent = info.getContent();
		paramContent[0] = rowKey;
	}
	public D4mDbResultSet doMatlabRangeQueryOnRows(String rows, String cols, String family, String authorizations) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        super.columnFamily = family;
		this.family = super.columnFamily;
		connProps.setAuthorizations(authorizations.split(","));
		return doMatlabRangeQueryOnRows(rows, cols);
	}

	private D4mDbResultSet doMatlabRangeQueryOnRows(String rows, String cols) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		log.debug("%%%% doMatlabRangeQueryOnRows %%%%");
		ParsedInfo rowInfo;

		if(this.rowInfo == null) {
			rowInfo =this.processParam(rows);
			this.rowInfo = rowInfo;
		}
		else {
			rowInfo = this.rowInfo;
		}
		String[] rowArray = rowInfo.getContent();

		Set<Range> ranges = new HashSet<>();
		if(!this.hasNext) {

			if(this.bscanner == null)
				this.bscanner = getBatchScanner();

			if (this.getRangeQueryType(rowArray).equals(D4mDbQueryAccumulo.KEY_RANGE)) {
				// System.out.println("queryType="+this.KEY_RANGE+
				// " rowArray[0]="+rowArray[0]+" rowArray[2]="+rowArray[2]+"<");
				Key startKey = new Key(new Text(rowArray[0]));
				Key endKey = new Key(new Text(rowArray[2]));
				//Range range = new Range(startKey, true, endKey.followingKey(1), false);
				Range range = new Range(startKey, true, endKey.followingKey(PartialKey.ROW), false);
				ranges.add(range);
				bscanner.setRanges(ranges);
				// Note; there is a bug in Accumulo 1.1 for ranges including end key,
				// use "endKey.followingKey(1), false" work around
			}

			if (this.getRangeQueryType(rowArray).equals(D4mDbQueryAccumulo.POSITIVE_INFINITY_RANGE)) {
				// System.out.println("queryType="+this.POSITIVE_INFINITY_RANGE+
				// " rowArray[0]="+rowArray[0]);
				Key startKey = new Key(new Text(rowArray[0]));
				Range range = new Range(startKey, true, null, true);
				ranges.add(range);
				bscanner.setRanges(ranges);
			}

			if (this.getRangeQueryType(rowArray).equals(D4mDbQueryAccumulo.NEGATIVE_INFINITY_RANGE)) {
				// System.out.println("queryType="+this.NEGATIVE_INFINITY_RANGE+
				// " rowArray[0]="+rowArray[0]);
				Key endKey = new Key(new Text(rowArray[2]));
				//Range range = new Range(null, true, endKey.followingKey(1), false);
				Range range = new Range(null, true, endKey.followingKey(PartialKey.ROW), false);
				ranges.add(range);
				bscanner.setRanges(ranges);
				// Note; there is a bug in Accumulo 1.1 for ranges including end key,
				// use "endKey.followingKey(1), false" work around
			}

			if (this.getRangeQueryType(rowArray).equals(D4mDbQueryAccumulo.REGEX_RANGE)) {
				// System.out.println("queryType="+this.REGEX_RANGE+
				// " rowArray[0]="+rowArray[0]);
				String regexParams = this.regexMapper(rowArray[0]);
                                int priority = 25;
                                IteratorSetting regex = new IteratorSetting(priority, "regex", RegExFilter.class);
                                RegExFilter.setRegexs(regex,regexParams ,null,null,null, false);
				bscanner.addScanIterator(regex);
				//bscanner.setRowRegex(regexParams);
				Range range = new Range();
				ranges.add(range);
				bscanner.setRanges(ranges);
			}
		}

		D4mDbResultSet results = new D4mDbResultSet();
		//ArrayList<D4mDbRow> rowList = new ArrayList<D4mDbRow>();
		long start = System.currentTimeMillis();

		if(this.scannerIter == null) {
			this.scannerIter = bscanner.iterator();
		}
		Entry<Key, Value> entry = iterateOverEntries(this.scannerIter);

		//Set the new row key to start next search
		if(entry != null) {
			setNewRowKeyInMap(entry.getKey().getRow().toString(), this.rowInfo);
		}

		// *********************************************
		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);
		this.testResultSet = results;
		return results;
	}

	private D4mDbResultSet doMatlabQueryOnColumns(String rows, String cols) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		log.debug(" <<<< doMatlabQueryOnColumns >>>> ");
		ParsedInfo objColInfo = this.processParam(cols);


		String [] colArray = objColInfo.getContent();
		compareUtil = new CompareUtil(colArray);

		log.debug("LENGTH_OF_COL_ARRAY = "+colArray.length);
		D4mDbResultSet results = new D4mDbResultSet();
		long start = System.currentTimeMillis();

		if(this.scannerIter == null) {

			//Scanner scanner = null;
			//	if(this.scanner == null) {
			scanner = getScanner();//connection.getScanner(tableName);
			//if( this.startKey != null)
			//{
			//Set the range to start search
			//	this.startRange = new Range(this.startKey,null);
			//	scanner.setRange(startRange);
			//} else {
			this.startRange =  new Range();//new Range(this.startKey,null);
			scanner.setRange(startRange);
			//}
			String regexName="D4mRegEx";
			try {
				//Accum-1.4  Use IteratorSetting
				IteratorSetting itSet  = new IteratorSetting(1, regexName, RegExFilter.class);
				//IteratorSetting.addOption(option, value)
				itSet.addOption(RegExFilter.COLQ_REGEX, compareUtil.getPattern().pattern());
				scanner.addScanIterator(itSet);

				//this.scanner.setScanIterators(1, RegExIterator.class.getName(), regexName);
				//this.scanner.setScanIteratorOption(regexName, RegExFilter.COLQ_REGEX, compareUtil.getPattern().pattern());

			} catch (RuntimeException e) {
				log.warn("Bad REGEX ="+ compareUtil.getPattern().pattern(),e);
				e.printStackTrace();
			}

			scanner.fetchColumnFamily(new Text(this.family));
			//	}



			this.scannerIter = scanner.iterator();
		}

		/*
		Entry<Key, Value> entry =null;
		while (  (this.hasNext =scannerIter.hasNext())) {

			entry = (Entry<Key, Value>) scannerIter.next();
			String rowKey = entry.getKey().getRow().toString();
			//String column = new String(entry.getKey().getColumnQualifier().toString());
			String finalColumn =  entry.getKey().getColumnQualifier().toString();  //column.replace(this.family, "");
			String value=null;
			boolean isGood=false;
			isGood = compareUtil.compareIt(finalColumn.replace(this.family, ""));
			if(isGood) {
				value = new String(entry.getValue().get());
				if(this.buildReturnString(entry.getKey(),rowKey, finalColumn, value)) {
					break;
				}
			}

		}
		 */
		iterateOverEntries(this.scannerIter, this.compareUtil);
		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);
		this.testResultSet = results;
		return results;
	}

	private Scanner getScanner() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		if(this.connection == null)
		this.connection = new AccumuloConnection(this.connProps);
		if(this.scanner == null)
			this.scanner = connection.createScanner(tableName);
		return scanner;
	}
	private BatchScanner getBatchScanner() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		if(this.connection == null)
		connection = new AccumuloConnection(this.connProps);
		if(this.bscanner == null)
			this.bscanner = connection.getBatchScanner(this.tableName, this.numberOfThreads);
		return this.bscanner;
	}

	public void close() {
		this.scannerIter = null;

		if(this.bscanner != null) {
			this.bscanner.close();
			this.bscanner = null;
		}
		if(this.scanner != null) {
			this.scanner = null;
		}
	}
	/*
	 *  Search by both row and column
	 *  
	 *  row   set the range
	 *  col   set the column qualifier
	 *  
	 *  Get batchscanner
	 */
	public D4mDbResultSet searchByRowAndColumn(String rows, String cols, String family, String authorizations)  {
		//boolean useBatchSearch=false;
		clearBuffers();
		D4mDbResultSet results;
//		HashMap<String, Object> rowInfo = null;
		if( this.rowInfo == null) {
			this.rowInfo = this.processParam(rows);
//			rowInfo = this.rowInfo;
		}
//		else
//			rowInfo = this.rowInfo;
		ParsedInfo columnInfo;
		if(this.colInfo == null) {
			columnInfo =this.processParam(cols);
			this.colInfo = columnInfo;
		} 
		//else
			//columnMap = this.colInfo;

		results = searchByRowAndColumn(this.rowInfo, colInfo);

		return results;
	}

	private D4mDbResultSet searchByRowAndColumn (ParsedInfo rowInfo,ParsedInfo columnInfo) {
		D4mDbResultSet results = new D4mDbResultSet();
		HashSet<Range> ranges;
		Range range;
		String[] rowArray = rowInfo.getContent();
		String[] columnArray = columnInfo.getContent();
		int length=rowArray.length;
		String rowkey1;
		String rowkey2;
		boolean rowArrayGood=false;

		long start = System.currentTimeMillis();
		if(length == 1) {
			rowkey1 = rowArray[0];
			rowkey2 = rowArray[0];
			range = new Range(rowkey1,true,rowkey2, true);
			SearchIt(range,columnArray);
			rowArrayGood=true;
		} else if(length == 2 & !rowArray[length-1].equals(":")) {
			// Ex, rowQuery='a,c,'
			// Query for rows with 'a' and 'c'
			// Then, we should do use BatchScanner
			// Set up HashSet<Range> ranges
			//HashMap<String, String> loadRowMap(HashMap<String, Object> rowInfo)
			//HashMap<String,String> rowMapString = loadRowMap(rowInfo);
			ranges = loadRanges(rowArray);
			SearchIt(ranges,columnArray);

			rowArrayGood = true;

		} else 	if(length == 3) {

			if(rowArray[1].equals(":")) {
				rowkey1 = rowArray[0];
				rowkey2 = rowArray[length-1];
				if (log.isDebugEnabled())
					log.debug("3__RANGE__"+rowkey1+","+rowkey2);
				range = new Range(rowkey1,true,rowkey2, true);
				SearchIt(range,columnArray);
				rowArrayGood = true;

			} 
			else {//else if( rowArray[(length-1)].equals(":")) {
				//rowkey1 = rowArray[0];
				//rowkey2 = rowArray[1];
				ranges = loadRanges(rowArray);
				SearchIt(ranges,columnArray);
				rowArrayGood = true;
			}
		}
		if(!rowArrayGood || length > 3) {
			System.out.println("Currently, we cannot handle more than 3 arguments for row key and column key");
			System.out.println("The row and column queries must be in any of the following forms:");
			System.out.println("  *  rowkey (colkey) = \"a,\" ");
			System.out.println("  *  rowkey (colkey) = \"a,b,\"");
			System.out.println("  *  rowkey (colkey) = \"a,:,h,\"");
			System.out.println("  *  rowkey (colkey) = \":\"");

			return results;
		}

		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);
		this.testResultSet = results;
		return results;
	}

	private void SearchIt(Range range, String [] columnArray) {
		String colRegex = RegExpUtil.makeRegex(columnArray);
		if(compareUtil == null) {
			this.compareUtil = new CompareUtil(columnArray);
		}
		if(columnArray.length == 3 && columnArray[1].equals(":")) {
//			String colStart1 = columnArray[0].substring(0, 1);
//			String colEnd1 = columnArray[2].substring(0,1);
			//colRegex = RegExpUtil.makeRegex(columnArray);//"^["+colStart1+"-"+colEnd1+"].*";
			log.debug("COLUMN REGEX="+colRegex);
			SearchIt(range,colRegex);
			//SearchIt(range,this.compareUtil);
		} else {

			if(this.scannerIter == null)
				this.scannerIter = getIteratorFromScanner(range);
			Iterator<Entry<Key,Value>> iter = this.scannerIter;
			Entry<Key, Value> entry;
			Pattern pat = Pattern.compile(colRegex);
			while(this.hasNext=iter.hasNext()) {
				entry = iter.next();
				Key colkey = entry.getKey();

				//	for(String colKey : columnArray) {
				//	if(!colKey.equals(":")) {
				String col = colkey.getColumnQualifier().toString();
				Matcher match = pat.matcher(col);
				//	boolean isMatching = Pattern.matches(colKey, col);
				if(match.matches() && buildReturnString(colkey, entry.getValue())) {
					break;
				}
				//	SearchIt(range,colKey);

				//					}
				//}
			}
		}

		if(!this.hasNext) {
			close();
		}
	}

	private Iterator <Entry<Key, Value>> getIteratorFromScanner(Range range) {
		if(this.scannerIter != null)
			return this.scannerIter;
		try {
			this.scanner = getScanner();
			this.scanner.setRange(range);
			//Setup regular expression
			//			String regexName="D4mRegEx";
			//			this.scanner.setScanIterators(1, RegExIterator.class.getName(), regexName);
			//			this.scanner.setScanIteratorOption(regexName, RegExFilter.COLQ_REGEX, colRegex);
			//scanner.setColumnQualifierRegex(colRegex);
			scanner.fetchColumnFamily(new Text(this.family));

		} catch (Exception e) {
			e.printStackTrace();
		}
		return this.scanner.iterator();
	}
	private void SearchIt(Range range, String colRegex) {
		if(this.scanner == null) {
			try {
				this.scanner = getScanner();
				this.scanner.setRange(range);
				//Setup regular expression
				//String regexName="D4mRegEx";
				//this.scanner.setScanIterators(1, RegExIterator.class.getName(), regexName);
				//this.scanner.setScanIteratorOption(regexName, RegExFilter.COLQ_REGEX, colRegex);
				//scanner.setColumnQualifierRegex(colRegex);
				scanner.fetchColumnFamily(new Text(this.family));

			} catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
				e.printStackTrace();
			}
			//catch (IOException e) {
			//	e.printStackTrace();
			//}
		}

		//setup query, set range
		if(this.scannerIter == null) {
			this.scannerIter = this.scanner.iterator();
		}

		iterateOverEntries(this.scannerIter, this.compareUtil);
	}
	private  Entry<Key, Value> iterateOverEntries(Iterator<Entry<Key, Value>> scannerIter, CompareUtil compUtil) {
		Entry<Key, Value> entry =null;
		String rowKey;
		int count=0;
		while (this.hasNext =scannerIter.hasNext()) {
			if(this.limit == 0 || this.count < this.limit) {
				entry = scannerIter.next();
				this.startKey = entry.getKey();
				rowKey = entry.getKey().getRow().toString();
				String column = entry.getKey().getColumnQualifier().toString();
				//System.out.println(count+"BEFORE_ENTRY="+rowKey+","+column);
				//String value = new String(entry.getValue().get());
				boolean isGood = compUtil.compareIt(column.replace(this.family, ""));
				if(isGood) {
					String value = new String(entry.getValue().get(), StandardCharsets.UTF_8);
					if(this.buildReturnString(entry.getKey(),rowKey, column, value)) {
						break;
					}

					count++;
				}
			}
			else {
				break;
			}
		}
		log.debug("Number of entries = "+count);
		return entry;
	}

	private  Entry<Key, Value> iterateOverEntries(Iterator<Entry<Key, Value>> scannerIter) {
		Entry<Key, Value> entry =null;
		String rowKey;
		int count=0;
		while (this.hasNext =scannerIter.hasNext()) {
			if(this.limit == 0 || this.count < this.limit) {
				entry = scannerIter.next();
				startKey = entry.getKey();
				rowKey = startKey.getRow().toString();
				String column = startKey.getColumnQualifier().toString();
				//System.out.println(count+"BEFORE_ENTRY="+rowKey+","+column);
				String value = new String(entry.getValue().get(), StandardCharsets.UTF_8);
				if(this.buildReturnString(startKey,rowKey, column, value)) {
					break;
				}
				count++;
			}
			else {
				break;
			}
		}
		log.debug("Number of entries = "+count);
		return entry;
	}

	private void SearchIt(HashSet<Range> ranges, String [] columnArray) {
		//String colRegex = "";
		if(compareUtil == null) {
			this.compareUtil = new CompareUtil(columnArray);
		}

		if(columnArray.length == 3 && columnArray[1].equals(":")) {
			//String colStart1 = columnArray[0].substring(0, 1);
			//String colEnd1 = columnArray[2].substring(0,1);
			//colRegex = RegExpUtil.makeRegex(columnArray);//"["+colStart1+"-"+colEnd1+"].*";
			SearchIt(ranges,this.compareUtil);
		} else {
			//for(String colKey : columnArray) {
			String colregex = RegExpUtil.makeRegex(columnArray);
			Pattern colpat = Pattern.compile(colregex);
			SearchIt(ranges,colpat);
			//}
		}

	}

	private void SearchIt(HashSet<Range> ranges, CompareUtil compUtil) {
		if(this.scannerIter == null) {
			try {
				this.bscanner = getBatchScanner();
				this.bscanner.setRanges(ranges);
				bscanner.fetchColumnFamily(new Text(this.family));

				this.scannerIter = bscanner.iterator();
			} catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
				e.printStackTrace();
			}

		}
		iterateOverEntries(this.scannerIter, compUtil);


	}

	private void SearchIt(HashSet<Range> ranges, Pattern col) {
		//BatchScanner scanner = null;
		Entry<Key, Value> entry =null;
		String rowKey = null;
		try {
			if(this.scannerIter == null) {
				if( this.bscanner == null || !this.hasNext) {
					bscanner = getBatchScanner();
					bscanner.setRanges(ranges);

					//loop over columns
					//reset column by scanner.clearColumns()s
					//set new columns fetchColumn(fam,col)
					//iterate
					//fill output buffer
					//scanner.setColumnQualifierRegex(colRegex);
					String regexName="D4mRegEx";
					IteratorSetting itSet  = new IteratorSetting(1, regexName, RegExFilter.class);
					//IteratorSetting.addOption(option, value)
					itSet.addOption(RegExFilter.COLQ_REGEX, col.pattern());
					bscanner.addScanIterator(itSet);

					//bscanner.setScanIterators(1, RegExIterator.class.getName(), regexName);
					//bscanner.setScanIteratorOption(regexName, RegExFilter.COLQ_REGEX, col.pattern());

					bscanner.fetchColumnFamily(new Text(this.family));
				}
				this.scannerIter = bscanner.iterator();
			}


			//int count=0;

			//		    iterateOverEntries(this.scannerIter,col);
			iterateOverEntries(this.scannerIter);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			setNewRowKeyInMap(rowKey, this.rowInfo);
			if(!this.hasNext)
				close();
		}

	}

	private void saveTestResults(String rowKey, String columnFamily, String finalColumn, String value) {
		D4mDbRow row = new D4mDbRow();
		row.setRow(rowKey);
		row.setColumnFamily(columnFamily);
		row.setColumn(finalColumn);
		row.setValue(value);
		this.rowList.add(row);
	}
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

		if (args.length < 5) {
			System.out.println("Usage: D4mAccumuloQuery host table rows cols");
			return;
		}

		String hostName = args[0];
		String tableName = args[1];
//		String rows = args[2];
//		String cols = args[3];
		int limit = Integer.parseInt(args[4]);
		D4mDbQueryAccumulo tool = new D4mDbQueryAccumulo("org.apache.accumulo", hostName, tableName, "root", "ALL4114ALL");
		tool.setLimit(limit);
		tool.doTest = false;

		//System.out.println("RowReturnString=" + tool.getRowReturnString());
		//System.out.println("ColumnReturnString=" + tool.getColumnReturnString());
		//System.out.println("ValueReturnString=" + tool.getValueReturnString());
		//System.out.println("\n\n########################\n");
		//testSearchByRowAndCol();
	}


	public static void test1(D4mDbQueryAccumulo tool, String rows, String cols) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		D4mDbResultSet resultSet = tool.doMatlabQuery(rows, cols);
		double totalQueryTime = resultSet.getQueryTime();
		System.out.println("totalQueryTime = " + totalQueryTime);
		List<?> rowsArr = resultSet.getMatlabDbRow();

		Iterator<?> it = rowsArr.iterator();
		System.out.println("");
		System.out.println("");

		int rowsToPrint = 20 + 1;
		int counter = 0;
		while (it.hasNext()) {
			counter++;
			D4mDbRow row = (D4mDbRow) it.next();
			String rowNumber = row.getRow();
			String column = row.getColumn();
			String value = row.getValue();
			if (counter < rowsToPrint) {
				System.out.println("Row; " + rowNumber);
				System.out.println("Column; " + column);
				System.out.println("Value; " + value);
				System.out.println("");
				System.out.println("");
			}
		}

	}
	public static void testSearchByRowAndCol() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		String rowkeys="a b c ";
		String cols ="a b bb";
		String family="";
		String authorizations="";

		String instanceName="org.apache.accumulo";
		String host="f-2-10.llgrid.ll.mit.edu";
		String table="SearchRowAndColTEST";
		String username="root"; 
		String password="ALL4114ALL";

		D4mDbQueryAccumulo query = new D4mDbQueryAccumulo( instanceName,  host, table, username, password);
		query.searchByRowAndColumn(rowkeys, cols, family, authorizations);
		System.out.println("####RowReturnString=" + query.getRowReturnString());
		System.out.println("####ColumnReturnString=" + query.getColumnReturnString());
		System.out.println("####ValueReturnString=" + query.getValueReturnString());
	}

	//	public int getLimit() {
	//		return this.limit;
	//	}
	//
	//	public void setLimit(int limit) {
	//		this.limit = limit;
	//	}

	private ParsedInfo processParam(final String param) {
		String content = param.substring(0, param.length() - 1);
		String delimiter = param.substring(param.length()-1);
        //FIXME: Previously the delimiter was stored before prefixing
        //with slashes.  I'm not doing that now, but it doesn't seem important
        //since the delimited is not used later.
		//map.put("delimiter", delimiter);
		if (delimiter.equals("|")) {
			delimiter = "\\" + delimiter;
		}
		return new ParsedInfo(delimiter, content);
	}

	private ArrayList<Key> param2keys(String [] params) {
		ArrayList<Key> keys = new ArrayList<>();
		for(String s : params) {
			if(s.equals(":")) continue;
			Key k = new Key(s);
			keys.add(k);
		}
		return keys;
	}

	public String getColumnReturnString() {
		return this.columnReturnString;
	}

	public void setColumnReturnString(String columnReturnString) {
		this.columnReturnString = columnReturnString;
	}

	public String getRowReturnString() {
		return this.rowReturnString;
	}

	public void setRowReturnString(String rowReturnString) {
		this.rowReturnString = rowReturnString;
	}

	public String getValueReturnString() {
		return this.valueReturnString;
	}

	public void setValueReturnString(String valueReturnString) {
		this.valueReturnString = valueReturnString;
	}

	public void setAuthorizations(String[] authorizations) {
		this.connProps.setAuthorizations(authorizations);
	}

	public HashSet<Range> loadRanges(HashMap<String, String> queryMap) {
		HashSet<Range> ranges = new HashSet<>();
		for (String rowId : queryMap.keySet()) {
			//System.out.println("==>>ROW_ID="+rowId+"<<++");
			if (rowId != null) {
				Key key = new Key(new Text(rowId));
				//Range range = new Range(key, true, key.followingKey(1), false);
				Range range = new Range(key, true, key.followingKey(PartialKey.ROW), false);

				ranges.add(range);
			}
		}
		return ranges;
	}
	public HashSet<Range> loadRanges(String [] rowsRange) {
		HashSet<Range> ranges = new HashSet<>();
		//Iterator<String> it = rangeQuery.iterator();
		//	System.out.println("<<< ROW_ARRAY_LENGTH="+rowsRange.length+" >>>");
		int len = rowsRange.length;
		//for (int i = 0; i < len; i++) {
		for (String rowId :rowsRange ) {
			if(rowId != null && !rowId.equals(":")) {
				Key key = new Key(new Text(rowId));
				//Range range = new Range(key, true, key.followingKey(1), false);
				Range range = new Range(key, true, key.followingKey(PartialKey.ROW), false);
				ranges.add(range);
			}
		}
		return ranges;
	}

	public HashSet<Range> loadRanges(ArrayList<Key> rowsRange) {
		HashSet<Range> ranges = new HashSet<>();
		for(Key key : rowsRange) {
			Range range = new Range(key, true, key.followingKey(PartialKey.ROW), false);
			ranges.add(range);
		}
		return ranges;

	}

	private String regexMapper(String regex) {

		String charStr = regex.replace("*", "");
		return "^" + charStr + "*|^" + charStr + ".";
	}
	public boolean isStartRowInclusive() {
		return startRowInclusive;
	}
	public void setStartRowInclusive(boolean startRowInclusive) {
		this.startRowInclusive = startRowInclusive;
	}
	public void setStartRowInclusive(Boolean startRowInclusive) {
		this.startRowInclusive = startRowInclusive;
	}

	public boolean isEndRowInclusive() {
		return endRowInclusive;
	}
	public void setEndRowInclusive(boolean endRowInclusive) {
		this.endRowInclusive = endRowInclusive;
	}
	public void setEndRowInclusive(Boolean endRowInclusive) {
		this.endRowInclusive = endRowInclusive;
	}

	public boolean isPositiveInfinity() {
		return positiveInfinity;
	}
	public void setPositiveInfinity(boolean positiveInfinity) {
		this.positiveInfinity = positiveInfinity;
	}

	public void clearBuffers() {
		this.count = 0;
		int len = this.sbRowReturn.length();
		if(len > 0)
			this.sbRowReturn = this.sbRowReturn.delete(0, len);
		len = this.sbColumnReturn.length();
		if(len > 0)
			this.sbColumnReturn = this.sbColumnReturn.delete(0, len);
		len = this.sbValueReturn.length();
		if(len > 0)
			this.sbValueReturn = this.sbValueReturn.delete(0, len);
		this.columnReturnString = "";
		this.valueReturnString = "";
		this.rowReturnString = "";
	}
	public boolean isDoAllRanges() {
		return doAllRanges;
	}
	public void setDoAllRanges(boolean doAllRanges) {
		this.doAllRanges = doAllRanges;
	}
	/*
	 * Get the cumulative total of results retrieved.
	 */
	public long getCumCount() {
		return this.cumCount;
	}
	@Override
	public void reset() {
		this.startRange = null;
		this.startKey   = null;
		this.rowsQuery = null;
		this.colsQuery = null;
		this.colInfo = null;
		this.rowInfo = null;
		this.rowStringMap = null;
		this.hasNext=false;
		this.getAllData = false;
		this.count = 0;
		this.cumCount=0;
		this.rangesList.clear();
		this.rowKeys = null;
		this.compareUtil = null;
		try {
			//Close the BatchScanner
			close();
		} catch(Exception ignored) {

		} finally {
			this.bscanner = null;
		}

		this.scanner = null;
		this.scannerIter = null;

		clearBuffers();
                if(this.connection != null)
                   this.connection.setAuthorizations(connProps);

	}
	public String getFamily() {
		return super.columnFamily;
	}
	public void setFamily(String family) {
		super.columnFamily = family;
	}
	public int getNumRows() {
		return numRows;
	}
	public void setNumRows(int numRows) {
		this.numRows = numRows;
	}

	public void setHasNext(boolean hasNext) {
		this.hasNext = hasNext;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	@Override
	public D4mDataObj getResults() {
		D4mDataObj data = new D4mDataObj();
		data.setRow(this.rowReturnString);
		data.setColQualifier(this.columnReturnString);
		data.setValue(this.valueReturnString);

		data.setRowList(rowList);
		return data;
	}
}
/*
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % D4M: Dynamic Distributed Dimensional Data Model 
 * % MIT Lincoln Laboratory
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % (c) <2010>  Massachusetts Institute of Technology
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */

