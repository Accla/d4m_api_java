package edu.mit.ll.d4m.db.cloud;

import edu.mit.ll.d4m.db.cloud.accumulo.AccumuloInsert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.mit.ll.cloud.connection.ConnectionProperties;


/**
 * @author William Smith
 */

public class D4mDbInsert extends D4mParent {

	private static final Logger log = LoggerFactory.getLogger(D4mDbInsert.class);

	// used to control special behavior in AccumuloInsert
	private static boolean IntEncodeValueAndDropEmpty = false; // set vals to empty; UIntegerLexicode
	@SuppressWarnings("unused")
	public static void setIntEncodeValueAndDropEmpty(boolean intEncodeValueAndDropEmpty) {
		IntEncodeValueAndDropEmpty = intEncodeValueAndDropEmpty;
	}
	@SuppressWarnings("unused")
	public static boolean isIntEncodeValueAndDropEmpty() {
		return IntEncodeValueAndDropEmpty;
	}
	private static boolean IntEncodeKeyAndPrependLastByteRev = false; // prepend shard byte to Row; set vals to empty; 4 bytes integer encode
	@SuppressWarnings("unused")
	public static void setIntEncodeKeyAndPrependLastByteRev(boolean intEncodeKeyAndPrependLastByteRev) {
		IntEncodeKeyAndPrependLastByteRev = intEncodeKeyAndPrependLastByteRev;
	}
	@SuppressWarnings("unused")
	public static boolean isIntEncodeKeyAndPrependLastByteRev() {
		return IntEncodeKeyAndPrependLastByteRev;
	}

	private String tableName = "";
	private String rows = "";
	private String cols = "";
	private String vals = "";
	private String family = "";
	private String visibility = "";
	static final boolean doTest = false;
	static final boolean printOutput = false;
	static final int maxMutationsToCache = 10000;
	private int numThreads = 50;

	private ConnectionProperties connProps = new ConnectionProperties();

	private D4mInsertBase d4mInserter=null;

	/**
	 * @param instanceName
	 * @param hostName
	 * @param tableName
	 * @param username
	 * @param password
	 */
	public D4mDbInsert(String instanceName, String hostName, String tableName, String username, String password)  {
		super();
		this.tableName = tableName;

		this.connProps.setHost(hostName);
		this.connProps.setInstanceName(instanceName);
		this.connProps.setUser(username);
		this.connProps.setPass(password);
		this.connProps.setMaxNumThreads(this.numThreads);
	}
	/**
	 * @param instanceName accumulo instance
	 * @param hostName    zookeeper host
	 * @param tableName   name of table to insert data into
	 * @param username    user name
	 * @param password    user's password
	 * @param numThreads   number of threads to use to write to table
	 */
	public D4mDbInsert(String instanceName, String hostName, String tableName, String username, String password, int numThreads) {
		super();
		this.tableName = tableName;

		this.connProps.setHost(hostName);
		this.connProps.setInstanceName(instanceName);
		this.connProps.setUser(username);
		this.connProps.setPass(password);
		this.numThreads= numThreads;
		this.connProps.setMaxNumThreads(this.numThreads);
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 5) {
			return;
		}

		String hostName = args[0];
		String tableName = args[1];
		String rows = args[2];
		String cols = args[3];
		String vals = args[4];
/*
		D4mDbInsert acc = new D4mDbInsert("accumulo", hostName, tableName, "root", "secret", rows, cols, vals);
		String visibility = "s|ts|sci";
		String family = "d4mFamily";
		String columnQualifier = "d4mFamilyValue";
		acc.doProcessing(visibility, family, columnQualifier);
		*/
	}


	public void doProcessing(String rows, String cols, String vals, String family, String visibility) throws Exception {

		this.rows = rows;
		this.cols = cols;
		this.vals = vals;
		this.family = family;
		this.visibility = visibility;
		if(this.d4mInserter == null) {
			this.d4mInserter = new AccumuloInsert();
			this.d4mInserter.setConnProps(connProps);
		}
		this.d4mInserter.setTableName(this.tableName);

		long start = System.currentTimeMillis();
		this.d4mInserter.doProcessing(rows, cols, vals, family, visibility);
		long end = System.currentTimeMillis();
		double elapsed = ((double)(end-start))/1000.0;
		log.info("INGEST time elapsed(sec) = "+elapsed);
		//System.out.println("INGEST time (sec) = "+elapsed);

		//
		//doProcessing();
	}

//	/** Create a weird map out of an Assoc string - never used */
//	private HashMap<String, Object> processParam(String param) {
//		HashMap<String, Object> map = new HashMap<String, Object>();
//		String content = param.substring(0, param.length() - 1);
//		String delimiter = param.substring(param.length()-1);  //.replace(content, ""); // DH: Just take the last character
//		map.put("delimiter", delimiter);
//		if (delimiter.equals("|")) {
//			delimiter = "\\" + delimiter;
//		}
//		map.put("content", content.split(delimiter));
//		map.put("length", content.length());
//		return map;
//	}

	/** calls to D4mDbInfo.getTableList() to retrieve the table list from the connector   */
	public boolean doesTableExistFromMetadata(String tableName) {
		boolean exist = false;
		D4mDbInfo info = new D4mDbInfo(this.connProps);
		String tableNames = "";
		try {
			tableNames = info.getTableList();		//
			if (tableNames.contains(tableName)) {
				exist = true;
			}

		}
		catch (Exception ex) {
			log.warn("",ex);
		}
		return exist;
	}

	public void doLoadTest() {
		int loops = 10;
		int capacity = loops;

		StringBuilder sb1 = new StringBuilder(capacity);
		StringBuilder sb2 = new StringBuilder(capacity);
		StringBuilder sb3 = new StringBuilder(capacity);

		System.out.println("Creating test data for " + loops + " entries.");
		for (int i = 1; i < loops + 1; i++) {
			sb1.append(i + " ");
			sb2.append(i + " ");
			sb3.append(i + " ");
		}

		this.rows = sb1.toString();
		this.cols = sb2.toString();
		this.vals = sb3.toString();
		System.out.println("Completed creation of test data for " + loops + " entries.");
	}


}
/*
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % D4M: Dynamic Distributed Dimensional Data Model 
 * % MIT Lincoln Laboratory
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % (c) <2010> Massachusetts Institute of Technology
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */
