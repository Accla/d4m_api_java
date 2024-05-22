/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.test;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.regex.Pattern;

import edu.mit.ll.cloud.connection.ConnectionProperties;

//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.mit.ll.d4m.db.cloud.D4mConfig;
import edu.mit.ll.d4m.db.cloud.D4mDbInsert;
import edu.mit.ll.d4m.db.cloud.accumulo.D4mDbQueryAccumulo;
import edu.mit.ll.d4m.db.cloud.D4mDbResultSet;
import edu.mit.ll.d4m.db.cloud.D4mDbRow;
import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations;
import edu.mit.ll.d4m.db.cloud.util.RegExpUtil;
import edu.mit.ll.d4m.db.cloud.util.D4mDataObj;

/**
 * @author cyee
 *
 */
public class D4mDbQueryTest7 {
	private static Logger log = LoggerFactory.getLogger(D4mDbQueryTest7.class);

	static String instanceName = "";
	static String host = "";
	static String username = "";
	static String password = "";
	static String table = "";
	static String columnFamily="";
	int i127=127;
	String ASCI_127 = Character.toString((char) i127);

	static boolean isReady = false;

	@AfterClass
	public static void tearDown() throws Exception {
		//Delete table
		D4mDbTableOperations dbTable = new D4mDbTableOperations(instanceName,host,username,password);

		dbTable.init(instanceName, host, username, password, "Accumulo");
	
		dbTable.deleteTable(table);

	}

	/**
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
        AccumuloTestConnection testConnection = new AccumuloTestConnection("local.conf");
        ConnectionProperties cp = testConnection.getConnectionProperties();
        instanceName               = cp.getInstanceName();
        host                       = cp.getHost();
        username                   = cp.getUser();
        password                   = cp.getPass();
		table                      = "iTest7";
		columnFamily        = "";

		/*
		 * 
		 * 
a :a []    a-a
a :aa []    a-aa
a :aaa []    a-aaa
a :b []    a-b
a :bb []    a-bb
a :bbb []    a-bbb
aa :a []    aa-a
aa :aa []    aa-aa
aaa :a []    aaa-a
aaa :aaa []    aaa-aaa
b :a []    b-a
b :b []    b-b
bb :a []    bb-a
bb :bb []    bb-bb
bbb :a []    bbb-a
bbb :bbb []    bbb-bbb

		 */
		if(!isReady) {

		String row = "a,a,a,a,a,a,aa,aa,aaa,aaa,b,b,bb,bb,bbb,bbb,";
		String col = "a,aa,aaa,b,bb,bbb,a,aa,a,aaa,a,b,a,bb,a,bbb,";
		String val = "a-a,a-aa,a-aaa,a-b,a-bb,a-bbb,aa-a,aa-aa,aaa-a,aaa-aaa,b-a,b-b,bb-a,bb-bb,bbb-a,bbb-bbb,";
		D4mDbInsert dbInsert = new D4mDbInsert(instanceName,host,table,username,password);
		
		dbInsert.doProcessing(row, col, val,"","");
		Thread.sleep(10000);
		isReady=true;
		}
	}

	/**
	 * @throws Exception
	 */

	/*
	 * Do  successive test queries 
	 * 1. row="aa,"  col=:
	 * 2. row="b,"  col=:
	 * 
	 */
	@Test
	public void testSuccessiveQueries() {
		String rows="aa,";
		String cols= ":";
		String authorizations="";
		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");
                D4mConfig.DEBUG=true;
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = false;


		try {
			//First query
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
                        D4mDataObj dataObj = d4m.getResults();  
			//if(d4m.TEST_ACCUMULO_PORT)
			d4m.next();

                        // aa,a,aa-a
			D4mDbQueryTest.print(d4m);

                        String [] rowReturns = dataObj.getRow().split("\\s+");
                        String [] colQualReturns = dataObj.getColQualifier().split("\\s+");
                        String [] valReturns = dataObj.getValue().split("\\s+");

                        System.out.println(" **row return = " + rowReturns[0]);
                        System.out.println(" **col return = " + colQualReturns[0]);
                        System.out.println(" **val return = " + valReturns[0]);
				Assert.assertTrue(rowReturns[0].equals("aa"));
			rows="b,";
			cols= ":";
			//Second query
			System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");

			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			d4m.next();
                        // aa, aa, aa-aa
			D4mDbQueryTest.print(d4m);

		} catch (Exception e) {
			e.printStackTrace();
		} 
		finally {
			d4m.close();
		}
		System.out.println("--------------------------------------");

	}

	@Test
	public void testB() {
		String rows="a,";
		String cols= "a,b,";
		String authorizations="";
		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");
                D4mConfig.DEBUG=true;

		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		try {
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			d4m.next();
			D4mDbQueryTest.print(d4m);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
		}
		System.out.println("--------------------------------------");

	}
	
	/*
	 *  row='a,b,'
	 *  col=:
	 *  
	 *  This should return 
	 *  row=a
	 *  row=b
	 */
	@Test
	public void testC() {
		String rows="a,b,";
		String cols= ":";
		String authorizations="";
                D4mConfig.DEBUG=true;
		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");

		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		try {
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			d4m.next();
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			HashMap<String,String> map = new HashMap<>();
			String [] rowArray = rows.split(",");
			map.put(rowArray[0], rowArray[0]);
			map.put(rowArray[1], rowArray[1]);
			
			for(D4mDbRow row : rowList) {
				String rowkey = row.getRow();
				Assert.assertTrue(map.containsKey(rowkey));
			}
			D4mDbQueryTest.print(d4m);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
		}
		System.out.println("--------------------------------------");

	}
	
	/*
	 *  Test if next() works with BatchScanner on doMatlabQeuryOnRow
	 */
	@Test
	public void testD() {
		String rows="a,b,";
		String cols= ":";
		String authorizations="";
		System.out.println("TestD::QUERY = ['"+ rows + "', '"+cols+"']");
                D4mConfig.DEBUG=true;

		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		int repeat=3;
		try {
			d4m.setLimit(2);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			
			for(int i=0; i < repeat; i++) {
				d4m.next();
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			HashMap<String,String> map = new HashMap<>();
			String [] rowArray = rows.split(",");
			map.put(rowArray[0], rowArray[0]);
			map.put(rowArray[1], rowArray[1]);
			
			for(D4mDbRow row : rowList) {
				String rowkey = row.getRow();
				Assert.assertTrue(map.containsKey(rowkey));
			}
                        System.out.println("TestD:: size = "+size);
			Assert.assertTrue("TestD::Yes 8 results!!!",size == 8);
			D4mDbQueryTest.print(d4m);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
		}
		System.out.println("--------------------------------------");


	}
	@Test
	public void testE() {
		String rows="b,";
		String cols= "a,b,";
		String authorizations="";
                D4mConfig.DEBUG=true;

		System.out.println("TestE::QUERY = ['"+ rows + "', '"+cols+"']");
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		int repeat=3;
		try {
			d4m.setLimit(2);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			
			for(int i=0; i < repeat; i++) {
				d4m.next();
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			HashMap<String,String> map = new HashMap<>();
			String [] rowArray = rows.split(",");
			map.put(rowArray[0], rowArray[0]);
			//map.put(rowArray[1], rowArray[1]);
			
			for(D4mDbRow row : rowList) {
				String rowkey = row.getRow();
				Assert.assertTrue("TestE:: rowKey matched...",map.containsKey(rowkey));
			}
			D4mDbQueryTest.print(d4m);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
			System.out.println("--------------------------------------");
		}

	}
	
	
	/*
	 *  TestF will test the doMatlabQueryOnColumn(row,col)
	 */
	@Test
	public void testF() {
		String rows=":";
		String cols= "a,";
		String authorizations="";
                D4mConfig.DEBUG=true;

		System.out.println("TestF::QUERY = ['"+ rows + "', '"+cols+"']");
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		int repeat=3;
		try {
			d4m.setLimit(1);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			
			while(d4m.hasNext()) {
				d4m.next();
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			HashMap<String,String> map = new HashMap<>();
			String [] rowArray = cols.split(",");
			map.put(rowArray[0], rowArray[0]);
			//map.put(rowArray[1], rowArray[1]);
			
			for(D4mDbRow row : rowList) {
				String rowkey = row.getColumn();
				Assert.assertTrue(map.containsKey(rowkey));
			}
			D4mDbQueryTest.print(d4m);
			Assert.assertTrue(size == 6);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
			System.out.println("--------------------------------------");
		}

	}

	/*
	 *  TestG 
	 *     row ='b : bASCII-127'
	 *     col = 'a,b,'
	 *     limit=1
	 */
	@Test
	public void testG() {
		String rows="b,:,b";
		int i127 = 127;
		String ascii127 = new Character((char)i127).toString();
		rows = rows+ascii127+",";
		String cols= "a,b,";
		String authorizations="";
                D4mConfig.DEBUG=true;

		System.out.println("TestG::QUERY = ['"+ rows + "', '"+cols+"']");
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		
		try {
			d4m.setLimit(1);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			
			while(d4m.hasNext()) {
				d4m.next();
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			HashMap<String,String> map = new HashMap<>();
			String [] rowArray = cols.split(",");
                        log.debug("RowArray[0]="+rowArray[0]);
			map.put(rowArray[0], rowArray[0]);
			map.put(rowArray[1], rowArray[1]);
			
			for(D4mDbRow row : rowList) {
				String rowkey = row.getColumn();
                                log.debug("TestG::rowKey = "+rowkey);
				Assert.assertTrue(map.containsKey(rowkey));
			}
			D4mDbQueryTest.print(d4m);
			Assert.assertTrue(size == 4);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
			System.out.println("--------------------------------------");
		}

	}

	/*
	 *  row="b,"
	 *  col="a,b,";
	 *  Set the limit=1
	 */
	@Test
	public void testH() {
		String rows="b,";
		String cols= "a,b,";
		String authorizations="";
                D4mConfig.DEBUG=true;

		System.out.println("TestH::QUERY = ['"+ rows + "', '"+cols+"']");
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		
		try {
			d4m.setLimit(1);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			
			while(d4m.hasNext()) {
				d4m.next();
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			HashMap<String,String> map = new HashMap<>();
			String [] tmpArray = cols.split(",");
			map.put(tmpArray[0], tmpArray[0]);
			map.put(tmpArray[1], tmpArray[1]);
			log.debug("   TestH:: col[0]="+tmpArray[0]);
			for(D4mDbRow row : rowList) {
				String rowkey = row.getColumn();
                                log.debug("    TestH::col="+rowkey);
				Assert.assertTrue(map.containsKey(rowkey));
			}
			D4mDbQueryTest.print(d4m);
			
			//Should get back 2 results
			Assert.assertTrue(size == 2);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
			System.out.println("--------------------------------------");
		}

	}

	/*
	 *  row="b,:, b(asci 127),"
	 *  col="a,:,a(asci 127),";
	 *  Set the limit=1
	 */
	@Test
	public void testI() {
		String rows="b,:,b";
		rows = rows+ASCI_127+",";
		String cols= "a,:,a"+ASCI_127+",";
		String authorizations="";
                D4mConfig.DEBUG=true;

		System.out.println("TestI::QUERY = ['"+ rows + "', '"+cols+"']");
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		
		try {
			d4m.setLimit(5);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			
			while(d4m.hasNext()) {
				d4m.next();
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			HashSet<String> map = new HashSet<>();
			String [] tmpArray = rows.split(",");
			map.add("b");
			map.add("bb");
			map.add("bbb");
			//map.put(rowArray[1], rowArray[1]);
			
			for(D4mDbRow row : rowList) {
				String rowkey = row.getRow();
				Assert.assertTrue(map.contains(rowkey));
			}
			D4mDbQueryTest.print(d4m);
			
			//Should get back 2 results
			Assert.assertTrue(size == 3);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
			System.out.println("--------------------------------------");
		}

	}

	/*
	 *  row=":"
	 *  col="a,:,a(asci 127),";
	 *  Set the limit=1
	 */
	@Test
	public void testJ() {
		String rows=":";
		String cols= "a,:,a"+ASCI_127+",";
		String authorizations="";
                D4mConfig.DEBUG=true;

		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		
		try {
			d4m.setLimit(1);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			
			while(d4m.hasNext()) {
				d4m.next();
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			//HashMap<String,String> map = new HashMap<String,String>();
			String [] tmpArray = cols.split(",");
			
			//map.put(rowArray[1], rowArray[1]);
			log.info("Number of results = "+size);
			String regex = RegExpUtil.makeRegex(tmpArray);
			for(D4mDbRow row : rowList) {
				String rowkey = row.getColumn();
					boolean isMatching = Pattern.matches(regex, rowkey);
					Assert.assertTrue(isMatching);
			}
			D4mDbQueryTest.print(d4m);
			//Should get back 2 results
			Assert.assertTrue(size == 10);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
			System.out.println("--------------------------------------");
		}

	}

	@Test
	public void testK() {
		String rows=":";
		String cols= ":";
		String authorizations="";
                D4mConfig.DEBUG=true;

		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		
		try {
			d4m.setLimit(1);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			int count=0;
			while(d4m.hasNext()) {
				d4m.next();
				count++;
				if(count > 20)
                                    break;
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			//HashMap<String,String> map = new HashMap<String,String>();
			String [] tmpArray = cols.split(",");
			
			//map.put(rowArray[1], rowArray[1]);
			log.info("Number of results = "+size);
			//	String regex = RegExpUtil.makeRegex(tmpArray);
			//for(D4mDbRow row : rowList) {
			//	String rowkey = row.getColumn();
			//		boolean isMatching = Pattern.matches(regex, rowkey);
			//		Assert.assertTrue(isMatching);
			//	}
			D4mDbQueryTest.print(d4m);
			//Should get back 2 results
			//Assert.assertTrue(size == 10);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
			System.out.println("--------------------------------------");
		}

	}

	@Test
	public void testL() {
		String rows=":";
		String cols= "a,b,";
		String authorizations="";
                D4mConfig.DEBUG=true;

		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		
		try {
			d4m.setLimit(1);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			int count=0;
			while(d4m.hasNext()) {
				d4m.next();
				count++;
				if(count > 20)
                                    break;
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			//HashMap<String,String> map = new HashMap<String,String>();
			String [] tmpArray = cols.split(",");
			
			//map.put(rowArray[1], rowArray[1]);
			log.info("Number of results = "+size);
			//	String regex = RegExpUtil.makeRegex(tmpArray);
			//for(D4mDbRow row : rowList) {
			//	String rowkey = row.getColumn();
			//		boolean isMatching = Pattern.matches(regex, rowkey);
			//		Assert.assertTrue(isMatching);
			//	}
			D4mDbQueryTest.print(d4m);
			//Should get back 2 results
			//Assert.assertTrue(size == 10);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
			System.out.println("--------------------------------------");
		}

	}

	@Test
	public void testM() {
		String rows="a,:,a,";
		String cols= "a,b,";
		String authorizations="";
                D4mConfig.DEBUG=true;

		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		
		try {
			d4m.setLimit(1);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			int count=0;
			while(d4m.hasNext()) {
				d4m.next();
				count++;
				if(count > 20)
                                    break;
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			//HashMap<String,String> map = new HashMap<String,String>();
			String [] tmpArray = cols.split(",");
			
			//map.put(rowArray[1], rowArray[1]);
			log.info("Number of results = "+size);
			//	String regex = RegExpUtil.makeRegex(tmpArray);
			//for(D4mDbRow row : rowList) {
			//	String rowkey = row.getColumn();
			//		boolean isMatching = Pattern.matches(regex, rowkey);
			//		Assert.assertTrue(isMatching);
			//	}
			D4mDbQueryTest.print(d4m);
			//Should get back 2 results
			//Assert.assertTrue(size == 10);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			d4m.close();
			System.out.println("--------------------------------------");
		}

	}

	//@Test
	public void testN() {
		String rows="a,b,";
		String cols= "a,b,";
		String authorizations="";
                D4mConfig.DEBUG=true;

		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		
		try {
			d4m.setLimit(1);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			int count=0;
			while(d4m.hasNext()) {
				d4m.next();
				count++;
				if(count > 5)  break;
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			//HashMap<String,String> map = new HashMap<String,String>();
			String [] tmpArray = cols.split(",");
			
			//map.put(rowArray[1], rowArray[1]);
			log.info("Number of results = "+size);
			//	String regex = RegExpUtil.makeRegex(tmpArray);
			//for(D4mDbRow row : rowList) {
			//	String rowkey = row.getColumn();
			//		boolean isMatching = Pattern.matches(regex, rowkey);
			//		Assert.assertTrue(isMatching);
			//	}
			D4mDbQueryTest.print(d4m);
			//Should get back 2 results
			//Assert.assertTrue(size == 10);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
			System.out.println("--------------------------------------");
		}

	}

	@Test
	public void testO() {
		String rows="a,:,a";
		rows = rows+this.ASCI_127+",";
		String cols= "a,:,a";
		cols = cols + this.ASCI_127 + ",";
		String authorizations="";
                D4mConfig.DEBUG=true;

		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		
		try {
			d4m.setLimit(1);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			int count=0;
			while(d4m.hasNext()) {
				d4m.next();
				count++;
				if(count > 20)
                                    break;
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			//HashMap<String,String> map = new HashMap<String,String>();
			String [] tmpArray = cols.split(",");
			
			//map.put(rowArray[1], rowArray[1]);
			log.info("Number of results = "+size);
			//	String regex = RegExpUtil.makeRegex(tmpArray);
			//for(D4mDbRow row : rowList) {
			//	String rowkey = row.getColumn();
			//		boolean isMatching = Pattern.matches(regex, rowkey);
			//		Assert.assertTrue(isMatching);
			//	}
			D4mDbQueryTest.print(d4m);
			//Should get back 2 results
			//Assert.assertTrue(size == 10);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
			System.out.println("--------------------------------------");
		}

	}

	@Test
	public void testP() {
		String rows="a,b,";
		//rows = rows+this.ASCI_127+",";
		String cols= "a,";
		//cols = cols + this.ASCI_127 + ",";
		String authorizations="";

                D4mConfig.DEBUG=true;
		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		
		try {
			d4m.setLimit(1);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			int count=0;
			while(d4m.hasNext()) {
				d4m.next();
				count++;
				//if(count > 20) break;
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			//HashMap<String,String> map = new HashMap<String,String>();
			String [] tmpArray = cols.split(",");
			
			//map.put(rowArray[1], rowArray[1]);
			log.info("Number of results = "+size);
			//	String regex = RegExpUtil.makeRegex(tmpArray);
			//for(D4mDbRow row : rowList) {
			//	String rowkey = row.getColumn();
			//		boolean isMatching = Pattern.matches(regex, rowkey);
			//		Assert.assertTrue(isMatching);
			//	}
			D4mDbQueryTest.print(d4m);
			//Should get back 2 results
			//Assert.assertTrue(size == 10);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
			System.out.println("--------------------------------------");
		}

	}
}
