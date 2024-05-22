/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.test;

import java.util.ArrayList;
import java.util.HashSet;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.*;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.mit.ll.d4m.db.cloud.accumulo.D4mDbQueryAccumulo;
import edu.mit.ll.d4m.db.cloud.util.D4mDataObj;

/**
 * @author CHV8091
 *
 */
public class D4mQueryTest10 {

	private static final Logger log = LoggerFactory.getLogger(D4mQueryTest10.class);

	String instanceName = "";
	String host = "";
	String username = "";
	String password = "";
	String table = "";
	String columnFamily="";
	int i127=127;
	String ASCI_127 = new Character((char)i127).toString();

	@Before
	public void setUp() throws Exception {
        AccumuloTestConnection testConnection = new AccumuloTestConnection("local.conf");
        ConnectionProperties cp = testConnection.getConnectionProperties();
        instanceName               = cp.getInstanceName();
        host                       = cp.getHost();
        username                   = cp.getUser();
        password                   = cp.getPass();
		table                      = "iTest10";
		String row = "a,a,a,a,a,a,aa,aa,aaa,aaa,b,b,bb,bb,bbb,bbb,";
		String col = "a,aa,aaa,b,bb,bbb,a,aa,a,aaa,a,b,a,bb,a,bbb,";
		String val = "a-a,a-aa,a-aaa,a-b,a-bb,a-bbb,aa-a,aa-aa,aaa-a,aaa-aaa,b-a,b-b,bb-a,bb-bb,bbb-a,bbb-bbb,";
	//	tearDown();
		D4mDbInsert dbInsert = new D4mDbInsert(instanceName,host,table,username,password);

		dbInsert.doProcessing(row, col, val,"","");


	}
	/**
	 * @throws Exception
	 */
	@After
	public void tearDown() throws Exception {
		D4mDbTableOperations dbTable = new D4mDbTableOperations(instanceName,host,username,password);
		dbTable.deleteTable(table);

	}

	@Test
	public void test1() {
		String rows="a,";
		String cols= ":";
		String authorizations="";
		log.debug("Test1::QUERY = ['"+ rows + "', '"+cols+"']");

		D4mConfig.DEBUG=true;
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;

		try {
			//First query
			d4m.setLimit(10);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			d4m.next();

			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			HashSet<String> map = new HashSet<>();
			String [] rowArray = rows.split(",");
			map.add(rowArray[0]);

			// Only 'a' should be a row value
			for(D4mDbRow row : rowList) {
				String rowkey = row.getRow();
				Assert.assertTrue(map.contains(rowkey));
			}
			// There should be 6 results
			Assert.assertTrue(size == 6);

			print(d4m.getResults());

			//rows="b,";
			//cols= ":";
			//Second query
			//log.debug("Test1::QUERY = ['"+ rows + "', '"+cols+"']");
			//d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			//d4m.next();
			//D4mDbResultSet resultsB = d4m.testResultSet;
			//ArrayList<D4mDbRow> rowListB = resultsB.getMatlabDbRow();
			//size = rowListB.size();
			//Assert.assertTrue(size == 2);

			//d4m.doNewMatlabQuery(rows, cols, columnFamily, authorizations);
			//d4m.nextNewMatlabQuery();
			//D4mDbQueryTest.print(d4m);

		} catch (Exception e) {
			e.printStackTrace();
		} 
		finally {
			d4m.close();
		}
		System.out.println("--------------------------------------");
	}
	@Test
	public void test2() {
		String rows=":";
		String cols= ":";
		String authorizations="";
		System.out.println("Test2::QUERY = ['"+ rows + "', '"+cols+"']");
		D4mConfig.DEBUG=true;

		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		try {
			//First query
			d4m.setLimit(0);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			d4m.next();
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			HashSet<String> map = new HashSet<>();
			String [] rowArray = rows.split(",");
			map.add("a");
			map.add("aa");

			map.add("aaa");
			map.add("b");
			map.add("bb");
			map.add("bbb");
			// Only 'a' should be a row value
			int cnt = 0;
			for(D4mDbRow row : rowList) {
				String rowkey = row.getRow();
				
				Assert.assertTrue(map.contains(rowkey));
				cnt++;
			}
			// There should be 6 results
			Assert.assertTrue(size == cnt);

			
			print(d4m.getResults());


			//D4mDbQueryTest.print(d4m);

		} catch (Exception e) {
			e.printStackTrace();
		} 
		finally {
			d4m.close();
		}
		System.out.println("--------------------------------------");
	}

	@Test
	public void test3() {
		//String rows="b,:,b";
		String rows="a,";
		int i127 = 127;
		String ascii127 = new Character((char)i127).toString();
		//rows = rows+ascii127+",";
		String cols= "a,:,a";
		cols = cols+ascii127+",";
		String authorizations="";
		D4mConfig.DEBUG=true;
		System.out.println("Test3::QUERY = ['"+ rows + "', '"+cols+"']");
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

			HashSet<String> map = new HashSet<>();
			String [] rowArray = cols.split(",");
			map.add("a");
			map.add("aa");
			map.add("aaa");
			//map.put(rowArray[1], rowArray[1]);

			for(D4mDbRow row : rowList) {
				String rowkey = row.getColumn();
				Assert.assertTrue(map.contains(rowkey));
			}
			D4mDbQueryTest.print(d4m);
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

	@Test
	public void test4() {
		//String rows="b,:,b";
		String rows="a,";
		int i127 = 127;
		String ascii127 = new Character((char)i127).toString();
		//rows = rows+ascii127+",";
		String cols= "a,:,aaa,";
		//cols = cols+ascii127+",";
		String authorizations="";
		D4mConfig.DEBUG=true;
		System.out.println("Test4QUERY = ['"+ rows + "', '"+cols+"']");
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

			HashSet<String> map = new HashSet<>();
			//String [] rowArray = cols.split(",");
			map.add("a");
			map.add("aa");
			map.add("aaa");//map.put(rowArray[1], rowArray[1]);

			for(D4mDbRow row : rowList) {
				String col = row.getColumn();
				Assert.assertTrue(map.contains(col));
			}
			D4mDbQueryTest.print(d4m);
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

	public void print(D4mDataObj d4mData) {
		System.out.println(d4mData.getRow());
		ArrayList<D4mDbRow>  testDataList = d4mData.getRowList();
		if(testDataList != null) {
			System.out.println("SIZE = "+testDataList.size());
			int cnt = 0;
			for(D4mDbRow row : testDataList) {
				cnt++;
				System.out.println(cnt+" :   "+row.toString());
			}
		}
	}
}
