package edu.mit.ll.d4m.db.cloud;

import java.util.ArrayList;

//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.mit.ll.d4m.db.cloud.accumulo.D4mDbQueryAccumulo;

public class TestD4mDbQuery {
	private static final Logger log = LoggerFactory.getLogger(TestD4mDbQuery.class);
	private static int count=0;
	
	// public void testQueryForAll() {
	// 	String row = ":";
	// 	String col = ":";
	// 	D4mDbQueryAccumulo d4m=null;

	// }
	public static void main(String[] args) throws Exception	{
		String instanceName = args[0];
		String host = args[1];
		String username = args[2];
		String password = args[3];
		String table = args[4];

		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		String rows=":";
		String cols = ":";
		String family= "";
		
		String authorizations="";
		if(args[5].length() > 0)
		authorizations=args[5];
		int numRepeats = Integer.parseInt(args[6]);
		family = args[7];
		d4m.setLimit(5);
		d4m.doTest = true;
		d4m.doMatlabQuery(rows, cols, family, authorizations);
		//print(d4m);
		for (int i = 0; i < numRepeats; i++) {
		d4m.next();
		}
		//print(d4m);
		//d4m.next();
		print(d4m);
		System.out.println("Cumulative count = "+d4m.getCumCount());
	}

	public static void print(D4mDbQueryAccumulo d4m) {
//		String rowresults = d4m.rowReturnString;
//		String colresults = d4m.columnReturnString;
//		log.info(rowresults+","+colresults);
//		System.out.println(rowresults+","+colresults);
		
		D4mDbResultSet results = d4m.testResultSet;
		ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
		for(D4mDbRow row : rowList) {
			System.out.println(count + "  "+ row.toString());
			count++;
		}
		System.out.println(" Total Number of rows "+rowList.size());
	}
}
