/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author CHV8091
 *
 */
public abstract class D4mInsertBase {
	private static final Logger log = LoggerFactory.getLogger(D4mInsertBase.class);

	protected String tableName = "";
	protected String rows = "";
	protected String cols = "";
	protected String vals = "";
	protected String family = "";
	protected String visibility = "";

	protected ConnectionProperties connProps = new ConnectionProperties();

	public D4mInsertBase() {
	    super();
	}
	public D4mInsertBase(String instanceName,String hostName, String tableName, String username, String password) {
	    super();
		init(instanceName,hostName,tableName,username,password);
	}

	
	public void init(String instanceName, String hostName, String tableName,
			String username, String password) {
		connProps.setInstanceName(instanceName);
		connProps.setHost(hostName);
		connProps.setUser(username);
		connProps.setPass(password);
		this.tableName = tableName;
	}

	
	public void doProcessing(String rows, String cols, String vals,
			String family, String visibility)throws Exception {
		this.rows = rows;
		this.cols = cols;
		this.vals = vals;
		this.family = family;
		this.visibility = visibility;
		long start = System.currentTimeMillis();
		doProcessing();
		long end = System.currentTimeMillis();
		double elapsed= ((double)(end-start))/1000.0;
		if(log.isDebugEnabled()) {
			String s = "Ingest time : "+ elapsed + " sec";
			log.debug(s);
		}
	}

	abstract public void doProcessing() throws Exception;

	public void setConnProps(ConnectionProperties connProps) {
		this.connProps = connProps;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% D4M: Dynamic Distributed Dimensional Data Model
% MIT Lincoln Laboratory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% (c) <2010> Massachusetts Institute of Technology
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */

