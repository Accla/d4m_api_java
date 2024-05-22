
/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.accumulo;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.D4mException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.thrift.transport.TTransportException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

//${accumulo.VERSION.1.6}import org.apache.accumulo.core.security.Credentials;  // 1.6
//${accumulo.VERSION.1.6}import org.apache.accumulo.core.util.ThriftUtil; // 1.6

/**
 * @author cyee
 *
 */
public class AccumuloConnection {
	private static final Logger log = LoggerFactory.getLogger(AccumuloConnection.class);

	private ConnectionProperties conn=null;
	private ZooKeeperInstance instance=null;
	private Connector connector= null;
	private Authorizations auth= Authorizations.EMPTY;
	public static final long maxMemory= 1024000L;
	public static final long maxLatency = 30;

	private String principal;
	private AuthenticationToken token;

	/**
	 * 
	 */
	public AccumuloConnection(ConnectionProperties conn) throws AccumuloException,AccumuloSecurityException {
		this.conn = conn;
		ClientConfiguration cconfig = new ClientConfiguration().withInstance(conn.getInstanceName()).withZkHosts(conn.getHost()).withZkTimeout(conn.getSessionTimeOut());
		this.instance = new ZooKeeperInstance(cconfig);
		principal = conn.getUser();
		token = new PasswordToken(conn.getPass());

        //principal = username = this.conn.getUser()
        //System.out.println("about to make connector: user="+this.conn.getUser()+"   password="+ new String(this.passwordToken.getPassword()));
        this.connector = this.instance.getConnector(this.conn.getUser(), token);
        //System.out.println("made connector");
        String [] sAuth = conn.getAuthorizations();
        if (sAuth != null && sAuth.length > 0) {
            this.auth = new Authorizations(sAuth);
        } else {
            this.auth= Authorizations.EMPTY;
        }
        if (log.isDebugEnabled())
	        log.debug("!!!WHOAMI="+this.connector.whoami());
	}



	public void createTable(String tableName) {
		try {
			connector.tableOperations().create(tableName);
		} catch (AccumuloException | AccumuloSecurityException e) {		
			log.warn("",e.getMessage());
		} catch (TableExistsException e) {
			log.warn("Table "+ tableName+"  exist.",e.getMessage());
		}
	}


	public BatchWriter createBatchWriter (String table, long maxMemory, long maxLatency,int maxWriteThreads) throws TableNotFoundException {
		BatchWriterConfig bwc = new BatchWriterConfig()
				.setMaxLatency(maxLatency, TimeUnit.MILLISECONDS)
				.setMaxMemory(maxMemory)
				.setMaxWriteThreads(maxWriteThreads);
		return connector.createBatchWriter(table, bwc);
	}
	public BatchWriter createBatchWriter (String table) throws TableNotFoundException {
		return createBatchWriter(table, maxMemory, maxLatency, conn.getMaxNumThreads());
	}


	public Scanner createScanner(String tableName) throws TableNotFoundException {
		return this.connector.createScanner(tableName, this.auth);
	}

	public BatchScanner getBatchScanner(String tableName, int numberOfThreads) throws TableNotFoundException  {
		return connector.createBatchScanner(tableName, this.auth, numberOfThreads);
	}

	public void deleteTable (String tableName)  {
		try {
			connector.tableOperations().delete(tableName);
		} catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
			log.warn("",e);
		}
	}

	public boolean tableExist(String tableName) {
		return connector.tableOperations().exists(tableName);

	}

	public void addSplit(String tableName, SortedSet<Text> partitions) {
		try {
			connector.tableOperations().addSplits(tableName, partitions);
		} catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
			log.warn("",e);
		}
	}
	public Instance getInstance() {	
		return connector.getInstance();
	}

	public MasterClientService.Client getMasterClient() throws TTransportException {
		//${accumulo.VERSION.1.6}return MasterClient.getConnection(getInstance()); // 1.6
    return MasterClient.getConnection(new ClientContext(instance, new Credentials(principal, token), instance.getConfiguration())); // 1.7
	}

	public TabletClientService.Iface getTabletClient (String tserverAddress) throws TTransportException {
		HostAndPort address = AddressUtil.parseAddress(tserverAddress,false);
    //${accumulo.VERSION.1.6}return ThriftUtil.getTServerClient( tserverAddress, instance.getConfiguration()); // 1.6
    return ThriftUtil.getTServerClient( address, new ClientContext(instance, new Credentials(principal, token), instance.getConfiguration())); // 1.7
	}

  public Map<String, String> getNameToIdMap() {
		//Map<String, String> nameToIdMap = Tables.getNameToIdMap(instance);
		return Tables.getNameToIdMap(getInstance());
	}
	public Collection<Text> getSplits(String tableName) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
		return this.connector.tableOperations().listSplits(tableName);
	}
	public SortedSet<String> getTableList() {
		return this.connector.tableOperations().list();
	}

	// TODO these are just wrappers; why have them when we could expose the TableOperations object directly?
	public void addIterator(String tableName, IteratorSetting iterSet) throws D4mException
	{
		TableOperations tops = this.connector.tableOperations();
		try {
			tops.attachIterator(tableName, iterSet); // adds on all scopes: majc, minc, scan 
		} catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
			log.warn("",e);
			throw new D4mException(e);
		}
	}

	public Map<String,EnumSet<IteratorUtil.IteratorScope>> listIterators(String tableName) throws D4mException
	{
		TableOperations tops = this.connector.tableOperations();
		try {
			return tops.listIterators(tableName);
		} catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
			log.warn("",e);
			throw new D4mException(e);
		}
	}

	public IteratorSetting getIteratorSetting(String tableName, String name, IteratorUtil.IteratorScope scope) throws D4mException
	{
		TableOperations tops = this.connector.tableOperations();
		try {
			return tops.getIteratorSetting(tableName, name, scope);
		} catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
			log.warn("",e);
			//e.printStackTrace();
			throw new D4mException(e);
		}
	}

	public void removeIterator(String tableName, String name, EnumSet<IteratorUtil.IteratorScope> scopes) throws D4mException
	{
		TableOperations tops = this.connector.tableOperations();
		try {
			tops.removeIterator(tableName, name, scopes);
		} catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
			log.warn("",e);
			throw new D4mException(e);
		}
  }

	public void checkIteratorConflicts(String tableName, IteratorSetting cfg, EnumSet<IteratorScope> scopes) throws D4mException 
	{
		TableOperations tops = this.connector.tableOperations();
		try {
			tops.checkIteratorConflicts(tableName, cfg, scopes);
		} catch (AccumuloException | TableNotFoundException e) {
			log.warn("",e);
			throw new D4mException(e);
		}

  }

	public void merge(String tableName, String startRow, String endRow) throws D4mException {
		TableOperations tops = this.connector.tableOperations();
		try {
			tops.merge(tableName, startRow == null ? null : new Text(startRow), endRow == null ? null : new Text(endRow));
		} catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
			log.warn("",e);
			throw new D4mException(e);
		}
  }

	public  TCredentials getCredentials() throws D4mException {
		TCredentials tCred;
		try {
			// DH2015: Copied from
			// 1.6: org.apache.accumulo.core.security.Credentials#toThrift()
			// 1.7: org.apache.accumulo.core.client.impl.Credentials#toThrift()
			// in order to create compatibility to 1.6 and 1.7, since the Credentials class moved.
			// This may still break in later versions because Credentials is non-public API.
			tCred = new TCredentials(principal, token.getClass().getName(),
					ByteBuffer.wrap(AuthenticationToken.AuthenticationTokenSerializer.serialize(token)), instance.getInstanceID());
			if (token.isDestroyed())
				throw new RuntimeException("Token has been destroyed", new AccumuloSecurityException(principal, SecurityErrorCode.TOKEN_EXPIRED));

//			tCred =  this.creds.toThrift(this.instance);  //.create(this.conn.getUser(), this.passwordToken, this.instance.getInstanceID() );
		} catch (Exception e) {
			log.warn("",e);
			throw new D4mException(e);
		}
		return tCred;
	}

	public void setConnectionProperties(ConnectionProperties connProp) {
		this.conn = connProp;
	}
	/*
	 *   auths    comma-separated list of authorizations
	 */	
	public void setAuthorizations(String auths) {
		if(auths == null) {
			this.auth= Authorizations.EMPTY;
			return;
		}
		this.auth = new Authorizations(auths.split(","));
	}
	public void setAuthorizations(ConnectionProperties connProp) {

		String [] sAuth = connProp.getAuthorizations();

		if(sAuth != null && sAuth.length > 0 )
			this.auth = new Authorizations(sAuth);
		else if( sAuth == null ){
			this.auth= Authorizations.EMPTY;
		}
	}

	public String locateTablet(String tableName, String splitName) {
		String tabletName = null;
		try {
			//${accumulo.VERSION.1.6}TabletLocator tc = TabletLocator.getLocator(instance, new Text(Tables.getTableId(instance, tableName))); // 1.6 change to getLocator for 1.6
      ClientContext cc = new ClientContext(instance, new Credentials(principal, token), instance.getConfiguration()); // 1.7
			// Change in API in 1.7 and 1.8 -- second parameter is String instead of Text
			String str = Tables.getTableId(instance, tableName);

			TabletLocator tc = getTabletLocator(cc, str); // use dynamic invocation to cross the API change
			
			org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation loc =
					//${accumulo.VERSION.1.6}tc.locateTablet(new Credentials(principal, token), new Text(splitName), false, false); // 1.6
          tc.locateTablet(cc, new Text(splitName), false, false); // 1.7
			tabletName = loc.tablet_location;
			log.debug("TableName="+tableName+", TABLET_NAME = "+tabletName);
		} catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
			log.warn("",e);
			e.printStackTrace();
		}


    return tabletName;
	}

	private TabletLocator getTabletLocator(ClientContext cc, String tableid) {
		// first try new one
		for (Method method : TabletLocator.class.getMethods()) {
			if (method.getName().equals("getLocator") && Modifier.isStatic(method.getModifiers()) && method.getReturnType().equals(TabletLocator.class)) {
				Type[] types = method.getGenericParameterTypes();
				if (types.length == 2 && types[0].equals(ClientContext.class)) {
					try {
						if (types[1].equals(String.class)) {
							return (TabletLocator) method.invoke(null, cc, tableid);
						} else if (types[1].equals(Text.class)) {
							return (TabletLocator) method.invoke(null, cc, new Text(tableid));
						}
					} catch (InvocationTargetException | IllegalAccessException e) {
						log.warn("problem calling getLocator on "+method, e);
					}
				}
			}
		}
		throw new RuntimeException("Cannot locate tablets for tableid "+tableid);
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
