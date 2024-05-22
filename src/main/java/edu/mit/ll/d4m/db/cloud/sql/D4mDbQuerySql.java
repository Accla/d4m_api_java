/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.mit.ll.d4m.db.cloud.D4mParent;

/**
 * Class to execute a SQL query and hold results as string [row, column, value]
 *  
 *  
 *  *Create db connection
 *  *Execute query
 *  *Iterate over ResultSet
 * 
 * @author CHV8091
 *
 */
public class D4mDbQuerySql extends D4mParent {
    private static final Logger log = LoggerFactory.getLogger(D4mDbQuerySql.class);
    private String rows = new String("");
    private String cols = new String("");
    private String vals = new String("");
    private Connection conn=null;
    private int recordCount=0;

    /*
     * Make DB connection
     */
    public D4mDbQuerySql(String url, String user, String pword) {
        super();
        try {
            conn = DriverManager.getConnection(url, user, pword);
        } catch (SQLException e) {
            log.warn("",e);
        }

    }
    public D4mDbQuerySql() {
        super();

    }

    public void executeQuery( String sqlQuery) {
        this.recordCount=0;
        this.rows = "";
        this.cols = "";
        this.vals = "";
        Statement st;
        try {
            st = this.conn.createStatement();
            ResultSet rs = st.executeQuery(sqlQuery);
            extractRowsColumnsValues(rs);
        } catch (SQLException e) {
            log.warn("",e);
        }

    }
    
    private void extractRowsColumnsValues(ResultSet rs) throws SQLException {
        StringBuffer sbVals= new StringBuffer("");
        StringBuffer sbCols= new StringBuffer("");
        StringBuffer sbRows= new StringBuffer("");
        int rowCount=1;
        while(rs.next()) {
            ResultSetMetaData rsmd = rs.getMetaData();
            int colCount= rsmd.getColumnCount();
            rowCount = rs.getRow();
            for(int i = 1; i <= colCount; i++) {
                String c = rsmd.getColumnName(i);
                String v = rs.getString(i);

                log.debug("ROW_COUNT="+rowCount+", COLUMN_NAME="+c+", VALUE=" + v);
                if(v == null || v.isEmpty()) {
                    sbVals.append("NULL");
                } else {
                    sbVals.append( v.replace("\n", ""));
                }
                sbVals.append("\n");  
                sbCols.append(c).append("\n");
                sbRows.append(Integer.toString(rowCount)).append("\n");
            }
            this.recordCount++;
        }

        vals = sbVals.toString();
        cols = sbCols.toString();
        rows = sbRows.toString();
    }
    public Connection getConn() {
        return conn;
    }
    public void setConn(Connection conn) {
        this.conn = conn;
    }
    public String getVals() {
        return vals;
    }
    public void setVals(String vals) {
        this.vals = vals;
    }
    public String getRows() {
        return rows;
    }
    public void setRows(String rows) {
        this.rows = rows;
    }
    public String getCols() {
        return cols;
    }
    public void setCols(String cols) {
        this.cols = cols;
    }

    public void close() {
        try {
            this.conn.close();
        } catch (SQLException e) {
            log.warn(" SQL exception.",e);
        }
    }
        public int getRecordCount() {
            return recordCount;
        }
        public void setRecordCount(int recordCount) {
            this.recordCount = recordCount;
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
