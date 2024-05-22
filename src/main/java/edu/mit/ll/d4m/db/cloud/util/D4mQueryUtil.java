/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.util;

import edu.mit.ll.d4m.db.cloud.QueryMethod;
//import org.apache.logging.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Static methods
 * @author cyee
 *
 */
public class D4mQueryUtil {

	private static final Logger log = LoggerFactory.getLogger(D4mQueryUtil.class);
	public static final String KEY_RANGE = "KEY_RANGE";
	public static final String REGEX_RANGE = "REGEX_RANGE";
	public static final String POSITIVE_INFINITY_RANGE = "POSITIVE_INFINITY_RANGE";
	public static final String NEGATIVE_INFINITY_RANGE = "NEGATIVE_INFINITY_RANGE";

	private D4mQueryUtil() {
	}


	public static D4mDataObj whatQueryMethod (String rows, String cols) {

		D4mDataObj dataObj= new D4mDataObj();
		dataObj.setRow(rows);
		dataObj.setColQualifier(cols);

		if (!rows.equals(":") && cols.equals(":")) {

//			HashMap<String, Object> rowMap = processParam(rows);
			//this.rowMap = rowMap;
			String[] paramContent = processParam(rows);//(String[]) rowMap.get("content");
			// System.out.println("this.isRangeQuery(paramContent)="+this.isRangeQuery(paramContent));
			if (isRangeQuery(paramContent)) {
				log.debug("MATLAB_RANGE_QUERY_ON_ROWS");
				dataObj.setQueryMethod(QueryMethod.MATLAB_RANGE_QUERY_ON_ROWS);
				//return this.doMatlabRangeQueryOnRows(rows, cols);
			}
			else {
				log.debug("MATLAB_QUERY_ON_ROWS");
				dataObj.setQueryMethod(QueryMethod.MATLAB_QUERY_ON_ROWS);
				//				return this.doMatlabQueryOnRows(rows, cols);
			}
		} else if (rows.equals(":") && !cols.equals(":")) {
			log.debug("MATLAB_QUERY_ON_COLS");
			dataObj.setQueryMethod(QueryMethod.MATLAB_QUERY_ON_COLS);
			//			return this.doMatlabQueryOnColumns(rows, cols);
		} else if (rows.equals(":") && cols.equals(":")) {
			log.debug("GET_ALL_DATA");
			dataObj.setQueryMethod(QueryMethod.GET_ALL_DATA);
			//		return this.getAllData();
		} else if( !rows.startsWith(":") && !rows.equals(":") && !cols.startsWith(":") && !cols.equals(":")) {
			log.debug("SEARCH_BY_ROW_&_COL");
			dataObj.setQueryMethod(QueryMethod.SEARCH_BY_ROW_AND_COL);
			//	return this.searchByRowAndColumn(rows, cols, null,null);
		} else {
			//AssocColumnWithRow
			dataObj.setQueryMethod(QueryMethod.ASSOC_COLUMN_WITH_ROW);

		}


		return dataObj;
	}



	/**
	 * Range Queries are the following 'a,:,b,',  'a,:,end,',  ',:,b,'.
	 *  Note: Negative infinity Range a*,
	 */
	public static boolean isRangeQuery(String[] paramContent) {
		boolean rangeQuery = false;
		if (paramContent.length == 1 && paramContent[0].contains("*")) {
			rangeQuery = true;
		}
		if (paramContent.length == 3 && paramContent[1].contains(":")) {
			rangeQuery = true;
		}
		return rangeQuery;
	}

	public static String getRangeQueryType(String[] paramContent) {
		/*
		 * Range Queries are the following 'a,:,b,',  'a,:,end,',  ',:,b,'. 
		 * Note: Negative Infinity Range a*,
		 */
		String rangeQueryType = "";
		if (paramContent[0].contains("*")) {
			rangeQueryType = D4mQueryUtil.REGEX_RANGE;
		}
		if (paramContent.length == 3 && paramContent[1].contains(":")) {
			rangeQueryType = D4mQueryUtil.KEY_RANGE;
		}
		if (paramContent.length == 3) {
			if (paramContent[1].contains(":") && paramContent[2].toLowerCase().contains("end")) {
				rangeQueryType = D4mQueryUtil.POSITIVE_INFINITY_RANGE;
			}
			if (paramContent[1].contains(":") && paramContent[0].equals("")) {
				rangeQueryType = D4mQueryUtil.NEGATIVE_INFINITY_RANGE;
			}
		}
		return rangeQueryType;
	}
	
	/**
	 * Refitted by DH to just return the parameter split into an array of strings by the delimiter (the last character)
	 * @param param The string to split
	 * @return Array of Strings (w/o the delimiter)
	 */
	public static String[] processParam(String param) {
		if (param == null || param.isEmpty())
			return null;
		//HashMap<String, Object> map = new HashMap<String, Object>();
		int lastIdx = param.length() - 1;
		String content = param.substring(0, lastIdx);
		String delim = param.substring(lastIdx);
		return content.split(delim);
		//map.put("delimiter", delim);
		/*if (delim.equals("|")) {
			delim = "\\" + delim;
		}*/
		//map.put("content", content.split(delim));
		//map.put("length", content.length());
		//return map;
	}

	public static boolean isWithInRange(String key, String [] rangeCriteria) {
		boolean isInRange = false;
		
		if(rangeCriteria.length == 3 && rangeCriteria[1].equals(":")) {
			isInRange = (key.compareTo(rangeCriteria[0]) > 0 && key.compareTo(rangeCriteria[2]) < 0);
		}
		return isInRange;
	}
}
