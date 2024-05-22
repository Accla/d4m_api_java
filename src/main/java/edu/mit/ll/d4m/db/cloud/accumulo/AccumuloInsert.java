package edu.mit.ll.d4m.db.cloud.accumulo;

import edu.mit.ll.d4m.db.cloud.D4mDbInsert;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.UIntegerLexicoder;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.mit.ll.d4m.db.cloud.D4mInsertBase;
import edu.mit.ll.d4m.db.cloud.util.D4mQueryUtil;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

/**
 * @author CHV8091
 *
 */
public class AccumuloInsert extends D4mInsertBase {
	private static final Logger log = LoggerFactory.getLogger(AccumuloInsert.class);

	AccumuloConnection connection=null;
	public AccumuloInsert() {
		super();
	}


	public AccumuloInsert(String instanceName, String hostName, String tableName, String username, String password) {
		super(instanceName, hostName, tableName, username, password);

	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mInsertBase#doProcessing()
	 */
	@Override
	public void doProcessing() throws AccumuloException,AccumuloSecurityException,TableNotFoundException{
		if(connection == null)
			connection = new AccumuloConnection(super.connProps);

		//Create table
		createTable();
		//Build mutations and sort
		//Add mutation
		//    make connection
		try {
			makeAndAddMutations();
		} catch (MutationsRejectedException | TableNotFoundException e) {
			log.error("",e);
			throw e;
		}

	}

	private static class RowIndex implements Comparator<Integer> {
		private final String[] array;

		public RowIndex(String[] array) {
			this.array = array;
		}

		public Integer[] createIndexArray() {
			Integer[] indexes = new Integer[array.length];
			for (int i = 0; i < array.length; i++) {
				indexes[i] = i;
			}
			return indexes;
		}

		@Override
		public int compare(Integer index1, Integer index2) {
			return array[index1].compareTo(array[index2]);
		}
	}

	private void makeAndAddMutations() throws TableNotFoundException, MutationsRejectedException {
		//		AccumuloConnection connection = new AccumuloConnection(super.connProps);
//		HashMap<String, Object> rowsMap = D4mQueryUtil.processParam(rows);
//		HashMap<String, Object> colsMap = D4mQueryUtil.processParam(cols);
//		HashMap<String, Object> weightMap = D4mQueryUtil.processParam(vals);

		final String[] rowsArr = D4mQueryUtil.processParam(rows);//(String[]) rowsMap.get("content");
		if (rowsArr.length == 0)
			return;
		final String[] colsArr = D4mQueryUtil.processParam(cols);//(String[]) colsMap.get("content");
		final String[] valsArr = D4mQueryUtil.processParam(vals);//(String[]) weightMap.get("content");

		final BatchWriter bw = this.connection.createBatchWriter(tableName);

		// DH: group same rows together for efficient mutations
		final RowIndex rowIndex = new RowIndex(rowsArr);
		final Integer[] indices = rowIndex.createIndexArray();
		Arrays.sort(indices, rowIndex);

		final ColumnVisibility colVisibility = new ColumnVisibility(super.visibility);
		final Text colFamily = new Text(super.family);
		String mRow = rowsArr[indices[0]];
		Mutation m = new Mutation(transformRow(mRow));
		final Text mCol = new Text(transformColQ(colsArr[indices[0]]));
		final Value mVal = new Value(transformVal(valsArr[indices[0]]));

		for (int i : indices) {
			if (!mRow.equals(rowsArr[i])) {
				if( !D4mDbInsert.isIntEncodeValueAndDropEmpty() || m.size() > 0 )
					bw.addMutation(m);
				mRow = rowsArr[i];
				m = new Mutation(transformRow(rowsArr[i]));
			}

			if (D4mDbInsert.isIntEncodeValueAndDropEmpty() && mRow.compareTo(colsArr[i]) >= 0)
				continue;

			mCol.set(transformColQ(colsArr[i]));
			mVal.set(transformVal(valsArr[i]));

			if (log.isDebugEnabled())
				log.debug(i+" - INSERTING [r,c,v] =  ["+ mRow +","+mCol+","+mVal+"]");

			m.put(colFamily, mCol, colVisibility, mVal);
		}

		if( m.size() > 0 )
			bw.addMutation(m);
		bw.close();
	}

	private static final Lexicoder<Integer> LEX = new UIntegerLexicoder();

	private static byte[] transformRow(final String str) {
		if (D4mDbInsert.isIntEncodeKeyAndPrependLastByteRev()) {
			// parse String as int
			// first byte is the last byte of the int, reversed
			final int i = Integer.parseInt(str);
			return new byte[]{
					ReverseByteTable[i & 0xFF],
					(byte) (i >> 24),
					(byte) (i >> 16),
					(byte) (i >> 8),
					(byte) i
			};
		} else
			return str.getBytes(StandardCharsets.UTF_8);
	}
	private static byte[] transformColQ(final String str) {
		if( D4mDbInsert.isIntEncodeKeyAndPrependLastByteRev()) {
			final int i = Integer.parseInt(str);
			return new byte[] {
					(byte) (i >> 24),
					(byte) (i >> 16),
					(byte) (i >> 8),
					(byte)  i
			};
		} else
			return str.getBytes(StandardCharsets.UTF_8);
	}
	private static byte[] transformVal(final String str) {
		if( D4mDbInsert.isIntEncodeValueAndDropEmpty() || D4mDbInsert.isIntEncodeKeyAndPrependLastByteRev())
			return str.equals("1") ? EMPTY_BYTES : LEX.encode(Integer.parseInt(str));
		else
			return str.getBytes(StandardCharsets.UTF_8);
	}
	private static final byte[] EMPTY_BYTES = new byte[0];

	private static final byte[] ReverseByteTable = new byte[] {
			(byte)0x00, (byte)0x80, (byte)0x40, (byte)0xc0, (byte)0x20, (byte)0xa0, (byte)0x60, (byte)0xe0,
			(byte)0x10, (byte)0x90, (byte)0x50, (byte)0xd0, (byte)0x30, (byte)0xb0, (byte)0x70, (byte)0xf0,
			(byte)0x08, (byte)0x88, (byte)0x48, (byte)0xc8, (byte)0x28, (byte)0xa8, (byte)0x68, (byte)0xe8,
			(byte)0x18, (byte)0x98, (byte)0x58, (byte)0xd8, (byte)0x38, (byte)0xb8, (byte)0x78, (byte)0xf8,
			(byte)0x04, (byte)0x84, (byte)0x44, (byte)0xc4, (byte)0x24, (byte)0xa4, (byte)0x64, (byte)0xe4,
			(byte)0x14, (byte)0x94, (byte)0x54, (byte)0xd4, (byte)0x34, (byte)0xb4, (byte)0x74, (byte)0xf4,
			(byte)0x0c, (byte)0x8c, (byte)0x4c, (byte)0xcc, (byte)0x2c, (byte)0xac, (byte)0x6c, (byte)0xec,
			(byte)0x1c, (byte)0x9c, (byte)0x5c, (byte)0xdc, (byte)0x3c, (byte)0xbc, (byte)0x7c, (byte)0xfc,
			(byte)0x02, (byte)0x82, (byte)0x42, (byte)0xc2, (byte)0x22, (byte)0xa2, (byte)0x62, (byte)0xe2,
			(byte)0x12, (byte)0x92, (byte)0x52, (byte)0xd2, (byte)0x32, (byte)0xb2, (byte)0x72, (byte)0xf2,
			(byte)0x0a, (byte)0x8a, (byte)0x4a, (byte)0xca, (byte)0x2a, (byte)0xaa, (byte)0x6a, (byte)0xea,
			(byte)0x1a, (byte)0x9a, (byte)0x5a, (byte)0xda, (byte)0x3a, (byte)0xba, (byte)0x7a, (byte)0xfa,
			(byte)0x06, (byte)0x86, (byte)0x46, (byte)0xc6, (byte)0x26, (byte)0xa6, (byte)0x66, (byte)0xe6,
			(byte)0x16, (byte)0x96, (byte)0x56, (byte)0xd6, (byte)0x36, (byte)0xb6, (byte)0x76, (byte)0xf6,
			(byte)0x0e, (byte)0x8e, (byte)0x4e, (byte)0xce, (byte)0x2e, (byte)0xae, (byte)0x6e, (byte)0xee,
			(byte)0x1e, (byte)0x9e, (byte)0x5e, (byte)0xde, (byte)0x3e, (byte)0xbe, (byte)0x7e, (byte)0xfe,
			(byte)0x01, (byte)0x81, (byte)0x41, (byte)0xc1, (byte)0x21, (byte)0xa1, (byte)0x61, (byte)0xe1,
			(byte)0x11, (byte)0x91, (byte)0x51, (byte)0xd1, (byte)0x31, (byte)0xb1, (byte)0x71, (byte)0xf1,
			(byte)0x09, (byte)0x89, (byte)0x49, (byte)0xc9, (byte)0x29, (byte)0xa9, (byte)0x69, (byte)0xe9,
			(byte)0x19, (byte)0x99, (byte)0x59, (byte)0xd9, (byte)0x39, (byte)0xb9, (byte)0x79, (byte)0xf9,
			(byte)0x05, (byte)0x85, (byte)0x45, (byte)0xc5, (byte)0x25, (byte)0xa5, (byte)0x65, (byte)0xe5,
			(byte)0x15, (byte)0x95, (byte)0x55, (byte)0xd5, (byte)0x35, (byte)0xb5, (byte)0x75, (byte)0xf5,
			(byte)0x0d, (byte)0x8d, (byte)0x4d, (byte)0xcd, (byte)0x2d, (byte)0xad, (byte)0x6d, (byte)0xed,
			(byte)0x1d, (byte)0x9d, (byte)0x5d, (byte)0xdd, (byte)0x3d, (byte)0xbd, (byte)0x7d, (byte)0xfd,
			(byte)0x03, (byte)0x83, (byte)0x43, (byte)0xc3, (byte)0x23, (byte)0xa3, (byte)0x63, (byte)0xe3,
			(byte)0x13, (byte)0x93, (byte)0x53, (byte)0xd3, (byte)0x33, (byte)0xb3, (byte)0x73, (byte)0xf3,
			(byte)0x0b, (byte)0x8b, (byte)0x4b, (byte)0xcb, (byte)0x2b, (byte)0xab, (byte)0x6b, (byte)0xeb,
			(byte)0x1b, (byte)0x9b, (byte)0x5b, (byte)0xdb, (byte)0x3b, (byte)0xbb, (byte)0x7b, (byte)0xfb,
			(byte)0x07, (byte)0x87, (byte)0x47, (byte)0xc7, (byte)0x27, (byte)0xa7, (byte)0x67, (byte)0xe7,
			(byte)0x17, (byte)0x97, (byte)0x57, (byte)0xd7, (byte)0x37, (byte)0xb7, (byte)0x77, (byte)0xf7,
			(byte)0x0f, (byte)0x8f, (byte)0x4f, (byte)0xcf, (byte)0x2f, (byte)0xaf, (byte)0x6f, (byte)0xef,
			(byte)0x1f, (byte)0x9f, (byte)0x5f, (byte)0xdf, (byte)0x3f, (byte)0xbf, (byte)0x7f, (byte)0xff
	};

	private void createTable () throws AccumuloException,AccumuloSecurityException{
		if(connection == null)
			connection = new AccumuloConnection(super.connProps);
		if(!connection.tableExist(super.tableName)) {
			connection.createTable(super.tableName);
		}
	}
}
