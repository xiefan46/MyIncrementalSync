package com.alibaba.middleware.race.sync.model;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.sync.codec.ByteArray;
import com.alibaba.middleware.race.sync.codec.ByteArray2;

/**
 * @author wangkai
 *
 */
public class Table {

	private Map<ByteArray, Byte>	columnIndexs	= new HashMap<>();

	private int				columnSize;

	private int[]				columnNameSkip;
	
	private int[]				columnValueSkip;
	
	private int				tableSchemaLen;

	private int				delSkip;

	private byte[]				tableSchemaBytes;

	public byte [] newRecord() {
		return new byte [columnSize * 8];
	}

	public byte getIndex(ByteArray2 array) {
		return columnIndexs.get(array);
	}

	public static Table newTable(byte [] tableSchemaBytes,byte [] []cols,int []columnValueSkip) {
		byte index = 0;
		Table t = new Table();
		t.columnValueSkip = columnValueSkip;
		t.tableSchemaBytes = tableSchemaBytes;
		t.tableSchemaLen = tableSchemaBytes.length + 2;
		t.columnSize = cols.length;
		t.columnNameSkip = new int[cols.length];
		for (int i = 0; i < cols.length; i++) {
			byte [] name = cols[i];
			t.columnNameSkip[i] = name.length + 10;
			t.columnIndexs.put(new ByteArray(name), index++);
			t.delSkip = t.delSkip + t.columnNameSkip[i] + 1 + columnValueSkip[i];
		}
		return t;
	}
	
	public static Table newOffline(){
		return newTable(
				new byte[]{'m','i','d','d','l','e','w','a','r','e','3','|','s','t','u','d','e','n','t'}, 
				new byte [][]{
					new byte []{'f','i','r','s','t','_','n','a','m','e'},
					new byte []{'l','a','s','t','_','n','a','m','e'},
					new byte []{'s','e','x'},
					new byte []{'s','c','o','r','e'}},
				new int[]{3,3,3,1});
	}
	
	public static Table newOnline(){
		return newTable(
				new byte[]{'m','i','d','d','l','e','w','a','r','e','8','|','s','t','u','d','e','n','t'}, 
				new byte [][]{
					new byte []{'f','i','r','s','t','_','n','a','m','e'},
					new byte []{'l','a','s','t','_','n','a','m','e'},
					new byte []{'s','e','x'},
					new byte []{'s','c','o','r','e'},
					new byte []{'s','c','o','r','e','2'}},
				new int[]{3,3,3,1,1});
	}
	
	//first_name:2:0|NULL|彭|last_name:2:0|NULL|恬|sex:2:0|NULL|男|score:1:0|NULL|479|score2:1:0|NULL|159370|

	public int getColumnSize() {
		return columnSize;
	}
	
	public int[] getColumnNameSkip() {
		return columnNameSkip;
	}

	public int getDelSkip() {
		return delSkip;
	}
	
	public byte[] getTableSchemaBytes() {
		return tableSchemaBytes;
	}
	
	/**
	 * @return the tableSchemaLen
	 */
	public int getTableSchemaLen() {
		return tableSchemaLen;
	}
	
	/**
	 * @return the columnValueSkip
	 */
	public int[] getColumnValueSkip() {
		return columnValueSkip;
	}
	
}
