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

	private int					columnSize;
	
	private int []			columnNameSkip;
	
	private int				delSkip;

	public byte [] newRecord() {
		return new byte [columnSize * 8];
	}

	public byte getIndex(ByteArray2 array) {
		return columnIndexs.get(array);
	}

	public static Table newTable(String []cols) {
		byte index = 0;
		Table t = new Table();
		t.columnSize = cols.length;
		t.columnNameSkip = new int[cols.length];
		for (int i = 0; i < cols.length; i++) {
			byte [] name = cols[i].getBytes();
			t.columnNameSkip[i] = name.length + 10;
			t.columnIndexs.put(new ByteArray(name), index++);
			t.delSkip = t.delSkip + t.columnNameSkip[i] + 2;
		}
		t.delSkip += 2;
		return t;
	}
	
	public static Table newOffline(){
		return newTable(new String[]{"first_name","last_name","sex","score"});
	}
	
	public static Table newOnline(){
		return newTable(new String[]{"first_name","last_name","sex","score","score2"});
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
}
