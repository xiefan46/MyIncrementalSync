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
	
	private int[]					columnSkip;

	public Record newRecord() {
		Record record = new Record();
		record.newCols(columnSize);
		return record;
	}

	public byte getIndex(ByteArray2 array) {
		return columnIndexs.get(array);
	}

	public static Table newTable(String []cols) {
		byte index = 0;
		Table t = new Table();
		t.columnSize = cols.length;
		t.columnSkip = new int[cols.length];
		for (int i = 0; i < cols.length; i++) {
			byte [] name = cols[i].getBytes();
			t.columnSkip[i] = name.length + 11;
			t.columnIndexs.put(new ByteArray(name), index++);
		}
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

	public int[] getColumnSkip() {
		return columnSkip;
	}
	
}
