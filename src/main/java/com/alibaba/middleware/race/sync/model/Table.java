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

	private Map<ByteArray, Integer>	columnIndexs	= new HashMap<>();

	private int					columnSize;

	public Record newRecord() {
		return new Record(columnSize);
	}

	public int getIndex(ByteArray2 array) {
		return columnIndexs.get(array);
	}

	public static Table newTable(RecordLog recordLog) {
		int index = 0;
		Table t = new Table();
		t.columnSize = recordLog.getEdit();
		for (int i = 0; i < recordLog.getEdit(); i++) {
			FullColumnLog c = (FullColumnLog) recordLog.getColumn(i);
			byte [] name = c.getNameBytes();
			t.columnIndexs.put(new ByteArray(name), index++);
			
		}
		return t;
	}

	public int getColumnSize() {
		return columnSize;
	}

}
