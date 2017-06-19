package com.alibaba.middleware.race.sync.model;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.sync.codec.ByteArray;

/**
 * @author wangkai
 *
 */
public class Table {

	private Map<ByteArray, Integer>	columnIndexs	= new HashMap<>();

	private Map<Integer, byte[]>		indexToName;

	private int					columnSize;

	public Record newRecord() {
		return new Record(columnSize);
	}

	public int getIndex(ByteArray array) {
		return columnIndexs.get(array);
	}

	public byte[] getNameByIndex(int index) {
		return indexToName.get(index);
	}

	public static Table newTable(RecordLog recordLog) {
		int index = 0;
		Table t = new Table();
		t.columnSize = recordLog.getEdit();
		t.indexToName	= new HashMap<>(t.columnSize << 1);
		for (int i = 0; i < recordLog.getEdit(); i++) {
			ColumnLog c = recordLog.getColumn(i);
			byte [] name = c.getName();
			t.indexToName.put(index, name);
			t.columnIndexs.put(new ByteArray(name), index++);
			
		}
		return t;
	}

	public int getColumnSize() {
		return columnSize;
	}

}
