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

	private int					columnSize;

	private ThreadLocal<ByteArray>	byteArrayLogal	= new ThreadLocal<ByteArray>() {
												public ByteArray initialValue() {
													return new ByteArray(null);
												}
											};

	public Record newRecord() {
		return new Record(columnSize);
	}

	public int getIndex(byte[] name) {
		if (name == null) {
			System.out.println("null name");
		}
		return columnIndexs.get(byteArrayLogal.get().reset(name));
	}

	public static Table newTable(RecordLog recordLog) {
		int index = 0;
		Table t = new Table();
		t.columnSize = recordLog.getColumns().size();
		for (ColumnLog c : recordLog.getColumns()) {
			byte[] nameB = c.getName();
			String name = new String(nameB, 0, nameB.length);
			t.columnIndexs.put(new ByteArray(c.getName()), index++);
		}
		return t;
	}

	public int getColumnSize() {
		return columnSize;
	}

}
