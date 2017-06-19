package com.alibaba.middleware.race.sync.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.alibaba.middleware.race.sync.codec.ByteArray;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

/**
 * @author wangkai
 *
 */
public class Table {

	private Map<ByteArray, Integer>	columnIndexs	= new HashMap<>();

	private int					columnSize;

	private ByteArray				byteArray		= new ByteArray(null);

	public List<Set<String>>			diffValue		= new ArrayList<>();

	private static final Logger		logger		= LoggerFactory.getLogger(Table.class);

	public byte[][] newRecord() {
		return new byte[columnSize][];
	}

	public int getIndex(byte[] name) {
		return columnIndexs.get(byteArray.reset(name));
	}

	public static Table newTable(RecordLog recordLog) {
		int index = 0;
		Table t = new Table();
		t.columnSize = recordLog.getColumns().size();
		for (int i = 0; i < t.columnSize; i++) {
			t.diffValue.add(new HashSet<String>());
		}
		for (ColumnLog c : recordLog.getColumns()) {
			t.columnIndexs.put(new ByteArray(c.getName()), index++);
		}
		return t;
	}

	public int getColumnSize() {
		return columnSize;
	}

	public void statRecord(RecordLog recordLog) {
		for (ColumnLog c : recordLog.getColumns()) {
			byte[] value = c.getValue();
			if (value != null)
				diffValue.get(getIndex(c.getName())).add(new String(value, 0, value.length));
		}
	}

	public void printStat() {
		StringBuilder sb = new StringBuilder();
		sb.append("different value num : [");
		for (int i = 0; i < columnSize; i++) {
			sb.append(i + ":" + diffValue.get(i).size() + " ");
		}
		sb.append("]");
		logger.info(sb.toString());
	}

}
