package com.alibaba.middleware.race.sync.model;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.sync.codec.ByteArray;

/**
 * @author wangkai
 *
 */
public class Table {
	
	private Map<ByteArray, Integer> columnIndexs = new HashMap<>();
	
	private ByteArray byteArray = new ByteArray(null);
	
	public Record newRecord() {
		return new Record(columnIndexs.size());
	}
	
	public int getIndex(byte [] name){
		return columnIndexs.get(byteArray.reset(name));
	}
	
	public static Table newTable(RecordLog recordLog){
		int index = 0;
		Table t = new Table();
		PrimaryColumnLog pcl = recordLog.getPrimaryColumn();
		t.columnIndexs.put(new ByteArray(pcl.getName()), index++);
		for(ColumnLog c : recordLog.getColumns()){
			t.columnIndexs.put(new ByteArray(c.getName()), index++);
		}
		return t;
	}
	
}
