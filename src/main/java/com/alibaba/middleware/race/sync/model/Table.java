package com.alibaba.middleware.race.sync.model;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.sync.codec.ByteArray;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

/**
 * @author wangkai
 *
 */
public class Table {
	
	private static Logger logger = LoggerFactory.getLogger(Table.class);
	
	private Map<ByteArray, Integer> columnIndexs = new HashMap<>();
	
	private int columnSize;
	
	private ByteArray byteArray = new ByteArray(null);
	
	public Record newRecord() {
		return new Record(columnSize);
	}
	
	public int getIndex(byte [] name){
		return columnIndexs.get(byteArray.reset(name));
	}
	
	public static Table newTable(RecordLog recordLog){
		logger.info("new table....................");
		int index = 1;
		Table t = new Table();
		t.columnSize = recordLog.getColumns().size() + 1;
		for(ColumnLog c : recordLog.getColumns()){
			t.columnIndexs.put(new ByteArray(c.getName()), index++);
		}
		return t;
	}
	
}
