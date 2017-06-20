package com.alibaba.middleware.race.sync.model;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.sync.Constants;

/**
 * @author wangkai
 */
//FIXME 考虑合并schema-table
public class RecordLog {

	// 一个唯一的字符串编号,例子:000001:106
	//	private String			binaryId;
	// 数据变更发生的时间戳,毫秒精度
	//	private long			timestamp;
	//	// 数据变更对应的库名	
	//	private String			schema;
	//	// 数据变更对应的表名
	//	private String			table;

	private int edit;
	//  I(1)代表insert, U(2)代表update, D(0)代表delete
	private volatile byte			alterType;
	// 该记录的列信息
	private List<ColumnLog>	columns;
	// 该记录的主键
	private PrimaryColumnLog	primaryColumn = new PrimaryColumnLog();

	//	public long getTimestamp() {
	//		return timestamp;
	//	}
	//
	//	public void setTimestamp(long timestamp) {
	//		this.timestamp = timestamp;
	//	}

	public byte getAlterType() {
		return alterType;
	}

	public void setAlterType(byte alterType) {
		this.alterType = alterType;
	}

	public ColumnLog getColumn() {
		return columns.get(edit++);
	}
	
	public ColumnLog getColumn(int index) {
		return columns.get(index);
	}

	public void newColumns(int cols) {
		List<ColumnLog> columns = new ArrayList<>(cols);
		for (int i = 0; i < cols; i++) {
			columns.add(new ColumnLog());
		}
		this.columns = columns;
	}

	public PrimaryColumnLog getPrimaryColumn() {
		return primaryColumn;
	}

	public void setPrimaryColumn(PrimaryColumnLog primaryColumn) {
		this.primaryColumn = primaryColumn;
	}
	
	public void reset(){
		edit = 0;
	}
	
	public int getEdit() {
		return edit;
	}

	public boolean isPKUpdate() {
		return Constants.PK_UPDATE == alterType;
	}

	public static RecordLog newRecordLog(int cols){
		RecordLog r = new RecordLog();
		r.newColumns(cols);
		return r;
	}

}
