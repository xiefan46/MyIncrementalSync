package com.alibaba.middleware.race.sync.model;

import com.alibaba.middleware.race.sync.Constants;

import java.util.ArrayList;
import java.util.List;

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

	//  I(1)代表insert, U(2)代表update, D(0)代表delete
	private byte			alterType;
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

	public void addColumn(ColumnLog column) {
		columns.add(column);
	}

	public void newColumns() {
		this.columns = new ArrayList<>();
	}

	public List<ColumnLog> getColumns() {
		return columns;
	}

	public PrimaryColumnLog getPrimaryColumn() {
		return primaryColumn;
	}

	public void setPrimaryColumn(PrimaryColumnLog primaryColumn) {
		this.primaryColumn = primaryColumn;
	}

	public boolean isPKUpdate() {
		if (this.alterType == Constants.UPDATE) {
			return primaryColumn.isPkChange();
		}
		return false;
	}

}
