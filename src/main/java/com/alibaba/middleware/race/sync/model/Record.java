package com.alibaba.middleware.race.sync.model;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangkai
 *
 */
//FIXME 考虑合并schema-table
public class Record {

	// 一个唯一的字符串编号,例子:000001:106
//	private String			binaryId;
	// 数据变更发生的时间戳,毫秒精度,例子:12345(自1970年1月1号开始)
//	private long			timestamp;
//	// 数据变更对应的库名	
//	private String			schema;
//	// 数据变更对应的表名
//	private String			table;
	// 数据变更对应的库名-表名
	private String 			tableSchema;
	//  I(1)代表insert, U(2)代表update, D(0)代表delete
	private byte				alterType;
	// 该记录的列信息
	private Map<String, Column> 	columns;
	// 该记录的主键
	private PrimaryColumn		primaryColumn;

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

	public void addColumn(Column column) {
		columns.put(column.getName(), column);
	}

	public Map<String, Column> getColumns() {
		return columns;
	}

	public void setColumns(Map<String, Column> columns) {
		this.columns = columns;
	}
	
	public void newColumns(){
		this.columns = new HashMap<>();
	}

	public PrimaryColumn getPrimaryColumn() {
		return primaryColumn;
	}

	public void setPrimaryColumn(PrimaryColumn primaryColumn) {
		this.primaryColumn = primaryColumn;
	}

	public String getTableSchema() {
		return tableSchema;
	}

	public void setTableSchema(String tableSchema) {
		this.tableSchema = tableSchema;
	}
	
}
