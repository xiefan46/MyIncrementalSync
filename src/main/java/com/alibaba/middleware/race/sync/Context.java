package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 *
 */
public class Context {

	private long					endId;
	private RecordLogReceiver		receiver;
	private long					startId;
	private String					tableSchema;
	private boolean				executeByCoreProcesses	= false;
	private RecalculateContext		recalculateContext;
	private Table					table;
	private int					availableProcessors		= Runtime.getRuntime()
			.availableProcessors() - 2;

	public Context(long endId, RecordLogReceiver receiver, long startId, String tableSchema) {
		this.endId = endId;
		this.receiver = receiver;
		this.startId = startId;
		this.tableSchema = tableSchema;
	}

	public RecordLogReceiver getReceiver() {
		return receiver;
	}

	public String getTableSchema() {
		return tableSchema;
	}

	public void initialize() {
		recalculateContext = new RecalculateContext(this, getReceiver());
	}

	public void setReceiver(RecordLogReceiver receiver) {
		this.receiver = receiver;
	}

	public void setTableSchema(String tableSchema) {
		this.tableSchema = tableSchema;
	}

	public long getEndId() {
		return endId;
	}

	public void setEndId(long endId) {
		this.endId = endId;
	}

	public long getStartId() {
		return startId;
	}

	public void setStartId(long startId) {
		this.startId = startId;
	}

	public RecalculateContext getRecalculateContext() {
		return recalculateContext;
	}

	public boolean isExecuteByCoreProcesses() {
		return executeByCoreProcesses;
	}
	
	public int getAvailableProcessors() {
		return availableProcessors;
	}

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
		this.recalculateContext.setTable(table);
	}
	
}
