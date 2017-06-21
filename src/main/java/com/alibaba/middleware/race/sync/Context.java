package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangkai
 *
 */
public class Context {

	private int					endId;

	private RecordLogReceiver		receiver;

	private int					startId;

	private String					tableSchema;

	private boolean				executeByCoreProcesses	= false;

	private Table					table;

	private int					availableProcessors		= Runtime.getRuntime()
			.availableProcessors() - 2;

	private Map<Integer, Record> records = new HashMap<>((int)(1024 * 256 * 1.5));

	public Context(int endId, RecordLogReceiver receiver, int startId, String tableSchema) {
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


	public void setReceiver(RecordLogReceiver receiver) {
		this.receiver = receiver;
	}

	public void setTableSchema(String tableSchema) {
		this.tableSchema = tableSchema;
	}

	public int getEndId() {
		return endId;
	}

	public void setEndId(int endId) {
		this.endId = endId;
	}

	public int getStartId() {
		return startId;
	}

	public void setStartId(int startId) {
		this.startId = startId;
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
	}

	public Map<Integer, Record> getRecords() {
		return records;
	}

	public void setRecords(Map<Integer, Record> records) {
		this.records = records;
	}
}
