package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangkai
 *
 */
public class Context {

	private long				endId;

	private RecordLogReceiver	receiver;

	private long				startId;

	private String				tableSchema;

	private Table				table;

	private int				availableProcessors	= Runtime.getRuntime().availableProcessors()
			- 2;

	private Map<Long, byte[][]>	records			= new HashMap<>((int) (1024 * 256 * 1.5));

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

	public int getAvailableProcessors() {
		return availableProcessors;
	}

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public Map<Long, byte[][]> getRecords() {
		return records;
	}

	public void setRecords(Map<Long, byte[][]> records) {
		this.records = records;
	}
}
