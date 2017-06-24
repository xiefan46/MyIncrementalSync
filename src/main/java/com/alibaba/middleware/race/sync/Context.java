package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.map.ArrayHashMap;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 *
 */
public class Context {

	private long				endId;
	private RecordLogReceiver	receiver;
	private long				startId;
	private String				tableSchema;
	private ReadChannel			readChannel;
	private boolean			executeByCoreProcesses	= false;
	private Table				table;
	private int				availableProcessors		= Runtime.getRuntime()
			.availableProcessors() - 2;

	private ArrayHashMap		recordMap;

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
		this.table = Table.newOnline();
		this.recordMap = new ArrayHashMap(table);
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

	public ReadChannel getReadChannel() {
		return readChannel;
	}

	public void setReadChannel(ReadChannel readChannel) {
		this.readChannel = readChannel;
	}

	public ArrayHashMap getRecordMap() {
		return recordMap;
	}

	public void setRecordMap(ArrayHashMap recordMap) {
		this.recordMap = recordMap;
	}
}
