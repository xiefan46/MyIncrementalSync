package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
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

	private Table				table;

	private ReadChannel			channel;

	private Map<Long, Record>	records	= new HashMap<>();

	public Context(long endId, RecordLogReceiver receiver, long startId, String tableSchema,
			ReadChannel channel, Table table) {
		this.endId = endId;
		this.receiver = receiver;
		this.startId = startId;
		this.tableSchema = tableSchema;
		this.channel = channel;
		this.table = table;
	}

	public RecordLogReceiver getReceiver() {
		return receiver;
	}

	public String getTableSchema() {
		return tableSchema;
	}

	public void initialize() {

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

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public ReadChannel getChannel() {
		return channel;
	}

	public void setChannel(ReadChannel channel) {
		this.channel = channel;
	}

	public Map<Long, Record> getRecords() {
		return records;
	}

	public void setRecords(Map<Long, Record> records) {
		this.records = records;
	}
}
