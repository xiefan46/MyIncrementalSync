package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.sync.model.Record;

/**
 * 
 * @author wangkai
 *
 */
public class Context {

	private ReadChannel			channel;
	private int				endId;
	private RecordLogReceiver	receiver;
	private Map<Long, Record>	records	= new HashMap<>();
	private int				startId;
	private String				tableSchema;
	
	public Context(ReadChannel channel, int endId, RecordLogReceiver receiver, int startId,
			String tableSchema) {
		this.channel = channel;
		this.endId = endId;
		this.receiver = receiver;
		this.startId = startId;
		this.tableSchema = tableSchema;
	}

	public ReadChannel getChannel() {
		return channel;
	}

	public int getEndId() {
		return endId;
	}

	public RecordLogReceiver getReceiver() {
		return receiver;
	}

	public Map<Long, Record> getRecords() {
		return records;
	}

	public int getStartId() {
		return startId;
	}

	public String getTableSchema() {
		return tableSchema;
	}

	public void initialize() {

	}

	public void setChannel(ReadChannel channel) {
		this.channel = channel;
	}

	public void setEndId(int endId) {
		this.endId = endId;
	}

	public void setReceiver(RecordLogReceiver receiver) {
		this.receiver = receiver;
	}

	public void setRecords(Map<Long, Record> records) {
		this.records = records;
	}

	public void setStartId(int startId) {
		this.startId = startId;
	}

	public void setTableSchema(String tableSchema) {
		this.tableSchema = tableSchema;
	}

}
