package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 *
 */
public class Context {

	private long				endId;

	private long				startId;

	private String				tableSchema;

	private Table				table;

	private ReadChannel			channel;

	private ReadRecordLogThread	readRecordLogThread = new ReadRecordLogThread();

	private int				availableProcessors	= (Runtime.getRuntime().availableProcessors() - 0) / 2;

	private Dispatcher			dispatcher		= new Dispatcher(this);
	
	private int 				ringBufferSize		= (int) (1024 * 1024 * 4);

	public Context(long endId, long startId, String tableSchema) {
		this.endId = endId;
		this.startId = startId;
		this.tableSchema = tableSchema;
		setTable(Table.newOnline());
	}

	public String getTableSchema() {
		return tableSchema;
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
		this.dispatcher.setTable(table);
	}

	public Dispatcher getDispatcher() {
		return dispatcher;
	}

	public ReadChannel getChannel() {
		return channel;
	}

	public void setChannel(ReadChannel channel) {
		this.channel = channel;
	}

	public long [] getRecord(int id) {
		return dispatcher.getRecord(id);
	}

	public ReadRecordLogThread getReadRecordLogThread() {
		return readRecordLogThread;
	}
	
	/**
	 * @return the ringBufferSize
	 */
	public int getRingBufferSize() {
		return ringBufferSize;
	}

}
