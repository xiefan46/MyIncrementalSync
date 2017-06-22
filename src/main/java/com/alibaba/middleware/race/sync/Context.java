package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.channel.MuiltFileInputStream;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 *
 */
public class Context {

	private long				endId;
	private RecordLogReceiver	receiver;
	private long				startId;
	private MuiltFileInputStream	readChannel;
	private Table				table;
	private ReaderThread		readerThread	= new ReaderThread(this);
	private int				threadNum		= 2;
	private int				blockSize		= 1024 * 1024 * 256;

	public Context(RecordLogReceiver receiver, long endId, long startId) {
		this.endId = endId;
		this.receiver = receiver;
		this.startId = startId;
	}

	public RecordLogReceiver getReceiver() {
		return receiver;
	}

	public void initialize() {
		setTable(Table.newOnline());
	}

	public void setReceiver(RecordLogReceiver receiver) {
		this.receiver = receiver;
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

	public MuiltFileInputStream getReadChannel() {
		return readChannel;
	}

	public void setReadChannel(MuiltFileInputStream readChannel) {
		this.readChannel = readChannel;
	}

	public int getThreadNum() {
		return threadNum;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public long[] getRecord(int i) {
		return null;
	}
}
