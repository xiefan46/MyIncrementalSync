package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.channel.MuiltFileInputStream;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 *
 */
public class Context {

	private long				endId;
	private long				startId;
	private MuiltFileInputStream	readChannel;
	private Table				table;
	private ReaderThread		readerThread	= new ReaderThread(this);
	private int				recalThreadNum	= 8;
	private int				parseThreadNum	= 2;
	private int				blockSize		= (int) (1024 * 1024 * 16);
	private Dispatcher			dispatcher;

	public Context(long endId, long startId) {
		this.endId = endId;
		this.startId = startId;
	}

	public void initialize() {
		setTable(Table.newOnline());
		dispatcher = new Dispatcher(this);
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

	public int getRecalThreadNum() {
		return recalThreadNum;
	}

	public int getParseThreadNum() {
		return parseThreadNum;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public ReaderThread getReaderThread() {
		return readerThread;
	}
	
	public Dispatcher getDispatcher() {
		return dispatcher;
	}

	public byte[] getRecord(int i) {
		return dispatcher.getRecord(i);
	}
	
}
