package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.channel.MultiFileInputStream;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 *
 */
public class Context {

	private long				endId;
	private long				startId;
	private MultiFileInputStream	readChannel;
	private Table				table;
	private MainThread			mainThread = new MainThread(this);
	private int				recalThreadNum	= 8;
	private int				parseThreadNum	= 7;
	private Dispatcher			dispatcher;
	private ByteBufPool			byteBufPool;

	public Context(long endId, long startId) {
		this.endId = endId;
		this.startId = startId;
	}

	public void initialize() {
		setTable(Table.newOffline());
		dispatcher = new Dispatcher(this);
		byteBufPool = new ByteBufPool(parseThreadNum * 2, (int) (1024 * 1024 * 4));
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

	public MultiFileInputStream getReadChannel() {
		return readChannel;
	}

	public void setReadChannel(MultiFileInputStream readChannel) {
		this.readChannel = readChannel;
	}

	public int getRecalThreadNum() {
		return recalThreadNum;
	}

	public int getParseThreadNum() {
		return parseThreadNum;
	}

	public MainThread getMainThread() {
		return mainThread;
	}

	public Dispatcher getDispatcher() {
		return dispatcher;
	}

	public byte[] getRecord(int i) {
		return dispatcher.getRecord(i);
	}

	/**
	 * @return the byteBufPool
	 */
	public ByteBufPool getByteBufPool() {
		return byteBufPool;
	}

}
