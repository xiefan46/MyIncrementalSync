package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.channel.MuiltFileInputStream;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 *
 */
public class Context {

	private int				endId;
	private int				startId;
	private MuiltFileInputStream	readChannel;
	private Table				table;
	private MainThread			mainThread = new MainThread(this);
	private int				recalThreadNum	= 2;
	private int				parseThreadNum	= 2;
	private int				blockSize = (int) (1024 * 1024 * 4);
	private Dispatcher			dispatcher;
	private ByteBufPool			byteBufPool;

	public Context(long endId, long startId) {
		this.endId = (int) endId;
		this.startId = (int) startId;
	}

	public void initialize() {
		setTable(Table.newOffline());
		dispatcher = new Dispatcher(this);
		byteBufPool = new ByteBufPool(parseThreadNum * 2, blockSize);
	}

	public int getEndId() {
		return endId;
	}

	public int getStartId() {
		return startId;
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
	
	public int getBlockSize() {
		return blockSize;
	}

}
