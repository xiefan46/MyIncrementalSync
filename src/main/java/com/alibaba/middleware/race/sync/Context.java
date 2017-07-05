package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.channel.MultiFileInputStream;
import com.alibaba.middleware.race.sync.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wangkai
 *
 */
public class Context {

	private int				endId;
	private int				startId;
	private MultiFileInputStream	readChannel;
	private Table				table;

	private MainThread			mainThread = new MainThread(this);
	private int				coreProcesses = Runtime.getRuntime().availableProcessors();
	private int				recalThreadNum	= coreProcesses;
	private int				parseThreadNum	= coreProcesses;
	private int				blockSize = (int) (1024 * 1024 * 2);
	private Dispatcher			dispatcher;
	private ByteBufPool			byteBufPool;
	private static final Logger	logger		= LoggerFactory.getLogger(Context.class);

	public Context(long endId, long startId) {
		this.endId = (int) endId;
		this.startId = (int) startId;
	}

	public void initialize() {
		if (Constants.ON_LINE) {
			setTable(Table.newOnline());
			logger.info("使用online模式初始化table");
		} else {
			setTable(Table.newOffline());
			logger.info("使用offline模式初始化table,提交到线上记得切换!!!!!!");
		}
		dispatcher = new Dispatcher(this);
		byteBufPool = new ByteBufPool(parseThreadNum * 4, blockSize);
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
	
	public int getBlockSize() {
		return blockSize;
	}

}
