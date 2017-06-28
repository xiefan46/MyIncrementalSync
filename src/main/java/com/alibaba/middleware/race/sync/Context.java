package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;

import com.alibaba.middleware.race.sync.channel.MultiFileInputStream;
import com.alibaba.middleware.race.sync.model.Table;
import com.alibaba.middleware.race.sync.util.LoggerUtil;
import com.alibaba.middleware.race.sync.util.RecordMap2;

/**
 * @author wangkai
 *
 */
public class Context {

	private int				endId;
	private int				startId;
	private MultiFileInputStream	readChannel;
	private Table				table;
	private MainThread			mainThread	= new MainThread(this);
	private int				parseThreadNum	= Runtime.getRuntime().availableProcessors();
	private int				blockSize		= (int) (1024 * 1024 * 2);
	private RecordMap2			recordMap;
	private ByteBufPool			byteBufPool;
	private static final Logger	logger		= LoggerUtil.get();

	public Context(int endId, int startId) {
		this.endId = endId;
		this.startId = startId;
	}

	public void initialize() {
		//parseThreadNum = 1;
		if (Constants.ON_LINE) {
			setTable(Table.newOnline());
			logger.info("使用online模式初始化table");
		} else {
			setTable(Table.newOffline());
			logger.info("使用offline模式初始化table,提交到线上记得切换!!!!!!");
		}
		long startTime = System.currentTimeMillis();
		recordMap = new RecordMap2(endId - startId, startId, table.getColumnSize());
		logger.info("record map init:{}", (System.currentTimeMillis() - startTime));
		byteBufPool = new ByteBufPool(parseThreadNum * 8, blockSize);
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

	public int getParseThreadNum() {
		return parseThreadNum;
	}

	public MainThread getMainThread() {
		return mainThread;
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

	public RecordMap2 getRecordMap() {
		return recordMap;
	}

}
