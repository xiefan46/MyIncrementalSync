package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.MultiFileInputStream;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.Table;
import com.alibaba.middleware.race.sync.util.RecordMap;

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
	private int				parseThreadNum	= 8;
	private int				blockSize = (int) (1024 * 1024 * 4);
	private RecordMap			recordMap;
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
		recordMap = new RecordMap(endId - startId, startId,table.getColumnSize());
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

	public byte[] getRecord(int pk) {
		return recordMap.getResult(pk);
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
	
	public RecordMap getRecordMap() {
		return recordMap;
	}

}
