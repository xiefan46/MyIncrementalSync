package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.IOException;
import java.net.Socket;

import com.alibaba.middleware.race.sync.channel.MuiltFileInputStream;
import com.alibaba.middleware.race.sync.channel.MuiltFileReadChannelSplitor;
import com.alibaba.middleware.race.sync.common.BufferPool;
import com.alibaba.middleware.race.sync.common.HashPartitioner;
import com.alibaba.middleware.race.sync.common.RangePartitioner;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * Created by xiefan on 6/24/17.
 */
public class Context {

	private static volatile Context INSTANCE = new Context();

	public static Context getInstance() {
		return INSTANCE;
	}

	private long				startTime			= System.currentTimeMillis();

	private String				schema;

	private String				table;

	private long				startPk;

	private long				endPk;

	private RangePartitioner		rangePartitioner;

	private HashPartitioner		hashPartitioner	= new HashPartitioner(
			Config.OUTRANGE_REPLAYER_COUNT);

	// 内存池
	private BufferPool			blockBufferPool	= new BufferPool(128, 1024 * 1024);

	private BufferPool			recordLogBufferPool	= new BufferPool(1024, 256 * 1024);

	private volatile Socket		client;

	private MuiltFileInputStream	muiltFileInputStream;

	private Context() {
	}

	public void initQuery(String schema, String table, long startPk, long endPk)
			throws IOException {
		this.startPk = startPk;
		this.endPk = endPk;
		this.schema = schema;
		this.table = table;
		rangePartitioner = new RangePartitioner(startPk, endPk, Config.INRANGE_REPLAYER_COUNT);
		this.muiltFileInputStream = initMultiFileStream();
	}

	public String getSchema() {
		return schema;
	}

	public String getTableName() {
		return table;
	}

	public long getStartPk() {
		return startPk;
	}

	public long getEndPk() {
		return endPk;
	}

	public long getCostTime() {
		return System.currentTimeMillis() - startTime;
	}

	public BufferPool getBlockBufferPool() {
		return blockBufferPool;
	}

	public Socket getClient() {
		return client;
	}

	public void setClient(Socket client) {
		this.client = client;
	}

	public RangePartitioner getRangePartitioner() {
		return rangePartitioner;
	}

	public HashPartitioner getHashPartitioner() {
		return hashPartitioner;
	}

	private MuiltFileInputStream initMultiFileStream() throws IOException {
		File root = new File(Constants.DATA_HOME);
		return MuiltFileReadChannelSplitor.newInputStream(root.getAbsolutePath() + "/", 1, 10,
				1024 * 256);
	}

	public MuiltFileInputStream getMuiltFileInputStream() {
		return muiltFileInputStream;
	}

	public void setMuiltFileInputStream(MuiltFileInputStream muiltFileInputStream) {
		this.muiltFileInputStream = muiltFileInputStream;
	}

	public Table getTable() {
		if (Constants.DEBUG) {
			return Table.newOffline();
		} else {
			return Table.newOnline();
		}
	}

	public BufferPool getRecordLogPool() {
		return recordLogBufferPool;
	}

	public void setRecordLogPool(BufferPool recordLogPool) {
		this.recordLogBufferPool = recordLogPool;
	}
}
