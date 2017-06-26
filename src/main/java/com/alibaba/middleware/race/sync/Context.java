package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.IOException;
import java.net.Socket;

import com.alibaba.middleware.race.sync.channel.MuiltFileInputStream;
import com.alibaba.middleware.race.sync.channel.MuiltFileReadChannelSplitor;
import com.alibaba.middleware.race.sync.common.BufferPool;
import com.alibaba.middleware.race.sync.common.RangeSearcher;
import com.alibaba.middleware.race.sync.model.Table;
import com.alibaba.middleware.race.sync.service.CalculateStage;
import com.alibaba.middleware.race.sync.service.ParseStage;
import com.alibaba.middleware.race.sync.util.ByteArrayBuffer;

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

	private int				startPk;

	private int				endPk;

	private BufferPool			blockBufferPool	= new BufferPool(256, 1024 * 1024);

	private BufferPool			recordLogBufferPool	= new BufferPool(512, 512 * 1024);

	private MuiltFileInputStream	muiltFileInputStream;

	private RangeSearcher		rangeSearcher;

	private ByteArrayBuffer		resultBuffer;

	private Context() {
	}

	public void initQuery(String schema, String table, int startPk, int endPk) throws IOException {
		this.startPk = startPk;
		this.endPk = endPk;
		this.schema = schema;
		this.table = table;
		this.muiltFileInputStream = initMultiFileStream();
		this.rangeSearcher = new RangeSearcher(startPk + 1, endPk, CalculateStage.REPLAYER_COUNT);
	}

	public String getSchema() {
		return schema;
	}

	public String getTableName() {
		return table;
	}

	public int getStartPk() {
		return startPk;
	}

	public int getEndPk() {
		return endPk;
	}

	public long getCostTime() {
		return System.currentTimeMillis() - startTime;
	}

	public BufferPool getBlockBufferPool() {
		return blockBufferPool;
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

	public RangeSearcher getRangeSearcher() {
		return rangeSearcher;
	}

	public ByteArrayBuffer getResultBuffer() {
		return resultBuffer;
	}

	public void setResultBuffer(ByteArrayBuffer resultBuffer) {
		this.resultBuffer = resultBuffer;
	}
}
