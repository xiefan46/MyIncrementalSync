package com.alibaba.middleware.race.sync;

import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.sync.common.BufferPool;
import com.alibaba.middleware.race.sync.common.HashPartitioner;
import com.alibaba.middleware.race.sync.common.RangePartitioner;
import com.alibaba.middleware.race.sync.common.SegmentPool;
import com.alibaba.middleware.race.sync.model.Column;

/**
 * Created by xiefan on 6/24/17.
 */
public class Context {

	public static final int			READ_BUFFER_POOL_SIZE	= 128;

	private static volatile Context	INSTANCE				= new Context();

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
	private BufferPool			readBufferPool		= new BufferPool(READ_BUFFER_POOL_SIZE,
			Config.READ_BUFFER_SIZE, true);
	private SegmentPool			segmentPool		= null;							//new SegmentPool();

	private volatile Socket		client;

	private Map<String, Column>	columnMap			= new HashMap<>();
	private List<Column>		columnList		= new ArrayList<>();
	private List<byte[]>		columnByteList		= new ArrayList<>();

	public int				RECORD_SIZE;

	private Context() {

		columnList.add(new Column(0, true));
		columnList.add(new Column(1, false));
		columnList.add(new Column(2, false));
		columnList.add(new Column(3, false));
		columnList.add(new Column(4, true));
		if (!Constants.DEBUG)
			columnList.add(new Column(5, true));

		columnMap.put("id", new Column(0, true));
		columnMap.put("first_name", new Column(1, false));
		columnMap.put("last_name", new Column(2, false));
		columnMap.put("sex", new Column(3, false));
		columnMap.put("score", new Column(4, true));
		if (!Constants.DEBUG)
			columnMap.put("score2", new Column(5, true));

		columnByteList.add("id".getBytes());
		columnByteList.add("firse_name".getBytes());
		columnByteList.add("last_name".getBytes());
		columnByteList.add("sex".getBytes());
		columnByteList.add("score".getBytes());
		if (!Constants.DEBUG)
			columnByteList.add("score2".getBytes());

		RECORD_SIZE = (columnList.size() - 1) * 8;
	}

	public void initQuery(String schema, String table, long startPk, long endPk) {
		this.startPk = startPk;
		this.endPk = endPk;
		this.schema = schema;
		this.table = table;
		rangePartitioner = new RangePartitioner(startPk, endPk, Config.INRANGE_REPLAYER_COUNT);
	}

	public String getSchema() {
		return schema;
	}

	public String getTable() {
		return table;
	}

	public long getStartPk() {
		return startPk;
	}

	public long getEndPk() {
		return endPk;
	}

	public List<Column> getColumnList() {
		return columnList;
	}

	public long getCostTime() {
		return System.currentTimeMillis() - startTime;
	}

	public BufferPool getReadBufferPool() {
		return readBufferPool;
	}

	public Map<String, Column> getColumnMap() {
		return columnMap;
	}

	public Socket getClient() {
		return client;
	}

	public void setClient(Socket client) {
		this.client = client;
	}

	public SegmentPool getSegmentPool() {
		return segmentPool;
	}

	public RangePartitioner getRangePartitioner() {
		return rangePartitioner;
	}

	public HashPartitioner getHashPartitioner() {
		return hashPartitioner;
	}

	public List<byte[]> getColumnByteList() {
		return columnByteList;
	}
}
