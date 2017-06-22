package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.sync.model.Table;
import com.alibaba.middleware.race.sync.util.collection.ShardMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * @author wangkai
 *
 */
public class RecalculateContext {

	private RecordLogReceiver		recordLogReceiver;

	private Context				context;
	
	private Table					table;

	public RecalculateContext(Context context, RecordLogReceiver recordLogReceiver) {
		this.recordLogReceiver = recordLogReceiver;
	}

	 private Map<Integer, byte[][]> records = new HashMap<>();
	 //private Map<Integer, byte[][]> records = new ShardMap<>(2,16);
	 private Int2ObjectMap<byte[][]> fastRecords=new Int2ObjectOpenHashMap();

	public Int2ObjectMap<byte[][]> getFastRecords() {
		return fastRecords;
	}

	public void setFastRecords(Int2ObjectMap<byte[][]> fastRecords) {
		this.fastRecords = fastRecords;
	}

	//private Int
	public RecordLogReceiver getRecordLogReceiver() {
		return recordLogReceiver;
	}

	public Map<Integer, byte[][]> getRecords() {
		return records;
	}

	public Context getContext() {
		return context;
	}

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}
	
}
