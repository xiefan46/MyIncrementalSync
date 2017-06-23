package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 *
 */
public class RecalculateContext {

	private RecordLogReceiver		recordLogReceiver;

	private Context				context;
	
	private Table					table;

	public RecalculateContext(Context context, RecordLogReceiver recordLogReceiver) {
		this.context = context;
		this.recordLogReceiver = recordLogReceiver;
	}

	
//	private ShardMap2<long[]> records = new ShardMap2<>(4,1024 * 16);
//	private IntObjectHashMap<long[]> records = new IntObjectHashMap<>(1024 * 1024 * 1);
	 private final Map<Integer, long[]> records = new HashMap<>(1024 * 1024 * 1);
//	 private Map<Integer, long[]> records = new ShardMap<>(2,1024 * 1024 * 8);

	public RecordLogReceiver getRecordLogReceiver() {
		return recordLogReceiver;
	}

//	public ShardMap2<long[]> getRecords() {
//		return records;
//	}
	
//	public IntObjectHashMap<long[]> getRecords() {
//		return records;
//	}

	public Map<Integer, long[]> getRecords() {
		return records;
	}
	
	public long [] getRecord(int id) {
		return records.get(id);
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
