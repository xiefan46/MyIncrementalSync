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

	 private Map<Integer, long[]> records = new HashMap<>(1024 * 1024 * 1);
//	 private Map<Integer, long[]> records = new ShardMap<>(2,1024 * 1024 * 8);

	public RecordLogReceiver getRecordLogReceiver() {
		return recordLogReceiver;
	}

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
