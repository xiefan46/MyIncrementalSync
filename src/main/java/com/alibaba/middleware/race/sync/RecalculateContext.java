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
		this.recordLogReceiver = recordLogReceiver;
	}

	private Map<Integer, byte[][]> records = new HashMap<>((int)(1024 * 256 * 1.5));

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
