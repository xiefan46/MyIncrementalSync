package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.alibaba.middleware.race.sync.util.H2MVStore;
import org.h2.mvstore.MVMap;


/**
 * @author wangkai
 *
 */
public class RecalculateContext {

	private RecordLogReceiver		recordLogReceiver;

	private BlockingQueue<RecordLog>	recordLogs;

	private Context				context;
	
	private Table					table;

	public RecalculateContext(Context context, RecordLogReceiver recordLogReceiver,
			BlockingQueue<RecordLog> recordLogs) {
		this.recordLogReceiver = recordLogReceiver;
		this.recordLogs = recordLogs;
	}

	private Map<Long, Record> records = new HashMap<>();

	private MVMap<Long,Record> mvRecords= H2MVStore.getRecordMap("m3");

	public RecordLogReceiver getRecordLogReceiver() {
		return recordLogReceiver;
	}

	public Map<Long, Record> getRecords() {
		return records;
	}

	public MVMap<Long, Record> getMvRecords() {
		return mvRecords;
	}

	public BlockingQueue<RecordLog> getRecordLogs() {
		return recordLogs;
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
