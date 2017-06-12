package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.alibaba.middleware.race.sync.store.H2KVStoreProvider;
import com.alibaba.middleware.race.sync.store.KVStoreProvider;
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

	private KVStoreProvider			provider	= new H2KVStoreProvider();

	private Map<Long, Record>		records;

	public RecalculateContext(Context context, RecordLogReceiver recordLogReceiver,
			BlockingQueue<RecordLog> recordLogs) {
		this.recordLogReceiver = recordLogReceiver;
		this.recordLogs = recordLogs;
		this.records = provider.provide();
	}

	public RecordLogReceiver getRecordLogReceiver() {
		return recordLogReceiver;
	}

	public Map<Long, Record> getRecords() {
		return records;
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
