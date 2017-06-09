package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;

/**
 * @author wangkai
 *
 */
public class RecalculateContext {

	private RecordLogReceiver		recordLogReceiver;

	private BlockingQueue<RecordLog>	recordLogs;

	private Context				context;

	public RecalculateContext(Context context, RecordLogReceiver recordLogReceiver,
			BlockingQueue<RecordLog> recordLogs) {
		this.recordLogReceiver = recordLogReceiver;
		this.recordLogs = recordLogs;
	}

	private Map<Long, Record> records = new HashMap<>();

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

}
