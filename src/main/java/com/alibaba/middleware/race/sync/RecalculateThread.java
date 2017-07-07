package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.alibaba.middleware.race.sync.util.MyList;
import com.carrotsearch.hppc.IntObjectHashMap;

/**
 * Created by xiefan on 6/16/17.
 */
public class RecalculateThread extends WorkThread implements Constants {

	private IntObjectHashMap<byte[]>	recordMap;

	private Table				table;

	private MyList<RecordLog>	task;

	private MainThread			mainThread;

	public RecalculateThread(Context context, IntObjectHashMap<byte[]> recordMap, MyList<RecordLog> task,
			int i) {
		super("recal-", i);
		this.table = context.getTable();
		this.recordMap = recordMap;
		this.task = task;
		this.mainThread = context.getMainThread();
	}

	@Override
	protected void work() throws Exception {
		IntObjectHashMap<byte []> recordMap = this.recordMap;
		Table table = this.table;
		MyList<RecordLog> task = this.task;
		int limit = task.getPos();
		for (int i = 0; i < limit; i++) {
			received(table, recordMap, task.get(i));
		}
		setWork(false);
		mainThread.recalDone(getIndex());
	}

	public IntObjectHashMap<byte []> getRecords() {
		return recordMap;
	}

	public void received(Table table, IntObjectHashMap<byte []> recordMap, RecordLog r)
			throws Exception {
		int pk = r.getPk();
		byte alterType = r.getAlterType();
		if (alterType == UPDATE) {
			update(recordMap.get(pk), r);
		}else if(alterType == PK_UPDATE){
			int beforeValue = r.getBeforePk();
			byte[] oldRecord = recordMap.remove(beforeValue);
			update(oldRecord, r);
			recordMap.put(pk, oldRecord);
		}else if(alterType == INSERT){
			recordMap.put(pk, update(table.newRecord(), r));
		}else if(alterType == DELETE){
			recordMap.remove(pk);
		}
	}

	private byte[] update(byte[] oldRecord, RecordLog recordLog) {
		byte[] cols = recordLog.getColumns();
		for (int i = 0; i < recordLog.getEdit(); i++) {
			int off = i * 8;
			byte name = cols[off++];
			int len = cols[off++];
			RecordLog.setColumn(oldRecord, name, name, cols, off, len);
		}
		return oldRecord;
	}

}
