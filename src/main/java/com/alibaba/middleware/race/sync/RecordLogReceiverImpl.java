package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.carrotsearch.hppc.IntObjectHashMap;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordLogReceiverImpl implements RecordLogReceiver {

	@Override
	public void received(RecalculateContext context, RecordLog recordLog) throws Exception {
//		Map<Integer, byte[]> records = context.getRecords();
		IntObjectHashMap<byte[]> records = context.getRecords();
//		ShardMap2<byte[]> records = context.getRecords();
		Table table = context.getTable();
		int pk = recordLog.getPk();
		byte alterType = recordLog.getAlterType();
		if (alterType == Constants.UPDATE) {
			update(table, records.get(pk), recordLog);
		}else if (alterType == Constants.PK_UPDATE) {
			int beforeValue = recordLog.getBeforePk();
			byte[] oldRecord = records.remove(beforeValue);
			update(table, oldRecord, recordLog);
			records.put(pk, oldRecord);
		}else if (alterType == Constants.INSERT) {
			byte [] record = update(table, table.newRecord(), recordLog);
			records.put(pk, record);
		}else if (alterType == Constants.DELETE) {
			records.remove(pk);
		}
	}

	private byte[] update(Table table, byte[] oldRecord, RecordLog recordLog) {
		byte [] cols = recordLog.getColumns();
		for (int i = 0; i < recordLog.getEdit(); i++) {
			int off = i * 8;
			byte name = cols[off++];
			int len = cols[off++];
			RecordLog.setColumn(oldRecord, name, cols, off, len);
		}
		return oldRecord;
	}

}
