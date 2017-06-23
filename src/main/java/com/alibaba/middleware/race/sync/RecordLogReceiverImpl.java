package com.alibaba.middleware.race.sync;

import java.util.Map;

import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.PrimaryColumnLog;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordLogReceiverImpl implements RecordLogReceiver {

	@Override
	public void received(RecalculateContext context, RecordLog recordLog) throws Exception {
		Map<Integer, long[]> records = context.getRecords();
//		IntObjectHashMap<long[]> records = context.getRecords();
//		ShardMap2<long[]> records = context.getRecords();
		PrimaryColumnLog pcl = recordLog.getPrimaryColumn();
		Table table = context.getTable();
		int pk = pcl.getLongValue();
		byte alterType = recordLog.getAlterType();
		if (alterType == Constants.UPDATE) {
			update(table, records.get(pk), recordLog);
		}else if (alterType == Constants.PK_UPDATE) {
			int beforeValue = pcl.getBeforeValue();
			long[] oldRecord = records.remove(beforeValue);
			update(table, oldRecord, recordLog);
			records.put(pk, oldRecord);
		}else if (alterType == Constants.INSERT) {
			long [] record = update(table, table.newRecord(), recordLog);
			records.put(pk, record);
		}else if (alterType == Constants.DELETE) {
			records.remove(pk);
		}
	}

	private long[] update(Table table, long[] oldRecord, RecordLog recordLog) {
		for (int i = 0; i < recordLog.getEdit(); i++) {
			ColumnLog c = recordLog.getColumn(i);
			oldRecord[c.getName()] = c.getValue();
		}
		return oldRecord;
	}

}
