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
	public void received(Table table,Map<Integer, long[]> records, RecordLog recordLog) throws Exception {
		PrimaryColumnLog pcl = recordLog.getPrimaryColumn();
		Integer pk = pcl.getLongValue();
		switch (recordLog.getAlterType()) {
		case Constants.UPDATE:
			update(table, records.get(pk), recordLog);
			break;
		case Constants.PK_UPDATE:
			Integer beforeValue = pcl.getBeforeValue();
			long[] oldRecord = records.remove(beforeValue);
			update(table, oldRecord, recordLog);
			records.put(pk, oldRecord);
			break;
		case Constants.DELETE:
			records.remove(pk);
			break;
		case Constants.INSERT:
			records.put(pk, update(table, table.newRecord(), recordLog));
			break;
		default:
			break;
		}
	}
	
	@Override
	public void receivedFinal(Table table, Map<Integer, long[]> records,
			Map<Integer, long[]> records2) throws Exception {
		throw new UnsupportedOperationException();
	}

	private long[] update(Table table, long[] oldRecord, RecordLog recordLog) {
		for (int i = 0; i < recordLog.getEdit(); i++) {
			ColumnLog c = recordLog.getColumn(i);
			oldRecord[c.getName()] = c.getValue();
		}
		return oldRecord;
	}

}
