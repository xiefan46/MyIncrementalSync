package com.alibaba.middleware.race.sync;

import java.util.Map;

import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.NumberColumnLog;
import com.alibaba.middleware.race.sync.model.PrimaryColumnLog;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.StringColumnLog;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordLogReceiverImpl implements RecordLogReceiver {

	@Override
	public void received(Context context, RecordLog recordLog) throws Exception {
		Map<Integer, Record> records = context.getRecords();
		PrimaryColumnLog pcl = recordLog.getPrimaryColumn();
		Table table = context.getTable();
		Integer pk = pcl.getLongValue();
		switch (recordLog.getAlterType()) {
		case Constants.UPDATE:
			if (pcl.isPkChange()) {
				Integer beforeValue = pcl.getBeforeValue();
				Record oldRecord = records.remove(beforeValue);
				update(table, oldRecord, recordLog);
				records.put(pk, oldRecord);
				break;
			}
			update(table, records.get(pk), recordLog);
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

	private Record update(Table table, Record oldRecord, RecordLog recordLog) {
		for (ColumnLog c : recordLog.getColumns()) {
			if (c.isNumberCol()) {
				NumberColumnLog numberColumnLog = (NumberColumnLog) c;
				oldRecord.getNumberCols()[numberColumnLog.getNameIndex()] = numberColumnLog
						.getValue();
			} else {
				StringColumnLog stringColumnLog = (StringColumnLog) c;
				oldRecord.getStrCols()[stringColumnLog.getNameIndex()] = stringColumnLog
						.getId();
			}

		}
		return oldRecord;
	}

}
