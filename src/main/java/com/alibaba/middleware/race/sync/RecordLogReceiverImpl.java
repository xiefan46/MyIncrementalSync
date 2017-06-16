package com.alibaba.middleware.race.sync;

import java.util.Map;

import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.Constants;
import com.alibaba.middleware.race.sync.model.PrimaryColumnLog;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * Created by xiefan on 6/4/17.
 */
@Deprecated
public class RecordLogReceiverImpl implements RecordLogReceiver {

	@Override
	public void received(Context context, RecordLog recordLog) throws Exception {
		Map<Long, byte[][]> records = context.getRecords();
		PrimaryColumnLog pcl = recordLog.getPrimaryColumn();
		Table table = context.getTable();
		Long pk = pcl.getLongValue();
		switch (recordLog.getAlterType()) {
		case Constants.UPDATE:
			if (pcl.isPkChange()) {
				Long beforeValue = pcl.getBeforeValue();
				byte[][] oldRecord = records.remove(beforeValue);
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

	private byte[][] update(Table table, byte[][] oldRecord, RecordLog recordLog) {
		for (ColumnLog c : recordLog.getColumns()) {
			if (!c.isUpdate()) {
				break;
			}
			oldRecord[table.getIndex(c.getName())] = c.getValue();
			c.setUpdate(false);
		}
		return oldRecord;
	}

}
