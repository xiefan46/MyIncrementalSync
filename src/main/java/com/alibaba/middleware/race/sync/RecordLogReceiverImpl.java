package com.alibaba.middleware.race.sync;

import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.Constants;
import com.alibaba.middleware.race.sync.model.PrimaryColumnLog;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordLogReceiverImpl implements RecordLogReceiver {

	@Override
	public void received(RecalculateContext context, RecordLog recordLog) throws Exception {
		//Map<Integer, byte[][]> records = context.getRecords();
		Int2ObjectMap<byte[][]> records =context.getFastRecords();
		PrimaryColumnLog pcl = recordLog.getPrimaryColumn();
		Table table = context.getTable();
		Integer pk = pcl.getLongValue();
		switch (recordLog.getAlterType()) {
		case Constants.UPDATE:
			if (pcl.isPkChange()) {
				Integer beforeValue = pcl.getBeforeValue();
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
		List<ColumnLog> columnLogs = recordLog.getColumns();
		for (int i = 0; i < recordLog.getEdit(); i++) {
			ColumnLog c = columnLogs.get(i);
			oldRecord[table.getIndex1(c.getName())] = c.getValue();
		}
		recordLog.resetEdit();
		return oldRecord;
	}

}
