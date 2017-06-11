package com.alibaba.middleware.race.sync;

import java.util.Map;

import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.PrimaryColumnLog;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordLogReceiverImpl implements RecordLogReceiver {

	@Override
	public void received(Context context, RecordLog recordLog) throws Exception {
		Map<Long, Record> records = context.getRecords();
		PrimaryColumnLog pcl = recordLog.getPrimaryColumn();
		Table table = context.getTable();
		Long pk = pcl.getLongValue();
		switch (recordLog.getAlterType()) {
		case Constants.UPDATE:
			if (pcl.isPkChange()) {
				Long beforeValue = pcl.getBeforeValue();
				Record oldRecord = records.remove(beforeValue);
				if (oldRecord == null) {
					oldRecord = table.newRecord();
					oldRecord.setAlterType(Constants.UPDATE);
				}
				update(table, oldRecord, recordLog);
				if (!oldRecord.isPkUpdate()) {
					oldRecord.setPkUpdate(true);
					oldRecord.setOldPk(recordLog.getPrimaryColumn().getBeforeValue());
				}
				records.put(pk, oldRecord);
			} else {
				Record oldRecord = records.get(pk);
				if (oldRecord == null) {
					oldRecord = table.newRecord();
					oldRecord.setAlterType(Constants.UPDATE);
					records.put(pk, oldRecord);
				}
				update(table, oldRecord, recordLog);
			}
			break;
		case Constants.DELETE:
			Record r = records.get(pk);
			if (r == null) {
				r = table.newRecord();
				records.put(pk, r);
			}
			r.setAlterType(Constants.DELETE);
			break;
		case Constants.INSERT:
			Record record = update(table, table.newRecord(), recordLog);
			record.setAlterType(Constants.INSERT);
			records.put(pk, record);
			break;
		default:
			break;
		}
	}

	@Override
	public void receivedFinal(Context context, long pk, Record record) throws Exception {
		Map<Long, Record> records = context.getRecords();
		switch (record.getAlterType()) {
		case Constants.UPDATE:
			if (record.isPkUpdate()) {
				Long beforeValue = record.getOldPk();
				Record oldRecord = records.remove(beforeValue);
				update(oldRecord, record);
				records.put(pk, oldRecord);
			} else {
				Record oldRecord = records.get(pk);
				update(oldRecord, record);
			}
			break;
		case Constants.DELETE:
			records.remove(pk);
			break;
		case Constants.INSERT:
			records.put(pk, update(context.getTable().newRecord(), record));
			break;
		default:
			break;
		}
	}

	private Record update(Table table, Record oldRecord, RecordLog recordLog) {
		if (recordLog.getColumns() != null) {
			for (ColumnLog c : recordLog.getColumns()) {
				oldRecord.setColum(table.getIndex(c.getName()), c.getValue());
			}
		}

		return oldRecord;
	}

	private Record update(Record oldRecord, Record newRecord) {
		checkState(oldRecord.getColumns().length == newRecord.getColumns().length, "新旧记录列数不一致");
		for (int i = 0; i < oldRecord.getColumns().length; i++) {
			byte[] value = newRecord.getColumns()[i];
			if (value != null) {
				oldRecord.setColum(i, value);
			}
		}
		return oldRecord;
	}

}
