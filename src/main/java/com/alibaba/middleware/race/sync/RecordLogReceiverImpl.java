package com.alibaba.middleware.race.sync;

import java.util.Map;

import com.alibaba.middleware.race.sync.codec.ByteArray;
import com.alibaba.middleware.race.sync.model.Column;
import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.Constants;
import com.alibaba.middleware.race.sync.model.PrimaryColumn;
import com.alibaba.middleware.race.sync.model.PrimaryColumnLog;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordLogReceiverImpl implements RecordLogReceiver {

	@Override
	public void received(RecalculateContext context, RecordLog recordLog) throws Exception {
		Map<Long, Record> records = context.getRecords();
		PrimaryColumnLog pcl = recordLog.getPrimaryColumn();
		Long pk = (Long) (pcl.getLongValue());
		switch (recordLog.getAlterType()) {
		case Constants.UPDATE:
			if (pcl.IsPkChange()) {
				Long beforeValue = (Long)(pcl.getBeforeValue());
				Record oldRecord = records.remove(beforeValue);
				update(oldRecord, recordLog);
				records.put(pk, oldRecord);
				break;
			}
			Record oldRecord = records.get(pk);
			update(oldRecord, recordLog);
			break;
		case Constants.DELETE:
			records.remove(pk);
			break;
		case Constants.INSERT:
			records.put(pk, newRecord(recordLog, recordLog.getPrimaryColumn()));
			break;
		}
	}

	private Record newRecord(RecordLog record, PrimaryColumnLog pcl) {
		Record r = new Record();
		r.newColumns();
		PrimaryColumn pc = new PrimaryColumn();
		pc.setLongValue(pcl.getLongValue());
		pc.setValue(pcl.getValue());
		r.setPrimaryColumn(pc);
		for (ColumnLog cl : record.getColumns()) {
			Column c = new Column();
			c.setValue(cl.getValue());
			r.putColumn(cl.getName(), c);
		}
		return r;
	}

	private void update(Record oldRecord, RecordLog recordLog) {
		Map<ByteArray, Column> cols = oldRecord.getColumns();
		for (ColumnLog c : recordLog.getColumns()) {
			Column oldC = cols.get(c.getName());
			oldC.setValue(c.getValue());
		}
	}

}
