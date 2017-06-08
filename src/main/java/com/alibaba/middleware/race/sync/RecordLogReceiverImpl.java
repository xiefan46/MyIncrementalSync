package com.alibaba.middleware.race.sync;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

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
	public void received(RecalculateContext context, RecordLog record) throws Exception {
		Map<Long, Record> records = context.getRecords();
		checkNotNull(record.getPrimaryColumn(), "Primary column can not be null");
		updatePKIfNeeded(records, record);
		long pk = (long) (record.getPrimaryColumn().getLongValue());
		Record oldRecord = records.get(pk);

		if (oldRecord == null) {
			Record r = new Record();
			//FIXME fill r
			records.put(pk, r);
		} else {
			switch (record.getAlterType()) {
			case Constants.UPDATE:
				Map<byte[], Column> cols = oldRecord.getColumns();
				for (ColumnLog c : record.getColumns()) {
					cols.get(c.getName());
					//FIXME compare timestamp
				}
				break;
			case Constants.DELETE:
				records.remove(pk);
				break;
			case Constants.INSERT:
				throw new RuntimeException("Insert records with same primary key. PK : " + pk);
			}
		}

	}

	private void updatePKIfNeeded(Map<Long, Record> records, RecordLog record) {
		if (record.isPKUpdate()) { //主键变更的特殊情况
			PrimaryColumnLog newPc = record.getPrimaryColumn();
			long pk = (long) (newPc.getBeforeValue());
			long newPk = (long)(newPc.getLongValue());
			Record oldRecord = records.get(pk);
			PrimaryColumn pc = oldRecord.getPrimaryColumn();
			pc.setLastUpdate(record.getTimestamp());
			pc.setLongValue(newPk);
			pc.setValue(newPc.getValue());
			records.remove(pk);
			records.put(newPk, oldRecord);
		}
	}

}
