package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.Column;
import com.alibaba.middleware.race.sync.model.Record;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordLogReceiverImpl implements RecordLogReceiver {

	@Override
	public void received(Context context, Record record,long startId,long endId) throws Exception {
		checkNotNull(record);
		Map<Long, Record> records = context.getRecords();
		checkNotNull(record.getPrimaryColumn(), "Primary column can not be null");
		checkState(record.getPrimaryColumn().isNumber(),
				"Primary key is not number type, please check");
		updatePKIfNeeded(records, record);
		long pk = (long) (record.getPrimaryColumn().getValue());
		Record oldRecord = records.get(pk);

		if (oldRecord == null) {
			records.put(pk, record);
		} else {
			switch (record.getAlterType()) {
			case Record.UPDATE:
				for (Column c : record.getColumns().values()) {
					oldRecord.addColumn(c);
				}
				break;
			case Record.DELETE:
				records.remove(pk);
				break;
			case Record.INSERT:
				throw new RuntimeException("Insert records with same primary key. PK : " + pk);
			}
		}

	}

	@Override
	public void receivedFinal(Context context, Record record,long startId,long endId) throws Exception {
		received(context, record,startId,endId);
	}

	private void updatePKIfNeeded(Map<Long, Record> records, Record record) {
		if (record.isPKUpdate()) { //主键变更的特殊情况
			long pk = (long) (record.getPrimaryColumn().getBeforeValue());
			Record oldRecord = records.get(pk);
			oldRecord.setPrimaryColumn(record.getPrimaryColumn());
			records.remove(pk);
			long newPk = (long) (record.getPrimaryColumn().getValue());
			records.put(newPk, oldRecord);
		}
	}

}
