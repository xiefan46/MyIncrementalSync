package com.alibaba.middleware.race.sync;

import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.PrimaryColumnLog;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.alibaba.middleware.race.sync.util.ObjectPool;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordLogReceiverImpl implements RecordLogReceiver ,Constants{
	
	@Override
	public void received(ObjectPool<Record> pool,Table table,Map<Integer, Record> records, RecordLog recordLog) throws Exception {
		PrimaryColumnLog pcl = recordLog.getPrimaryColumn();
		int pk = pcl.getLongValue();
		Record oldRecord;
		switch (recordLog.getAlterType()) {
		case UPDATE:
			oldRecord = records.get(pk);
			if (oldRecord == null) {
				oldRecord = pool.get();
				oldRecord.setAlterType(UPDATE);
				update(table, oldRecord, recordLog);
				records.put(pk, oldRecord);
				break;
			}
			update(table, oldRecord, recordLog);
			break;
		case Constants.PK_UPDATE:
			int beforeValue = pcl.getBeforeValue();
			oldRecord = records.remove(beforeValue);
			if (oldRecord == null) {
				oldRecord = pool.get();
			}
			oldRecord.setAlterType(PK_UPDATE);
			if (oldRecord.getRootPk() == -1) {
				oldRecord.setRootPk(beforeValue);
			}
			update(table, oldRecord, recordLog);
			records.put(pk, oldRecord);
			break;
		case Constants.DELETE:
			oldRecord = records.get(pk);
			if (oldRecord == null) {
				oldRecord = pool.get();
				oldRecord.setAlterType(DELETE);
				oldRecord.delete();
				records.put(pk, oldRecord);
				break;
			}
			oldRecord.delete();
			if (oldRecord.canDelete()) {
				records.remove(pk);
				pool.put(oldRecord);
				break;
			}
			oldRecord.setAlterType(DELETE);
			break;
		case Constants.INSERT:
			oldRecord = records.get(pk);
			if (oldRecord != null) {
				oldRecord.setAlterType(INSERT);
				oldRecord.insert();
				oldRecord.setInserted(true);
				update(table, oldRecord, recordLog);
				break;
			}
			oldRecord = pool.get();
			oldRecord.setAlterType(INSERT);
			oldRecord.insert();
			oldRecord.setInserted(true);
			records.put(pk, update(table, oldRecord, recordLog));
			break;
		default:
			break;
		}
	}

	@Override
	public void receivedFinal(ObjectPool<Record> pool, Table table, Map<Integer, Record> records,
			Map<Integer, Record> records2) throws Exception {
		for(Entry<Integer, Record> es : records2.entrySet()){
			Integer id = es.getKey();
			Record recordLog = es.getValue();
			Record record;
			switch (recordLog.getAlterType()) {
			case UPDATE:
				record = records.get(id);
				update(table, record, recordLog);
				pool.put(recordLog);
				break;
			case Constants.PK_UPDATE:
				if (recordLog.isInserted()) {
					records.put(id, recordLog);
					break;
				}
				int beforeValue = recordLog.getRootPk();
				record = records.remove(beforeValue);
				update(table, record, recordLog);
				records.put(id, record);
				pool.put(recordLog);
				break;
			case Constants.DELETE:
				int rootPk = recordLog.getRootPk();
				if (rootPk != -1) {
					id = rootPk;
				}
				record = records.remove(id);
				pool.put(record);
				break;
			case Constants.INSERT:
				record = records.remove(id);
				if (record != null) {
					pool.put(record);
				}
				records.put(id, recordLog);
				break;
			default:
				throw new RuntimeException();
			}
		}
		records2.clear();
	}

	private Record update(Table table, Record oldRecord, RecordLog recordLog) {
		for (int i = 0; i < recordLog.getEdit(); i++) {
			ColumnLog c = recordLog.getColumn(i);
			oldRecord.setColumn(c.getName(), c.getValue());
		}
		return oldRecord;
	}
	
	private Record update(Table table, Record record, Record recordLog) {
		long [] cols = recordLog.getColumns();
		for (int i = 0; i < cols.length; i++) {
			long v = cols[i];
			if (v == 0) {
				continue;
			}
			record.setColumn(i,v);
		}
		return record;
	}

}
