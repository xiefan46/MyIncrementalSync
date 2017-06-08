package com.alibaba.middleware.race.sync;

import java.util.Map;
import java.util.Map.Entry;

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
		PrimaryColumnLog pcl = record.getPrimaryColumn();
		if (pcl.IsPkChange()) {
			long pk = (long) (pcl.getBeforeValue());
			long lastUpdate = record.getTimestamp();
			Record oldRecord = records.remove(pk);
			update(oldRecord, record, lastUpdate);
			PrimaryColumn pc = oldRecord.getPrimaryColumn();
			pc.setLastUpdate(lastUpdate);
			pc.setLongValue(pcl.getLongValue());
			pc.setValue(pcl.getValue());
			context.getContext().dispatch(newRecordLog(oldRecord,record));
			return;
		}
		
		long pk = (long) (pcl.getLongValue());
		long lastUpdate = record.getTimestamp();
		Record oldRecord = records.get(pk);

		if (oldRecord == null) {
			records.put(pk, newRecord(record, pcl, lastUpdate));
			return;
		}

		switch (record.getAlterType()) {
		case Constants.UPDATE:
			update(oldRecord, record, lastUpdate);
			break;
		case Constants.DELETE:
			update(oldRecord, record, true);
			break;
		case Constants.INSERT:
			insert(oldRecord, record, lastUpdate);
			update(oldRecord, record, false);
		}
	}
	
	private void update(Record oldRecord,RecordLog record,boolean delete){
		if (record.getTimestamp() > oldRecord.getLastUpdate()) {
			oldRecord.setDeleted(delete);
			oldRecord.setLastUpdate(record.getTimestamp());
		}
	}
	
	private Record newRecord(RecordLog record,PrimaryColumnLog pcl, long lastUpdate){
		Record r = new Record();
		PrimaryColumn pc = new PrimaryColumn();
		pc.setLastUpdate(lastUpdate);
		pc.setLongValue(pcl.getLongValue());
		pc.setValue(pcl.getValue());
		r.setPrimaryColumn(pc);
		r.setLastUpdate(lastUpdate);
		for(ColumnLog cl : record.getColumns()){
			Column c = new Column();
			c.setLastUpdate(lastUpdate);
			c.setValue(cl.getValue());
			r.putColumn(cl.getName(), c);
		}
		return r;
	}
	
	
	private RecordLog newRecordLog(Record record,RecordLog recordLog){
		throw new UnsupportedOperationException();
	}
	
	private void update(Record oldRecord,RecordLog record,long lastUpdate){
		Map<byte[], Column> cols = oldRecord.getColumns();
		for (ColumnLog c : record.getColumns()) {
			Column oldC = cols.get(c.getName());
			if (oldC.getLastUpdate() > lastUpdate) {
				continue;
			}
			oldC.setLastUpdate(lastUpdate);
			oldC.setValue(c.getValue());
		}
		oldRecord.getPrimaryColumn().setLastUpdate(lastUpdate);
	}
	
	private void updatePc(PrimaryColumn pc,long lastUpdate){
		if (pc.getLastUpdate() < lastUpdate) {
			
		}
		
		
	}
	
	
	private void insert(Record oldRecord,RecordLog record,long lastUpdate){
		Record newRecord = newRecord(record, record.getPrimaryColumn(), lastUpdate);
		Map<byte[], Column> cols = newRecord.getColumns();
		for (Entry<byte[], Column> c : oldRecord.getColumns().entrySet()) {
			Column oldC = cols.get(c.getKey());
			if (oldC.getLastUpdate() > lastUpdate) {
				continue;
			}
			oldC.setLastUpdate(lastUpdate);
			oldC.setValue(c.getValue().getValue());
		}
		oldRecord.getPrimaryColumn().setLastUpdate(lastUpdate);
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
