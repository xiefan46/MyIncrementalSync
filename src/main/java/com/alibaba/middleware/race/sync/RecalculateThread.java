package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * Created by xiefan on 6/16/17.
 */
public class RecalculateThread extends Thread implements Constants {

	private Map<Integer, Record>	records		= new HashMap<>((int) (1024 * 256 * 1.5));

	private Queue<RecordLog>		logQueue		= new ConcurrentLinkedQueue<>();

	private boolean			readOver		= false;

	private Table				table;

	private static final Logger	logger		= LoggerFactory
			.getLogger(RecalculateThread.class);

	private int				count		= 0;

	private static final String	CAN_NOT_HANDLE	= "Can not handle this alter type";

	private static final String	TYPE_NOT_EXIST	= "Type not exist";

	private Dispatcher			dispatcher;

	public RecalculateThread(Table table, Dispatcher dispatcher) {
		this.table = table;
		this.dispatcher = dispatcher;
	}

	@Override
	public void run() {
		try {
			long startTime = System.currentTimeMillis();
			while (!readOver || !logQueue.isEmpty()) {
				while (!logQueue.isEmpty()) {
					RecordLog recordLog = logQueue.poll();
					if (recordLog != null) {
						count++;
						received(recordLog);
					}
				}
				Thread.currentThread().sleep(10);
			}
			logger.info("线程 {} 重放完成, 耗时 : {}, 重放记录数 {}", Thread.currentThread().getId(),
					System.currentTimeMillis() - startTime, count);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void submit(RecordLog recordLog) {
		this.logQueue.add(recordLog);
	}

	public Map<Integer, Record> getRecords() {
		return records;
	}

	public boolean isReadOver() {
		return readOver;
	}

	public void setReadOver(boolean readOver) {
		this.readOver = readOver;
	}

	public void received(RecordLog recordLog) throws Exception {
		//logger.info("receive log.pk : {}", recordLog.getPrimaryColumn().getLongValue());
		switch (recordLog.getAlterType()) {
		case INSERT:
			handleInsert(recordLog);
			break;
		case UPDATE:
			handleUpdate(recordLog);
			break;
		case DELETE:
			handleDelete(recordLog);
			break;
		case PK_UPDATE:
			handlePkUpdate(recordLog);
			break;
		default:
			throw createTypeNotExistException(recordLog.getAlterType());
		}
	}

	private void handleInsert(RecordLog recordLog) throws Exception {
		int pk = recordLog.getPrimaryColumn().getLongValue();
		Record record = records.get(pk);
		if (record == null) {
			record = table.newRecord();
			update(table, record, recordLog);
			record.setAlterType(INSERT);
			records.put(pk, record);
		} else {
			switch (record.getAlterType()) {
			case INSERT:
				throw createCanNotHandleException(recordLog, record);
			case UPDATE:
				update2(table, record, recordLog);
				record.setAlterType(INSERT);
				break;
			case DELETE:
				records.remove(pk);
				break;
			case PK_UPDATE:
				update2(table, record, recordLog);
				record.setAlterType(INSERT);
				records.remove(pk);
				redirect(pk, record);
				break;
			default:
				throw createTypeNotExistException(record.getAlterType());
			}
		}
	}

	private void handleUpdate(RecordLog recordLog) {
		int pk = recordLog.getPrimaryColumn().getLongValue();
		Record record = records.get(pk);
		if (record == null) {
			//throw new RuntimeException("Update not exist record." + "pk : " + pk);
			record = table.newRecord();
			update(table, record, recordLog);
			record.setAlterType(UPDATE);
			records.put(pk, record);
		} else {
			switch (record.getAlterType()) {
			case INSERT:
				update(table, record, recordLog);
				break;
			case DELETE:
				throw createCanNotHandleException(recordLog, record);
			case PK_UPDATE:
				throw createCanNotHandleException(recordLog, record);
			case UPDATE:
				update(table, record, recordLog);
				break;
			default:
				throw createTypeNotExistException(record.getAlterType());

			}
		}
	}

	private void handleDelete(RecordLog recordLog) {
		int pk = recordLog.getPrimaryColumn().getLongValue();
		Record record = records.get(pk);
		if (record == null) {
			record = table.newRecord();
			record.setAlterType(DELETE);
			records.put(pk, record);
		} else {
			switch (record.getAlterType()) {
			case INSERT:
				records.remove(pk); //有insert证明已经完成,可以删除
				break;
			case UPDATE:
				record.setAlterType(DELETE); //等待insert到来才能删除
				break;
			case DELETE:
				throw createCanNotHandleException(recordLog, record);
			case PK_UPDATE:
				throw createCanNotHandleException(recordLog, record);
			default:
				throw createTypeNotExistException(record.getAlterType());
			}
		}
	}

	private void handlePkUpdate(RecordLog recordLog) throws Exception {
		int pkOld = recordLog.getPrimaryColumn().getBeforeValue();
		Record recordOld = records.get(pkOld);
		/*
		 * if (ChannelReader2.print(recordLog)) {
		 * logger.info("Record old null ? {}", recordOld == null); }
		 */
		if (recordOld == null) {
			recordOld = table.newRecord();
			recordOld.setAlterType(PK_UPDATE);
			update(table, recordOld, recordLog);
			recordOld.setNewId(recordLog.getPrimaryColumn().getLongValue());
			records.put(pkOld, recordOld);
		} else {
			switch (recordOld.getAlterType()) {
			case INSERT:
				update(table, recordOld, recordLog);
				recordOld.setAlterType(INSERT);
				records.remove(pkOld);
				recordOld.setNewId(recordLog.getPrimaryColumn().getLongValue());
				redirect(pkOld, recordOld);
				break;
			case UPDATE:
				update(table, recordOld, recordLog);
				recordOld.setAlterType(PK_UPDATE);
				recordOld.setNewId(recordLog.getPrimaryColumn().getLongValue());
				break;
			case DELETE:
				throw createCanNotHandleException(recordLog, recordOld);
			case PK_UPDATE:
				throw createCanNotHandleException(recordLog, recordOld);
			default:
				throw createTypeNotExistException(recordOld.getAlterType());
			}
		}
	}

	private RuntimeException createTypeNotExistException(byte alterType) {
		return new RuntimeException(TYPE_NOT_EXIST + " Type : " + (char) alterType);
	}

	private RuntimeException createCanNotHandleException(RecordLog recordLog, Record record) {
		String errorMsg = CAN_NOT_HANDLE + " record type : " + (char) record.getAlterType()
				+ " record log type : " + (char) recordLog.getAlterType()
				+ " recordLog old pk : " + recordLog.getPrimaryColumn().getBeforeValue()
				+ " recordLog new pk : " + recordLog.getPrimaryColumn().getLongValue()
				+ " record new pk : " + record.getNewId();
		return new RuntimeException(errorMsg);
	}

	private Record update(Table table, Record oldRecord, RecordLog recordLog) {
		for (ColumnLog c : recordLog.getColumns()) {
			if (c.getValue() != null) {
				Integer index = table.getIndex(c.getName());
				oldRecord.setColum(index, c.getValue());
			}
		}
		return oldRecord;
	}

	private Record update2(Table table, Record oldRecord, RecordLog recordLog) {
		for (ColumnLog c : recordLog.getColumns()) {
			if (c.getValue() != null) {
				Integer index = table.getIndex(c.getName());
				if (oldRecord.getColumns()[index] == null) {
					oldRecord.setColum(index, c.getValue()); //INSERT的优先级较低
				}
			}
		}
		return oldRecord;
	}

	private void redirect(int oldId, Record r) throws Exception {
		if (r.getAlterType() != INSERT)
			throw new RuntimeException(CAN_NOT_HANDLE);
		int oldHash = dispatcher.hashFun(oldId);
		int newHash = dispatcher.hashFun(r.getNewId());
		if (oldHash != newHash) {
			dispatcher.dispatch(createRecordLog(r));
		} else {
			received(createRecordLog(r));
		}
	}

	private RecordLog createRecordLog(Record r) {
		RecordLog recordLog = RecordLog.newRecordLog();
		recordLog.getPrimaryColumn().setLongValue(r.getNewId());
		recordLog.getPrimaryColumn().setBeforeValue(r.getNewId());
		recordLog.setAlterType(r.getAlterType());
		byte[][] columns = r.getColumns();
		for (int i = 0; i < columns.length; i++) {
			ColumnLog columnLog = new ColumnLog();
			recordLog.getColumns().add(columnLog);
			byte[] name = table.getNameByIndex(i);
			byte[] value = columns[i];
			columnLog.setName(name, 0, name.length);
			columnLog.setValue(value, 0, value.length);
		}
		return recordLog;
	}

}
