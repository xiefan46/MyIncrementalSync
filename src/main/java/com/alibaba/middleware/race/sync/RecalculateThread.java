package com.alibaba.middleware.race.sync;

import java.util.Map;
import java.util.concurrent.ThreadFactory;

import com.alibaba.middleware.race.sync.dis.RecordLogEvent;
import com.alibaba.middleware.race.sync.dis.RecordLogEventFactory;
import com.alibaba.middleware.race.sync.dis.RecordLogEventProducer;
import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Created by xiefan on 6/16/17.
 */
public class RecalculateThread implements Constants, EventHandler<RecordLogEvent> {

	private Map<Integer, Record>		records;

	private Table					table;

	private Context				context;

	private Dispatcher				dispatcher;

	private ReadRecordLogThread		readRecordLogThread;

	private RecordLogEventProducer	eventProducer;
	
	private Disruptor<RecordLogEvent> disruptor;

	private static final String		CAN_NOT_HANDLE	= "Can not handle this alter type";

	private static final String		TYPE_NOT_EXIST	= "Type not exist";

	public RecalculateThread(Context context, Table table, Map<Integer, Record> records) {
		this.context = context;
		this.dispatcher = context.getDispatcher();
		this.readRecordLogThread = context.getReadRecordLogThread();
		this.table = table;
		this.records = records;
	}

	public void startup(final int i,int ringBufferSize) {

		RecordLogEventFactory factory = new RecordLogEventFactory();

		ThreadFactory threadFactory = new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, "recalculate" + i);
			}
		};

		disruptor = new Disruptor<>(factory,
				ringBufferSize, threadFactory, ProducerType.SINGLE,
				new BusySpinWaitStrategy());

		disruptor.handleEventsWith(this);

		RingBuffer<RecordLogEvent> ringBuffer = disruptor.start();

		eventProducer = new RecordLogEventProducer(ringBuffer);
	}
	
	public void stop(){
		disruptor.shutdown();
	}

	@Override
	public void onEvent(RecordLogEvent event, long sequence, boolean endOfBatch) throws Exception {
		try {
			Table table = this.table;
			//			long startTime = System.currentTimeMillis();
			RecordLog r = event.getRecordLog();
			received(table, r);
			readRecordLogThread.getRecordLogEventProducer().publish(r);
			//			logger.info("线程 {} 重放完成, 耗时 : {}, 重放记录数 {}", Thread.currentThread().getId(),
			//					System.currentTimeMillis() - startTime, count);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void submit(RecordLog recordLog) throws InterruptedException {
		eventProducer.publish(recordLog);
	}

	public Map<Integer, Record> getRecords() {
		return records;
	}

	public void received(Table table, RecordLog recordLog) throws Exception {
		//logger.info("receive log.pk : {}", recordLog.getPrimaryColumn().getLongValue());
		switch (recordLog.getAlterType()) {
		case INSERT:
			handleInsert(table, recordLog);
			break;
		case UPDATE:
			handleUpdate(recordLog);
			break;
		case DELETE:
			handleDelete(recordLog);
			break;
		case PK_UPDATE:
			handlePkUpdate(table, recordLog);
			break;
		default:
			throw createTypeNotExistException(recordLog.getAlterType());
		}
	}

	private void handleInsert(Table table, RecordLog recordLog) throws Exception {
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
				redirect(table, pk, record,recordLog);
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

	private void handlePkUpdate(Table table, RecordLog recordLog) throws Exception {
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
				redirect(table, pkOld, recordOld,recordLog);
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
		for (int i = 0; i < recordLog.getEdit(); i++) {
			ColumnLog c = recordLog.getColumn(i);
			if (c.getValue() == 0) {
				continue;
			}
			oldRecord.setColum(c.getName(), c.getValue());
		}
		return oldRecord;
	}

	private Record update2(Table table, Record oldRecord, RecordLog recordLog) {
		for (int i = 0; i < recordLog.getEdit(); i++) {
			ColumnLog c = recordLog.getColumn(i);
			if (c.getValue() == 0) {
				continue;
			}
			if (oldRecord.getColumns()[c.getName()] == 0) {
				oldRecord.setColum(c.getName(), c.getValue()); //INSERT的优先级较低
			}
		}
		return oldRecord;
	}

	private void redirect(Table table, int oldId, Record r, RecordLog recordLog) throws Exception {
		if (r.getAlterType() != INSERT)
			throw new RuntimeException(CAN_NOT_HANDLE);
		int oldHash = dispatcher.hashFun(oldId);
		int newHash = dispatcher.hashFun(r.getNewId());
		if (oldHash != newHash) {
			dispatcher.dispatch(newHash, createRecordLog(table, r, recordLog));
		} else {
			received(table, createRecordLog(table, r, recordLog));
		}
	}

	private RecordLog createRecordLog(Table table, Record r, RecordLog recordLog) {
		recordLog.reset();
		recordLog.getPrimaryColumn().setLongValue(r.getNewId());
		recordLog.getPrimaryColumn().setBeforeValue(r.getNewId());
		recordLog.setAlterType(r.getAlterType());
		long[] columns = r.getColumns();
		for (int i = 0; i < columns.length; i++) {
			ColumnLog columnLog = recordLog.getColumn();
			columnLog.setName(i);
			columnLog.setValue(columns[i]);
		}
		return recordLog;
	}

}
