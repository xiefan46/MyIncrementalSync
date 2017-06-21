package com.alibaba.middleware.race.sync;

import java.util.Map;
import java.util.concurrent.ThreadFactory;

import com.alibaba.middleware.race.sync.dis.RecordLogEvent;
import com.alibaba.middleware.race.sync.dis.RecordLogEventFactory;
import com.alibaba.middleware.race.sync.dis.RecordLogEventProducer;
import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.PrimaryColumnLog;
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

	private Map<Integer, long []>		records;

	private Table					table;

	private ReadRecordLogThread		readRecordLogThread;

	private RecordLogEventProducer	eventProducer;
	
	private Disruptor<RecordLogEvent> disruptor;
	
	private Context				context;

	public RecalculateThread(Context context, Table table, Map<Integer, long []> records) {
		this.context = context;
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

	public void submit(RecordLog recordLog) {
		eventProducer.publish(recordLog);
	}

	public Map<Integer, long []> getRecords() {
		return records;
	}

	public void received(Table table, RecordLog recordLog) throws Exception {
		Map<Integer, long[]> records = this.records;
		PrimaryColumnLog pcl = recordLog.getPrimaryColumn();
		Integer pk = pcl.getLongValue();
		switch (recordLog.getAlterType()) {
		case Constants.UPDATE:
			update(records.get(pk), recordLog);
			break;
		case Constants.PK_UPDATE:
			Integer beforeValue = pcl.getBeforeValue();
			long[] oldRecord = records.remove(beforeValue);
			update(oldRecord, recordLog);
			records.put(pk, oldRecord);
			break;
		case Constants.DELETE:
			records.remove(pk);
			break;
		case Constants.INSERT:
			records.put(pk, update(table.newRecord(), recordLog));
			break;
		default:
			break;
		}
	}

	private long[] update(long[] oldRecord, RecordLog recordLog) {
		for (int i = 0; i < recordLog.getEdit(); i++) {
			ColumnLog c = recordLog.getColumn(i);
			oldRecord[c.getName()] = c.getValue();
		}
		return oldRecord;
	}

}
