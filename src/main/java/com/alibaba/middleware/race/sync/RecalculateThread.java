package com.alibaba.middleware.race.sync;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.generallycloud.baseio.common.ThreadUtil;

/**
 * Created by xiefan on 6/16/17.
 */
public class RecalculateThread extends Thread implements Constants{
	
	private static Logger logger = LoggerFactory.getLogger(RecalculateThread.class);

	private IntObjectHashMap<byte []>		records;

	private Table					table;

	private Context				context;
	
	private volatile	boolean		running = true;
	
	private Dispatcher				dispatcher;
	
//	private ArrayBlockingQueue<RecordLog> logs;
	
	private Queue<RecordLog>			logs = new ConcurrentLinkedQueue<>();
	
	public RecalculateThread(Context context, IntObjectHashMap<byte []> records,int queue,int i) {
		super("recal-"+i);
		this.context = context;
		this.dispatcher = context.getDispatcher();
		this.table = context.getTable();
		this.records = records;
//		this.logs = new ArrayBlockingQueue<>(queue);
	}

	public void stopThread(){
		running = false;
	}

	@Override
	public void run() {
//		BlockingQueue<RecordLog> logs = this.logs;
		Queue<RecordLog> logs = this.logs;
		IntObjectHashMap<byte []> records = this.records;
		Dispatcher dispatcher = this.dispatcher;
		Table table = this.table;
		for(;;){
			try {
//				RecordLog r = logs.poll(16, TimeUnit.MICROSECONDS);
				RecordLog r = logs.poll();
				if (r == null) {
					if (!running) {
						break;
					}
					ThreadUtil.sleep(1);
					continue;
				}
				received(table,records, r);
				dispatcher.countDown();
			} catch (Exception e) {
				logger.error(e.getMessage(),e);
			}
		}
	}

	public void submit(RecordLog recordLog) throws InterruptedException {
		logs.offer(recordLog);
//		for(;!logs.offer(recordLog,16,TimeUnit.MICROSECONDS);){
//		}
	}

	public IntObjectHashMap<byte []> getRecords() {
		return records;
	}

	public void received(Table table,IntObjectHashMap<byte []> records, RecordLog r) throws Exception {
		int pk = r.getPk();
		switch (r.getAlterType()) {
		case Constants.UPDATE:
			update(records.get(pk), r);
			break;
		case Constants.PK_UPDATE:
			int beforeValue = r.getBeforePk();
			byte[] oldRecord = records.remove(beforeValue);
			update(oldRecord, r);
			records.put(pk, oldRecord);
			break;
		case Constants.DELETE:
			records.remove(pk);
			break;
		case Constants.INSERT:
			records.put(pk, update(table.newRecord(), r));
			break;
		default:
			break;
		}
	}

	private byte[] update(byte[] oldRecord, RecordLog recordLog) {
		byte [] cols = recordLog.getColumns();
		for (int i = 0; i < recordLog.getEdit(); i++) {
			int off = i * 8;
			byte name = cols[off++];
			int len = cols[off++];
			RecordLog.setColumn(oldRecord,name, name, cols, off, len);
		}
		return oldRecord;
	}
	
}
