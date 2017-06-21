package com.alibaba.middleware.race.sync;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.PrimaryColumnLog;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * Created by xiefan on 6/16/17.
 */
public class RecalculateThread extends Thread implements Constants {

	private Map<Integer, long[]>	records;

	private Table				table;

	private volatile boolean		running	= true;

	private Queue<RecordLog>		logs		= new ConcurrentLinkedQueue<>();

	private Context			context;

	private static Logger		logger	= LoggerFactory.getLogger(RecalculateThread.class);

	public RecalculateThread(Context context, Table table, Map<Integer, long[]> records,int i) {
		super("recal-"+i);
		this.context = context;
		this.table = table;
		this.records = records;
	}

	public void stopThread() {
		running = false;
	}

	@Override
	public void run() {
		try {
			Table table = this.table;
			long startTime = System.currentTimeMillis();
			Queue<RecordLog> logs = this.logs;
			for (;;) {
				RecordLog r = logs.poll();
				if (r == null) {
					if (!running) {
						break;
					}
					continue;
				}
				received(table, r);
			}
			logger.info("线程 {} 重放完成, 耗时 : {}, 重放记录数 {}", Thread.currentThread().getId(),
					System.currentTimeMillis() - startTime);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void submit(RecordLog recordLog) {
		logs.offer(recordLog);
	}

	public Map<Integer, long[]> getRecords() {
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
