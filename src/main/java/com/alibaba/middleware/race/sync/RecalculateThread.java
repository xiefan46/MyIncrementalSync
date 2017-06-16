package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by xiefan on 6/16/17.
 */
public class RecalculateThread extends Thread {

	private Map<Long, byte[][]>	records	= new HashMap<>((int) (1024 * 256 * 1.5));

	private Queue<RecordLog>		logQueue	= new ConcurrentLinkedQueue<>();

	private boolean			readOver	= false;

	private Table				table;

	private static final Logger	logger	= LoggerFactory.getLogger(RecalculateThread.class);

	private int				count	= 0;

	public RecalculateThread(Table table) {
		this.table = table;
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
		this.logQueue.offer(recordLog);
	}

	public Map<Long, byte[][]> getRecords() {
		return records;
	}

	public void setRecords(Map<Long, byte[][]> records) {
		this.records = records;
	}

	public Queue<RecordLog> getLogQueue() {
		return logQueue;
	}

	public void setLogQueue(Queue<RecordLog> logQueue) {
		this.logQueue = logQueue;
	}

	public boolean isReadOver() {
		return readOver;
	}

	public void setReadOver(boolean readOver) {
		this.readOver = readOver;
	}

	public void received(RecordLog recordLog) throws Exception {
		PrimaryColumnLog pcl = recordLog.getPrimaryColumn();
		Long pk = pcl.getLongValue();
		switch (recordLog.getAlterType()) {
		case com.alibaba.middleware.race.sync.model.Constants.UPDATE:
			if (pcl.isPkChange()) {
				Long beforeValue = pcl.getBeforeValue();
				byte[][] oldRecord = records.remove(beforeValue);
				update(table, oldRecord, recordLog);
				records.put(pk, oldRecord);
				break;
			}
			update(table, records.get(pk), recordLog);
			break;
		case com.alibaba.middleware.race.sync.model.Constants.DELETE:
			records.remove(pk);
			break;
		case com.alibaba.middleware.race.sync.model.Constants.INSERT:
			records.put(pk, update(table, table.newRecord(), recordLog));
			break;
		default:
			break;
		}
	}

	private byte[][] update(Table table, byte[][] oldRecord, RecordLog recordLog) {
		for (ColumnLog c : recordLog.getColumns()) {
			if (!c.isUpdate()) {
				break;
			}
			Integer index = table.getIndex(c.getName());
			oldRecord[index] = c.getValue();
			c.setUpdate(false);
		}
		return oldRecord;
	}
}
