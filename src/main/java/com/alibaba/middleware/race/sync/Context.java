package com.alibaba.middleware.race.sync;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.alibaba.middleware.race.sync.model.RecordLog;

/**
 * @author wangkai
 *
 */
public class Context {

	private long					endId;
	private RecordLogReceiver		receiver;
	private long					startId;
	private String					tableSchema;
	private boolean				executeByCoreProcesses	= false;
	private BlockingQueue<RecordLog>	recordLogQueue;
	private RecalculateContext		recalculateContext;
	private RecalculateThread		recalculateThread;
	private int					availableProcessors		= Runtime.getRuntime()
			.availableProcessors() - 2;

	public Context(long endId, RecordLogReceiver receiver, long startId, String tableSchema) {
		this.endId = endId;
		this.receiver = receiver;
		this.startId = startId;
		this.tableSchema = tableSchema;
	}

	public RecordLogReceiver getReceiver() {
		return receiver;
	}

	public String getTableSchema() {
		return tableSchema;
	}

	public void initialize() {
		recordLogQueue = new ArrayBlockingQueue<>(1024 * 8);
		recalculateContext = new RecalculateContext(this, getReceiver(), recordLogQueue);
		recalculateThread = new RecalculateThread(recalculateContext);
	}

	public void setReceiver(RecordLogReceiver receiver) {
		this.receiver = receiver;
	}

	public void setTableSchema(String tableSchema) {
		this.tableSchema = tableSchema;
	}

	public long getEndId() {
		return endId;
	}

	public void setEndId(long endId) {
		this.endId = endId;
	}

	public long getStartId() {
		return startId;
	}

	public void setStartId(long startId) {
		this.startId = startId;
	}

	public RecalculateContext getRecalculateContext() {
		return recalculateContext;
	}

	public void stopRecalculateThreads() {
		recalculateThread.stop();
	}

	public RecalculateThread getRecalculateThread() {
		return recalculateThread;
	}

	public void dispatch(RecordLog recordLog) throws InterruptedException {
		for(;!recordLogQueue.offer(recordLog,16,TimeUnit.MICROSECONDS);){
		}
	}

	public boolean isExecuteByCoreProcesses() {
		return executeByCoreProcesses;
	}

	public int getAvailableProcessors() {
		return availableProcessors;
	}

}
