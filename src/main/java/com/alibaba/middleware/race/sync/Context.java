package com.alibaba.middleware.race.sync;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.alibaba.middleware.race.sync.model.RecordLog;

/**
 * @author wangkai
 *
 */
public class Context {

	private long							endId;
	private RecordLogReceiver				receiver;
	private long							startId;
	private String							tableSchema;
	private boolean						executeByCoreProcesses	= false;
	private BlockingQueue<RecordLog>[]	recordLogQueues;
	private RecalculateContext[]				recalculateContexts;
	private RecalculateThread []				recalculateThreads;
	private int							availableProcessors		= Runtime.getRuntime()
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
		int availableProcessors = getAvailableProcessors();
		recordLogQueues = new BlockingQueue[availableProcessors];
		recalculateContexts = new RecalculateContext[availableProcessors];
		recalculateThreads = new RecalculateThread[availableProcessors];
		for (int i = 0; i < availableProcessors; i++) {
			recordLogQueues[i] = new ArrayBlockingQueue<>(1024 * 8);
			recalculateContexts[i] = new RecalculateContext(getReceiver(), recordLogQueues[i]);
			recalculateThreads[i] = new RecalculateThread(recalculateContexts[i]);
		}
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
	
	public RecalculateContext[] getRecalculateContexts() {
		return recalculateContexts;
	}
	
	public void stopRecalculateThreads(){
		for(RecalculateThread t : recalculateThreads){
			t.stop();
		}
	}
	
	public RecalculateThread[] getRecalculateThreads() {
		return recalculateThreads;
	}

	public void dispatch(RecordLog recordLog) {
		int r = (int) (recordLog.getPrimaryColumn().getLongValue() % availableProcessors);
		recordLogQueues[r].offer(recordLog);
	}

	public boolean isExecuteByCoreProcesses() {
		return executeByCoreProcesses;
	}
	
	public int getAvailableProcessors() {
		return availableProcessors;
	}

}
