package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * Created by xiefan on 6/16/17.
 */
public class Dispatcher {

	private Table					table;

	private int					threadNum;

	private Map<Integer, Record>[]	recordMaps;

	private RecalculateThread[]		threads;
	
	private Context context;

	public Dispatcher(Context context) {
		this.context = context;
	}

	@SuppressWarnings("unchecked")
	public void start(RecordLog r) throws InterruptedException {
		threadNum = context.getAvailableProcessors();
		recordMaps = new Map[threadNum];
		threads = new RecalculateThread[threadNum];
		for (int i = 0; i < threadNum; i++) {
			recordMaps[i] = new HashMap<>((int)(1024 * 1024 * ((32f / threadNum))));
		}
		for (int i = 0; i < threadNum; i++) {
			threads[i] = new RecalculateThread(context, table, recordMaps[i]);
		}
		for (int i = 0; i < threadNum; i++) {
			threads[i].startup(i);
		}
	}

	public void dispatch(RecordLog recordLog) throws InterruptedException {
		int id = recordLog.getPrimaryColumn().getLongValue();
		int oldId = recordLog.getPrimaryColumn().getBeforeValue();
		if (id < 0) {
			context.getReadRecordLogThread().getRecordLogEventProducer().publish(recordLog);
			return;
		}
		if (recordLog.isPKUpdate()) {
			threads[hashFun(oldId)].submit(recordLog);
		} else {
			threads[hashFun(id)].submit(recordLog);
		}
	}

	public void dispatch(int index, RecordLog recordLog) throws InterruptedException {
		threads[index].submit(recordLog);
	}

	public void readRecordOver() {
		for (RecalculateThread thread : threads) {
			thread.stop();
		}
	}

	public Record getRecord(int id) {
		return recordMaps[hashFun(id)].get(id);

	}

	public int hashFun(int id) {
		return id % threadNum;
	}

	public void setTable(Table table) {
		this.table = table;
	}
}
