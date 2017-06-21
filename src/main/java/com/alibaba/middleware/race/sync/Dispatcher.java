package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * Created by xiefan on 6/16/17.
 */
public class Dispatcher {

	private Table					table;

	private int					threadNum;

	private Map<Integer, long []>[]	recordMaps;

	private RecalculateThread[]		threads;
	
	private Map<Integer, Byte>		redirectMap	= new HashMap<>(1024 * 512);
	
	private Context context;

	public Dispatcher(Context context) {
		this.context = context;
	}

	@SuppressWarnings("unchecked")
	public void start() throws InterruptedException {
		threadNum = context.getAvailableProcessors();
		recordMaps = new Map[threadNum];
		threads = new RecalculateThread[threadNum];
		for (int i = 0; i < threadNum; i++) {
			recordMaps[i] = new HashMap<>((int)(1024 * 1024 * ((32f / threadNum))));
		}
		for (int i = 0; i < threadNum; i++) {
			threads[i] = new RecalculateThread(context, table, recordMaps[i],i);
		}
		for (int i = 0; i < threadNum; i++) {
			threads[i].start();
		}
	}

	public void dispatch(RecordLog recordLog) {
		int id = recordLog.getPrimaryColumn().getLongValue();
		int oldId = recordLog.getPrimaryColumn().getBeforeValue();
		if (recordLog.isPKUpdate()) {
			Byte oldDirect = redirectMap.remove(oldId);
			if (oldDirect == null) {
				oldDirect = hashFun(oldId);
			}
			redirectMap.put(id, oldDirect);
			threads[oldDirect].submit(recordLog);
		} else {
			Byte threadId = redirectMap.get(id);
			if (threadId == null)
				threadId = hashFun(id);
			threads[threadId].submit(recordLog);
		}
	}

	public void readRecordOver() {
		for (RecalculateThread thread : threads) {
			thread.stopThread();
		}
	}

	public long [] getRecord(int id) {
		Byte b = redirectMap.get(id);
		if (b == null) {
			return recordMaps[hashFun(id)].get(id);
		}
		return recordMaps[b].get(id);
	}

	public byte hashFun(int id) {
		return (byte) (id % threadNum);
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public Map<Integer, long[]>[] getRecordMaps() {
		return recordMaps;
	}

	public Map<Integer, Byte> getRedirectMap() {
		return redirectMap;
	}
	
}
