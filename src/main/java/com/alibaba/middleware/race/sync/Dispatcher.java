package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.util.MyList;
import com.alibaba.middleware.race.sync.util.RecordMap;

/**
 * Created by xiefan on 6/16/17.
 */
public class Dispatcher {

	private int					recalThreadNum;
	
	private RecordMap<byte []>		recordMap;

	private RecalculateThread[]		threads;

	private MyList<RecordLog>[]		recordLogLists;

	private Context				context;

	public Dispatcher(Context context) {
		this.context = context;
	}

	@SuppressWarnings("unchecked")
	public void start() throws InterruptedException {
		int blockSize = context.getBlockSize();
		int rCapacity = context.getEndId() - context.getStartId();
		recalThreadNum = context.getRecalThreadNum();
		recordLogLists = new MyList[recalThreadNum];
		recordMap = new RecordMap<>(rCapacity, context.getStartId());
		threads = new RecalculateThread[recalThreadNum];
		for (int i = 0; i < recalThreadNum; i++) {
			recordLogLists[i] = new MyList<>((int)(blockSize * context.getParseThreadNum() / 80 ));
		}
		for (int i = 0; i < recalThreadNum; i++) {
			threads[i] = new RecalculateThread(context, recordMap, recordLogLists[i], i);
		}
		for (int i = 0; i < recalThreadNum; i++) {
			threads[i].start();
		}
	}

	public void readRecordOver() {
		for (RecalculateThread t : threads) {
			t.shutdown();
		}
	}

	public void startWork() {
		for (RecalculateThread t : threads) {
			t.startWork();
		}
	}

	public byte[] getRecord(int id) {
		return recordMap.get(id);
	}

	public byte hashFun(int id) {
		return (byte) (id % recalThreadNum);
	}

	public RecordMap<byte []> getRecordMaps() {
		return recordMap;
	}

	public void beforeDispatch() {
		for (int i = 0; i < recalThreadNum; i++) {
			recordLogLists[i].clear();
		}
	}

	public void dispatch(MyList<RecordLog> list) {
		MyList<RecordLog>[] currentRecordLogs = this.recordLogLists;
		int limit = list.getPos() - 1;
		for (int i = 0; i < limit; i++) {
			RecordLog r = list.get(i);
			int id = r.getPk();
			byte threadId = hashFun(id);
			currentRecordLogs[threadId].add(r);
		}
	}

}
