package com.alibaba.middleware.race.sync;

import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.carrotsearch.hppc.IntObjectHashMap;

/**
 * Created by xiefan on 6/16/17.
 */
public class Dispatcher {

	private int					threadNum;

	private IntObjectHashMap<byte []>[]	recordMaps;

	private RecalculateThread[]		threads;
	
	private IntObjectHashMap<Byte>	redirectMap	= new IntObjectHashMap<Byte>(1024 * 512);
	
	private CountDownLatch			countDownLatch;
	
	private Context context;

	public Dispatcher(Context context) {
		this.context = context;
	}

	@SuppressWarnings("unchecked")
	public void start() throws InterruptedException {
		threadNum = context.getRecalThreadNum();
		recordMaps = new IntObjectHashMap[threadNum];
		threads = new RecalculateThread[threadNum];
		for (int i = 0; i < threadNum; i++) {
			recordMaps[i] = new IntObjectHashMap<>((int)(1024 * 1024 * (32f / threadNum)));
		}
		for (int i = 0; i < threadNum; i++) {
			threads[i] = new RecalculateThread(context, recordMaps[i],(int)(1024 * (1024f / threadNum)),i);
		}
		for (int i = 0; i < threadNum; i++) {
			threads[i].start();
		}
	}

	public void dispatch(RecordLog recordLog) throws InterruptedException {
		int id = recordLog.getPk();
		int oldId = recordLog.getBeforePk();
		if (recordLog.isPkUpdate()) {
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

	public byte [] getRecord(int id) {
		Byte b = redirectMap.get(id);
		if (b == null) {
			return recordMaps[hashFun(id)].get(id);
		}
		return recordMaps[b].get(id);
	}

	public byte hashFun(int id) {
		return (byte) (id % threadNum);
	}

	public IntObjectHashMap<byte[]>[] getRecordMaps() {
		return recordMaps;
	}
	
	public void newCountDownLatch(int n){
		countDownLatch = new CountDownLatch(n);
	}
	
	public void countDown(){
		countDownLatch.countDown();
	}
	
	public void await() throws InterruptedException{
		countDownLatch.await();
	}
	
}
