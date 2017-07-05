package com.alibaba.middleware.race.sync;

import java.util.BitSet;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.util.MyIntByteHashMap;
import com.alibaba.middleware.race.sync.util.MyList;
import com.carrotsearch.hppc.IntObjectHashMap;

/**
 * Created by xiefan on 6/16/17.
 */
public class Dispatcher {
	
	private int					recalThreadNum;
	
	private IntObjectHashMap<byte []>[]	recordMaps;
	
	private BitSet					redirectFlag;
	
	private RecalculateThread[]		threads;
	
	private MyIntByteHashMap			redirectMap	= new MyIntByteHashMap(1024 * 1024 * 4);
	
	private MyList<RecordLog>[]		recordLogLists;
	
	private Context context;

	public Dispatcher(Context context) {
		this.context = context;
	}

	@SuppressWarnings("unchecked")
	public void start() throws InterruptedException {
		int blockSize = context.getBlockSize();
		recalThreadNum = context.getRecalThreadNum();
		recordLogLists = new MyList[recalThreadNum];
		recordMaps = new IntObjectHashMap[recalThreadNum];
		threads = new RecalculateThread[recalThreadNum];
		redirectFlag = new BitSet(Integer.MAX_VALUE);
		for (int i = 0; i < recalThreadNum; i++) {
			recordMaps[i] = new IntObjectHashMap<>((int)(1024 * 1024 * (32f / recalThreadNum)));
			recordLogLists[i] = new MyList<>((int)(blockSize * context.getParseThreadNum() / 80 ));
		}
		for (int i = 0; i < recalThreadNum; i++) {
			threads[i] = new RecalculateThread(context, recordMaps[i], recordLogLists[i], i);
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
	
	public void startWork(){
		for (RecalculateThread t : threads) {
			t.startWork();
		}
	}

	public byte [] getRecord(int id) {
		byte b = redirectMap.getOrDefault(id,(byte)(-1));
		if (b == -1) {
			return recordMaps[hashFun(id)].get(id);
		}
		return recordMaps[b].get(id);
	}

	public byte hashFun(int id) {
		return (byte) (id % recalThreadNum);
	}

	public IntObjectHashMap<byte[]>[] getRecordMaps() {
		return recordMaps;
	}
	
	public void beforeDispatch(){
		MyList<RecordLog>[] recordLogLists = this.recordLogLists;
		for (int i = 0; i < recalThreadNum; i++) {
			recordLogLists[i].clear();
		}
	}
	
	public void dispatch(MyList<RecordLog> rs){
		MyList<RecordLog>[] recordLogLists = this.recordLogLists;
		int limit = rs.getPos();
		byte B = -1;
		MyIntByteHashMap redirectMap = this.redirectMap;
		BitSet redirectFlag = this.redirectFlag;
		for (int i = 0; i < limit; i++) {
			RecordLog r = rs.get(i);
			int id = r.getPk();
			int oldId = r.getBeforePk();
			if (r.isPkUpdate()) {
				byte oldDirect = redirectMap.remove(oldId, B);
				if (oldDirect == B) {
					oldDirect = hashFun(oldId);
					if (oldDirect != hashFun(id)) {
						redirectFlag.set(id);
						redirectMap.put(id, oldDirect);
					}
				}else{
					redirectFlag.clear(oldId);
					redirectMap.put(id, oldDirect);
				}
				recordLogLists[oldDirect].add(r);
			} else {
				if (redirectFlag.get(id)) {
					byte threadId;
					if (r.getAlterType() == Constants.DELETE) {
						redirectFlag.clear(id);
						threadId = redirectMap.remove(id, B);
					}else{
						threadId = redirectMap.getOrDefault(id,B);
					}
					recordLogLists[threadId].add(r);
				}else{
					recordLogLists[hashFun(id)].add(r);
				}
			}
		}
	}
	
	public MyIntByteHashMap getRedirectMap() {
		return redirectMap;
	}
	
}
