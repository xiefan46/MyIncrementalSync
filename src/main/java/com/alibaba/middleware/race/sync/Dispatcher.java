package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.Node;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.util.collection.MyIntByteHashMap;
import com.carrotsearch.hppc.IntObjectHashMap;

/**
 * Created by xiefan on 6/16/17.
 */
public class Dispatcher {
	
	private int					threadNum;

	private IntObjectHashMap<byte []>[]	recordMaps;

	private RecalculateThread[]		threads;
	
	private MyIntByteHashMap			redirectMap	= new MyIntByteHashMap(1024 * 1024 * 4);
	
	private Task[]		rootTasks;
	
	private Node<RecordLog>[]		currentNodes;
	
	private Context context;

	public Dispatcher(Context context) {
		this.context = context;
	}

	@SuppressWarnings("unchecked")
	public void start() throws InterruptedException {
		threadNum = context.getRecalThreadNum();
		rootTasks = new Task[threadNum];
		currentNodes = new Node[threadNum];
		recordMaps = new IntObjectHashMap[threadNum];
		threads = new RecalculateThread[threadNum];
		for (int i = 0; i < threadNum; i++) {
			recordMaps[i] = new IntObjectHashMap<>((int)(1024 * 1024 * (32f / threadNum)));
			rootTasks[i] = new Task();
			rootTasks[i].rootNode = new Node<>();
		}
		for (int i = 0; i < threadNum; i++) {
			threads[i] = new RecalculateThread(context, recordMaps[i],rootTasks[i],i);
		}
		for (int i = 0; i < threadNum; i++) {
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
		return (byte) (id % threadNum);
	}

	public IntObjectHashMap<byte[]>[] getRecordMaps() {
		return recordMaps;
	}
	
	public void beforeDispatch(){
		Node<RecordLog>[] currentRecordLogs = this.currentNodes;
		for (int i = 0; i < threadNum; i++) {
			currentRecordLogs[i] = rootTasks[i].rootNode;
			rootTasks[i].limit = 0;
		}
	}
	
	public void dispatch(Node<RecordLog> pnr,int limit){
		Node<RecordLog>[] currentRecordLogs = this.currentNodes;
		int cols = context.getTable().getColumnSize();
		byte B = -1;
		MyIntByteHashMap redirectMap = this.redirectMap;
//		int startId = (int)context.getStartId();
//		int endId = (int)context.getEndId();
		for (int i = 0; i < limit; i++) {
			RecordLog r = pnr.getValue();
//			PkCount.get().count(r, startId, endId);
			int id = r.getPk();
			int oldId = r.getBeforePk();
			if (r.isPkUpdate()) {
				byte oldDirect = redirectMap.remove(oldId, B);
				if (oldDirect == B) {
					oldDirect = hashFun(oldId);
					if (oldDirect != hashFun(id)) {
						redirectMap.put(id, oldDirect);
					}
				}else{
					redirectMap.put(id, oldDirect);
				}
				currentRecordLogs[oldDirect] = getNext(currentRecordLogs[oldDirect], cols, oldDirect);
				currentRecordLogs[oldDirect].setValue(r);
			} else {
				byte threadId;
				if (r.getAlterType() == Constants.DELETE) {
					threadId = redirectMap.remove(id, B);
				}else{
					threadId = redirectMap.getOrDefault(id,B);
				}
				if (threadId == B) {
					threadId = hashFun(id);
				}
				currentRecordLogs[threadId] = getNext(currentRecordLogs[threadId], cols, threadId);
				currentRecordLogs[threadId].setValue(r);
			}
			pnr = pnr.getNext();
		}
		
	}
	
	private Node<RecordLog> getNext(Node<RecordLog> node,int cols,int limitIndex){
		rootTasks[limitIndex].limit++; 
		Node<RecordLog> next = node.getNext();
		if (next == null) {
			next = new Node<>();
			node.setNext(next);
			return next;
		}
		return next;
	}
	
	public MyIntByteHashMap getRedirectMap() {
		return redirectMap;
	}
	
	class Task{
		Node<RecordLog> rootNode;
		int limit;
	}
}
