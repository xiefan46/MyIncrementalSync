package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xiefan on 6/16/17.
 */
public class Dispatcher {

	private int					threadNum;

	private Map<Long, Byte>			redirectMap	= new HashMap<>();

	private List<RecalculateThread>	threadList	= new ArrayList<>();

	public Dispatcher(int threadNum) {
		this.threadNum = threadNum;
	}

	public void start(RecordLog r) {
		for (int i = 0; i < threadNum; i++) {
			RecalculateThread thread = new RecalculateThread(Table.newTable(r));
			threadList.add(thread);
			thread.start();
		}
	}

	public void dispatch(RecordLog recordLog) {
		long id = recordLog.getPrimaryColumn().getLongValue();
		long oldId = recordLog.getPrimaryColumn().getBeforeValue();
		if (recordLog.isPKUpdate()) {
			Byte oldDirect = redirectMap.remove(oldId);
			byte newThread = hashFun(id);
			if (oldDirect == null) //如果oldDirect不为空,则为连锁update 
				oldDirect = hashFun(oldId);
			if (oldDirect != newThread)
				redirectMap.put(id, oldDirect);
			threadList.get(oldDirect).submit(recordLog);
		} else {
			Byte threadId = redirectMap.get(id);
			if (threadId == null)
				threadId = hashFun(id);
			threadList.get(threadId).submit(recordLog);
		}

	}

	public void readRecordOver() {
		for (RecalculateThread thread : threadList) {
			thread.setReadOver(true);
		}
	}

	public void waitForOk(Context context) {
		try {
			for (RecalculateThread thread : threadList) {
				thread.join();
				context.getRecords().putAll(thread.getRecords());
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private byte hashFun(long id) {
		byte result = (byte) (id % threadNum);
		return result;
	}

}
