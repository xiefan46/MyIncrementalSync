package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xiefan on 6/16/17.
 */
public class Dispatcher {

	private int					threadNum;

	private Map<Integer, Byte>		redirectMap	= new HashMap<>();

	private List<RecalculateThread>	threadList	= new ArrayList<>();

	public Dispatcher(int threadNum) {
		this.threadNum = threadNum;
	}

	private static final Logger logger = LoggerFactory.getLogger(Dispatcher.class);

	public void start(Table table) {
		for (int i = 0; i < threadNum; i++) {
			RecalculateThread thread = new RecalculateThread(table);
			threadList.add(thread);
			thread.start();
		}
	}

	public void dispatch(RecordLog recordLog) {
		int id = recordLog.getPrimaryColumn().getLongValue();
		int oldId = recordLog.getPrimaryColumn().getBeforeValue();
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
		logger.info("thread num : " + threadList.size());
	}

	public void waitForOk(Context context) {
		long total = 0;
		try {
			for (RecalculateThread thread : threadList) {
				thread.join();
				long start = System.currentTimeMillis();
				context.getRecords().putAll(thread.getRecords());
				total += (System.currentTimeMillis() - start);
			}
			logger.info("合并结果花费时间 : {}", total);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private byte hashFun(long id) {
		byte result = (byte) (id % threadNum);
		/*
		 * if (id < 0) { System.out.println("id : " + id + " result : " +
		 * result); }
		 */
		return result;
	}

	public Map<Integer, Byte> getRedirectMap() {
		return redirectMap;
	}

}
