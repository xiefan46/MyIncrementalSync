package com.alibaba.middleware.race.sync;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

/**
 * Created by xiefan on 6/16/17.
 */
public class Dispatcher {

	private int					threadNum;

	//private Map<Integer, Byte>		redirectMap	= new HashMap<>();

	private List<RecalculateThread>	threadList	= new ArrayList<>();

	public Dispatcher(int threadNum) {
		this.threadNum = threadNum;
	}

	private static final Logger logger = LoggerFactory.getLogger(Dispatcher.class);

	public void start(RecordLog r) {
		for (int i = 0; i < threadNum; i++) {
			RecalculateThread thread = new RecalculateThread(Table.newTable(r), this);
			threadList.add(thread);
			thread.start();
		}
	}

	public void dispatch(RecordLog recordLog) {
		int id = recordLog.getPrimaryColumn().getLongValue();
		int oldId = recordLog.getPrimaryColumn().getBeforeValue();
		if (recordLog.isPKUpdate()) {
			threadList.get(hashFun(oldId)).submit(recordLog);
		} else {
			threadList.get(hashFun(id)).submit(recordLog);
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

	public int hashFun(int id) {
		int result = id % threadNum;
		if (result < 0)
			result = -result;
		return result;
	}

}
