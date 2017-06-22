package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.RecordLogs;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by xiefan on 6/23/17.
 */
public class MergeThread implements Runnable {

	private Queue<RecordLogs>	logQueue	= new ConcurrentLinkedQueue<>();

	private boolean			stop		= false;

	private int				count;

	@Override
	public void run() {
		try {
			while (!stop || !logQueue.isEmpty()) {
				while (!logQueue.isEmpty()) {
					RecordLogs data = logQueue.poll();
					if (data != null) {
						data.setLogs(null);
						count++;
						System.out.println("deal block count : " + count);
					}
				}
				Thread.currentThread().sleep(100);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void submit(RecordLogs logs) {
		this.logQueue.add(logs);
	}

	public boolean isStop() {
		return stop;
	}

	public void setStop(boolean stop) {
		this.stop = stop;
	}

}
