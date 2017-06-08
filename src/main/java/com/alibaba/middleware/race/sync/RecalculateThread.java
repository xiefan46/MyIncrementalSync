package com.alibaba.middleware.race.sync;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.model.RecordLog;

/**
 * @author wangkai
 *
 */
public class RecalculateThread implements Runnable {

	private RecalculateContext	context;

	private volatile boolean		running = true;

	private static Logger		logger	= LoggerFactory.getLogger(RecalculateThread.class);

	public RecalculateThread(RecalculateContext context) {
		this.context = context;
	}

	@Override
	public void run() {
		execute(context);
	}

	private void execute(RecalculateContext context) {
		RecordLogReceiver receiver = context.getRecordLogReceiver();
		BlockingQueue<RecordLog> queue = context.getRecordLogs();
		int all = 0;
		for (; running;) {
			try {
				RecordLog r = queue.poll(8, TimeUnit.MICROSECONDS);
				if (r == null) {
					continue;
				}
				all++;
				receiver.received(context, r);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
		logger.info("all record:{}",all);
	}

	public void stop() {
		running = false;
	}

}
