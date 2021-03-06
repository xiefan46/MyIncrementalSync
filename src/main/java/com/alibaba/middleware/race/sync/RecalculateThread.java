package com.alibaba.middleware.race.sync;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
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

	private volatile boolean		running		= true;

	private Logger		logger		= LoggerFactory.getLogger(getClass());
	
	private CountDownLatch		countDownLatch = new CountDownLatch(1);

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
		for (;;) {
			try {
				RecordLog r = queue.poll(16, TimeUnit.MICROSECONDS);
				if (r == null) {
					if (!running) {
						break;
					}
					continue;
				}
				all++;
				receiver.received(context, r);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
		countDownLatch.countDown();
		logger.info("all record:{}", all);
	}

	public void stop() {
		running = false;
	}

	public CountDownLatch getCountDownLatch() {
		return countDownLatch;
	}
}
