package com.alibaba.middleware.race.sync;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.util.LoggerUtil;

/**
 * @author wangkai
 *
 */
public class RecalculateThread implements Runnable {

	private RecalculateContext	context;

	private volatile boolean		running		= true;

	private static Logger		logger		= LoggerUtil.SERVER_LOGGER;

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
				RecordLog r = queue.poll(8, TimeUnit.MICROSECONDS);
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
		logger.info("all record:{}", all);
	}

	public void stop() {
		running = false;
	}

}
