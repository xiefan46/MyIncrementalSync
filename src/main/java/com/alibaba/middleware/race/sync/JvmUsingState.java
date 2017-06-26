package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.generallycloud.baseio.common.ThreadUtil;

/**
 * @author wangkai
 *
 */
public class JvmUsingState implements Runnable {

	private static final long	M		= 1024 * 1024;

	private static Logger		logger	= LoggerFactory.getLogger(JvmUsingState.class);

	@Override
	public void run() {
		for (;;) {
			ThreadUtil.sleep(4000);
			print();
		}
	}

	public static void print() {

		Runtime runtime = Runtime.getRuntime();

		long free = runtime.freeMemory() / M;
		long all = runtime.totalMemory() / M;
		long max = runtime.maxMemory() / M;

		logger.info("free:{}\tall:{}\tmax:{}", free, all, max);
	}

}
