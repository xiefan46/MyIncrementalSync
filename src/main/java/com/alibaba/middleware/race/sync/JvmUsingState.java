package com.alibaba.middleware.race.sync;


import com.alibaba.middleware.race.sync.util.LoggerUtil;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.ThreadUtil;

/**
 * @author wangkai
 *
 */
public class JvmUsingState implements Runnable {

	private static final long	M		= 1024 * 1024;

	private static Logger		logger	= LoggerUtil.get();

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

		logger.info("free:{}\tall:{}\tmax:{}", new Object[]{ free, all, max});
	}

}
