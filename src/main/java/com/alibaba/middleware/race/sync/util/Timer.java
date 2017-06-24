package com.alibaba.middleware.race.sync.util;

/**
 * Created by xiefan on 6/24/17.
 */
public class Timer {
	public static void sleep(long ms, int ns) {
		try {
			Thread.sleep(ms, ns);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
