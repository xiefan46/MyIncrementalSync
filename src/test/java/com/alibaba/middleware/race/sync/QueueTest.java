package com.alibaba.middleware.race.sync;

import org.junit.Test;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

/**
 * Created by xiefan on 6/17/17.
 */
public class QueueTest {
	@Test
	public void test() throws Exception {
		final Queue<Integer> queue = new ConcurrentLinkedQueue<>();
		final CountDownLatch latch = new CountDownLatch(1);
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					while (!queue.isEmpty()) {
						System.out.println(queue.poll());
                        try {
                            Thread.currentThread().sleep(5);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
					try {
						Thread.currentThread().sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}).start();
		for (int i = 0; i < 10000; i++) {
			queue.add(i);
		}
		latch.await();
	}
}
