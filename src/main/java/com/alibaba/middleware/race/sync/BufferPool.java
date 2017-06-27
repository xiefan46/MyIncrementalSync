package com.alibaba.middleware.race.sync;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferPool {

	private static final Logger				logger		= LoggerFactory
			.getLogger(BufferPool.class);

	private ConcurrentLinkedQueue<ByteBuffer>	pool			= new ConcurrentLinkedQueue<>();

	private String							name			= "";

	private AtomicInteger					allocateCount	= new AtomicInteger(0);

	private AtomicInteger					releaseCount	= new AtomicInteger(0);

	public BufferPool(int poolSize, int bufferSize) {
		for (int i = 0; i < poolSize; ++i) {
			pool.offer(ByteBuffer.allocate(bufferSize));
		}
		/*
		 * new Thread(new Runnable() {
		 * 
		 * @Override public void run() { try{
		 * Thread.currentThread().sleep(300);
		 * logger.info("pool name : {} allocate count : {} release count : {}"
		 * ,name, allocateCount.get(),releaseCount.get()); }catch (Exception
		 * e){ throw new RuntimeException(e); } } }).start();
		 */
	}

	public BufferPool(int poolSize, int bufferSize, String name) {
		this(poolSize, bufferSize);
		this.name = name;
	}

	private int count = 5;

	public ByteBuffer allocate() {
		allocateCount.incrementAndGet();
		ByteBuffer byteBuffer = pool.poll();
		while (byteBuffer == null) {
			try {
				Thread.currentThread().sleep(10);
				byteBuffer = pool.poll();
				/*
				 * if (count-- > 0)
				 * logger.info("buffer pool wait. pool name : {}",
				 * this.name);
				 */
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		return byteBuffer;
	}

	public void free(ByteBuffer buffer) {
		releaseCount.incrementAndGet();
		buffer.clear();
		pool.offer(buffer);
	}

	public int getSize() {
		return pool.size();
	}
}
