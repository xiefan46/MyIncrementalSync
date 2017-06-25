package com.alibaba.middleware.race.sync.common;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferPool {

	private static Logger					logger	= LoggerFactory
			.getLogger(BufferPool.class);

	private ConcurrentLinkedQueue<ByteBuffer>	pool		= new ConcurrentLinkedQueue<>();

	public BufferPool(int poolSize, int bufferSize) {
		for (int i = 0; i < poolSize; ++i) {
			pool.offer(ByteBuffer.allocate(bufferSize));
		}
		logger.info("Buffer pool init");
		//        stat.info("buffer pool cost {} MB", (poolSize * (long)bufferSize) >> 20);
	}

	public BufferPool(int poolSize, int bufferSize, boolean direct) {
		for (int i = 0; i < poolSize; ++i) {
			if (!direct)
				pool.offer(ByteBuffer.allocate(bufferSize));
			else
				pool.offer(ByteBuffer.allocateDirect(bufferSize));
		}
	}

	public ByteBuffer getBuffer() {
		return pool.poll();
	}

	public void freeBuffer(ByteBuffer buffer) {
		buffer.clear();
		pool.offer(buffer);
	}

	public int getSize() {
		return pool.size();
	}
}
