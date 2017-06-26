package com.alibaba.middleware.race.sync.common;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferPool {

	private static Logger					logger	= LoggerFactory
			.getLogger(BufferPool.class);

	private ConcurrentLinkedQueue<ByteBuffer>	pool		= new ConcurrentLinkedQueue<>();

	private String							name		= "";

	public BufferPool(int poolSize, int bufferSize) {
		for (int i = 0; i < poolSize; ++i) {
			pool.offer(ByteBuffer.allocate(bufferSize));
		}
		//logger.info("Buffer pool init");
		//        stat.info("buffer pool cost {} MB", (poolSize * (long)bufferSize) >> 20);
	}

	public BufferPool(int poolSize, int bufferSize, String name) {
		this(poolSize, bufferSize);
		this.name = name;
	}

	public ByteBuffer getBuffer() {
		return pool.poll();
	}

	private int count = 5;

	public ByteBuffer getBufferWait() {
		ByteBuffer byteBuffer = pool.poll();
		while (byteBuffer == null) {
			try {
				Thread.currentThread().sleep(10);
				byteBuffer = pool.poll();
				if (count-- > 0)
					logger.info("buffer pool wait. pool name : {}", this.name);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		return byteBuffer;
	}

	public void freeBuffer(ByteBuffer buffer) {
		buffer.clear();
		pool.offer(buffer);
	}

	public int getSize() {
		return pool.size();
	}
}