package com.alibaba.middleware.race.sync.common;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wubincen on 2017/6/16.
 */
public class BufferPool {
    private static Logger stat = LoggerFactory.getLogger("stat");
    private ConcurrentLinkedQueue<ByteBuffer> pool = new ConcurrentLinkedQueue<>();

    public BufferPool(int poolSize, int bufferSize) {
        for (int i = 0; i < poolSize; ++i) {
            pool.offer(ByteBuffer.allocate(bufferSize));
        }
//        stat.info("buffer pool cost {} MB", (poolSize * (long)bufferSize) >> 20);
    }

    public BufferPool(int poolSize, int bufferSize, boolean direct) {
        for (int i = 0; i < poolSize; ++i) {
            if (!direct)
            pool.offer(ByteBuffer.allocate(bufferSize));
            else pool.offer(ByteBuffer.allocateDirect(bufferSize));
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
