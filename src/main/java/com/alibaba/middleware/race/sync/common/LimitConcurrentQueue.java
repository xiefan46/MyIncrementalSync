package com.alibaba.middleware.race.sync.common;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by wubincen on 2017/6/12.
 */
public class LimitConcurrentQueue<U> {
    private ConcurrentLinkedQueue<U> queue = new ConcurrentLinkedQueue<>();
    private AtomicInteger size = new AtomicInteger(0);
    public void add(U u) {
        size.incrementAndGet();
        queue.add(u);
    }

    public U poll() {
        U ret = queue.poll();
        if (ret != null) size.decrementAndGet();
        return ret;
    }

    public int size() {
        return size.get();
    }
}
