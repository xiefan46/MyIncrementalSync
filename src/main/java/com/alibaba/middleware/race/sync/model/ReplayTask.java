package com.alibaba.middleware.race.sync.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.sync.common.BufferPool;

/**
 * Created by xiefan on 6/24/17.
 */
public class ReplayTask {
    // bytebuffer的内容是序列化后的操作日志
    private List<ByteBuffer> list = new ArrayList<>();

    // 表示这堆bytebuffer是哪个池子分配出来的,用于回收
    private BufferPool pool;

    // 全局时间戳
    private long epoch;

    public ReplayTask(BufferPool pool, long epoch) {
        this.pool = pool;
        this.epoch = epoch;
    }

    public long getEpoch() {
        return epoch;
    }

    public List<ByteBuffer> getList() {
        return list;
    }

    public void addBuffer(ByteBuffer buffer) {
        list.add(buffer);
    }

    public static final ReplayTask END_TASK = new ReplayTask(null, -1);

    public boolean isEnd() { return pool == null; }

    public BufferPool getPool() {
        return pool;
    }
}

