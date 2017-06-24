package com.alibaba.middleware.race.sync.entity;

import java.nio.ByteBuffer;

/**
 * Created by xiefan on 6/24/17.
 */
public class ParseTask {
    private ByteBuffer buffer;
    private long epoch;

    public ParseTask(ByteBuffer buffer, long epoch) {
        this.buffer = buffer;
        this.epoch = epoch;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public long getEpoch() {
        return epoch;
    }

    public boolean isEnd() { return buffer == null; }

    public static final ParseTask END_TASK = new ParseTask(null, -1);
}
