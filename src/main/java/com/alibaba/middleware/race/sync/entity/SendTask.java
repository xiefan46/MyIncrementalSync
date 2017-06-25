package com.alibaba.middleware.race.sync.entity;

import com.alibaba.middleware.race.sync.map.RecordMap;

/**
 * Created by xiefan on 6/24/17.
 */
public class SendTask {
    private RecordMap replayMap;
    private boolean end = false;
    private int partitionId;

    public SendTask(RecordMap replayMap, int partitionId) {
        this.replayMap = replayMap;
        this.partitionId = partitionId;
    }

    private SendTask() {
        end = true;
    }

    public boolean isEnd() {
        return end;
    }

    public RecordMap getReplayMap() {
        return replayMap;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public static final SendTask END_TASK = new SendTask();
}
