package com.alibaba.middleware.race.sync.entity;

import com.alibaba.middleware.race.sync.service.IReplayMap;

/**
 * Created by xiefan on 6/24/17.
 */
public class SendTask {
    private IReplayMap replayMap;
    private boolean end = false;
    private int partitionId;

    public SendTask(IReplayMap replayMap, int partitionId) {
        this.replayMap = replayMap;
        this.partitionId = partitionId;
    }

    private SendTask() {
        end = true;
    }

    public boolean isEnd() {
        return end;
    }

    public IReplayMap getReplayMap() {
        return replayMap;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public static final SendTask END_TASK = new SendTask();
}
