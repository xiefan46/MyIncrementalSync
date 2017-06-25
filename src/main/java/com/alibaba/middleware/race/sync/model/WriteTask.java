package com.alibaba.middleware.race.sync.model;

/**
 * Created by xiefan on 6/24/17.
 */
public class WriteTask {
    private int partitionId;
    private String ans;

    public WriteTask(int partitionId, String ans) {
        this.partitionId = partitionId;
        this.ans = ans;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getAns() {
        return ans;
    }

    public boolean isEnd() {
        return partitionId == -1;
    }

    public static final WriteTask END_TASK = new WriteTask(-1, null);
}
