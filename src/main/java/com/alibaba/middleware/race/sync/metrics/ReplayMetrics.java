package com.alibaba.middleware.race.sync.metrics;

/**
 * Created by xiefan on 6/24/17.
 */
public class ReplayMetrics {
    public long totalSize = 0;
    public int ansCount = 0;

    public String desc() {
        StringBuilder sb = new StringBuilder();
        sb.append("op totalSize: ").append(totalSize).append("\n")
            .append("ans count: ").append(ansCount).append("\n");
        return sb.toString();
    }

    public void merge(ReplayMetrics replayMetrics) {
        totalSize += replayMetrics.totalSize;
        ansCount += replayMetrics.ansCount;
    }
}
