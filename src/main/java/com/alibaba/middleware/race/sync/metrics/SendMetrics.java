package com.alibaba.middleware.race.sync.metrics;

/**
 * Created by xiefan on 6/25/17.
 */
public class SendMetrics {
    public int ansCount = 0;

    public String desc() {
        StringBuilder sb = new StringBuilder();
        sb.append("ansCount: ").append(ansCount).append("\n");
        return sb.toString();
    }

    public void merge(SendMetrics sendMetrics) {
        ansCount += sendMetrics.ansCount;
    }
}
