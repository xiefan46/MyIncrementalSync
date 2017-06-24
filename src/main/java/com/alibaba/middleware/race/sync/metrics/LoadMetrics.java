package com.alibaba.middleware.race.sync.metrics;

/**
 * Created by xiefan on 6/25/17.
 */
public class LoadMetrics {
    public int lineCount = 0;
    public int sleepTime = 0;
    public int totalSize = 0;

    public String desc() {
        return String.format("load %d lines, sleep %d times, total %d B", lineCount, sleepTime, totalSize);
    }

    public void merge(LoadMetrics loadMetrics) {
        lineCount += loadMetrics.lineCount;
        sleepTime += loadMetrics.sleepTime;
        totalSize += loadMetrics.totalSize;
    }
}
