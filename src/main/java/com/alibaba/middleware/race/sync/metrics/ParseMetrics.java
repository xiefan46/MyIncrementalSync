package com.alibaba.middleware.race.sync.metrics;

/**
 * Created by xiefan on 6/25/17.
 */
public class ParseMetrics {
    public long totalSize = 0;
    public long insertCount = 0;
    public long updateCount = 0;
    public long updatePkCount = 0;
    public long deleteCount = 0;
    public long valSize = 0;
    public long opSize = 0;
    public long kvCount = 0;
    public long relatedLineCount = 0;

    public long insertInRangeCount = 0;
    public long updateInRangeCount = 0;
    public long deleteInRangeCount = 0;
    public long updatePkOut2InCount = 0;
    public long updatePkIn2OutCount = 0;
    public long updatePkIn2InCount = 0;
    public long updatePkOut2OutCount = 0;

    public long maxPk = Long.MIN_VALUE;
    public long minPk = Long.MAX_VALUE;

    public long sleepTime = 0;

    public String desc() {
        StringBuilder sb = new StringBuilder();
        sb.append("[#]Parse Statistic:").append("\n")
            .append("Insert: ").append(insertCount).append("\n")
            .append("Update: ").append(updateCount).append("\n")
            .append("UpdatePk: ").append(updatePkCount).append("\n")
            .append("Delete: ").append(deleteCount).append("\n")
            .append("Val size: ").append(valSize).append("\n")
            .append("Op size: ").append(opSize).append("\n")
            .append("KV count: ").append(kvCount).append("\n")
            .append("Related count: ").append(relatedLineCount).append("\n")
            .append("maxPk: ").append(maxPk).append("\n")
            .append("minPk: ").append(minPk).append("\n")
            .append("sleep: ").append(sleepTime).append("\n")
            .append("insertInRange: ").append(insertInRangeCount).append("\n")
            .append("updateInRange: ").append(updateInRangeCount).append("\n")
            .append("deleteInRange: ").append(deleteInRangeCount).append("\n")
            .append("updatePkIn2Out: ").append(updatePkIn2OutCount).append("\n")
            .append("updatePkOut2In: ").append(updatePkOut2InCount).append("\n")
            .append("updatePkIn2In: ").append(updatePkIn2InCount).append("\n")
            .append("updatePkOut2Out: ").append(updatePkOut2OutCount).append("\n")
            .append("totalSize: ").append(totalSize).append("\n");
        return sb.toString();
    }

    public void merge(ParseMetrics parseMetrics) {
        insertCount += parseMetrics.insertCount;
        updateCount += parseMetrics.updateCount;
        updatePkCount += parseMetrics.updatePkCount;
        deleteCount += parseMetrics.deleteCount;
        valSize += parseMetrics.valSize;
        opSize += parseMetrics.opSize;
        kvCount += parseMetrics.kvCount;
        relatedLineCount += parseMetrics.relatedLineCount;
        maxPk = Math.max(maxPk, parseMetrics.maxPk);
        minPk = Math.min(minPk, parseMetrics.minPk);
        sleepTime += parseMetrics.sleepTime;
        updatePkIn2OutCount += parseMetrics.updatePkIn2OutCount;
        updateInRangeCount += parseMetrics.updateInRangeCount;
        updatePkIn2InCount += parseMetrics.updatePkIn2InCount;
        updatePkOut2InCount += parseMetrics.updatePkOut2InCount;
        updatePkOut2OutCount += parseMetrics.updatePkOut2OutCount;
        insertInRangeCount += parseMetrics.insertInRangeCount;
        deleteInRangeCount += parseMetrics.deleteInRangeCount;
        totalSize += parseMetrics.totalSize;
    }

    public void updatePk(long pk) {
        if (maxPk < pk) maxPk = pk;
        if (minPk > pk) minPk = pk;
    }
}
