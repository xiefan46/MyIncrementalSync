package com.alibaba.middleware.race.sync.common;


public class RangePartitioner implements Partitioner {
    private long[] partitionStart ;
    private long end;

    public RangePartitioner(long start, long end, int partitionNum) {
        long partitionLength = ((end - start + 1) / partitionNum);
        partitionStart = new long[partitionNum];
        partitionStart[0] = start;
        for (int i = 1; i < partitionNum; ++i) partitionStart[i] = partitionStart[i-1] + partitionLength;
        this.end = end;
    }

    @Override
    public int getPartitionId(long key) {
        int low = 0;
        int high = partitionStart.length - 1;
        while (low < high) {
            int mid = (low + high + 1) >> 1;
            if (partitionStart[mid] > key) high = mid - 1;
            else low = mid;
        }
        return low;
    }

    public long getStart(int partitionId) {
        return partitionStart[partitionId];
    }

    public long getEnd(int partitionId) {
        if (partitionId + 1 < partitionStart.length) return partitionStart[partitionId+1] - 1;
        return end;
    }

}
