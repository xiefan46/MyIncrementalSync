package com.alibaba.middleware.race.sync.common;

/**
 * Created by wubincen on 2017/6/23.
 */
public class HashPartitioner implements Partitioner{
    private int mask;

    public HashPartitioner(int partitionNum) {
        this.mask = partitionNum - 1;
    }

    @Override
    public int getPartitionId(long pk) {
        return (int) (pk & mask);
    }
}
