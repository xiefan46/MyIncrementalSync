package com.alibaba.middleware.race.sync.common;


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
