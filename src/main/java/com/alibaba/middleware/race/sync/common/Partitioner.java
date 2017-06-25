package com.alibaba.middleware.race.sync.common;


public interface Partitioner {
    int getPartitionId(long pk);
}
