package com.alibaba.middleware.race.sync.common;

/**
 * Created by wubincen on 2017/6/23.
 */
public interface Partitioner {
    int getPartitionId(long pk);
}
