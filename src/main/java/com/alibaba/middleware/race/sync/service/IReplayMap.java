package com.alibaba.middleware.race.sync.service;

/**
 * Created by xiefan on 6/24/17.
 */
public interface IReplayMap {
    int addPk(long pk);
    void remove(long pk);
    void update(int offset, int index, long val);
    void update(int offset, int index, byte[] buf, int off, int len);
    int getOffset(long pk);
}
