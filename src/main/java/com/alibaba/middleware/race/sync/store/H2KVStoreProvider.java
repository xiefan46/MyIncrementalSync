package com.alibaba.middleware.race.sync.store;

import com.alibaba.middleware.race.sync.model.Record;

import java.util.Map;

/**
 * Created by xiefan on 6/12/17.
 */
public class H2KVStoreProvider implements KVStoreProvider{
    @Override
    public Map<Long, Record> provide() {
        //包装h2代码
        return null;
    }
}
