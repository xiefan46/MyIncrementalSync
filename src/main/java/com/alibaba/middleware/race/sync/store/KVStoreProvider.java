package com.alibaba.middleware.race.sync.store;

import com.alibaba.middleware.race.sync.model.Record;

import java.util.Map;

/**
 * Created by xiefan on 6/12/17.
 */
public interface KVStoreProvider {

	Map<Long, Record> provide();
}
