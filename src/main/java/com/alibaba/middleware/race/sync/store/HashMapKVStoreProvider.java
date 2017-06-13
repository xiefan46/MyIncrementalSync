package com.alibaba.middleware.race.sync.store;

import com.alibaba.middleware.race.sync.model.Record;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xiefan on 6/12/17.
 */
public class HashMapKVStoreProvider implements KVStoreProvider {
	@Override
	public Map<Long, Record> provide() {
		return new HashMap<Long, Record>();
	}
}
