package com.alibaba.middleware.race.sync;

import java.util.Map;

import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.alibaba.middleware.race.sync.util.ObjectPool;

/**
 * @author wangkai
 *
 */
public interface RecordLogReceiver {

	void received(ObjectPool<Record> pool,Table table,Map<Integer, Record> records, RecordLog record) throws Exception;
	
	void receivedFinal(ObjectPool<Record> pool,Table table,Map<Integer, Record> records, Map<Integer, Record> records2) throws Exception;

}
