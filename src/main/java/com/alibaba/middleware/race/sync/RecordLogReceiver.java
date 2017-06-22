package com.alibaba.middleware.race.sync;

import java.util.Map;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 *
 */
public interface RecordLogReceiver {

	void received(Table table,Map<Integer, long[]> records, RecordLog record) throws Exception;
	
	void receivedFinal(Table table,Map<Integer, long[]> records, Map<Integer, long[]> records2) throws Exception;

}
