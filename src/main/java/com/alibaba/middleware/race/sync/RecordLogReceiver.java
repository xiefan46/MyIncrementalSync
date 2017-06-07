package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.Record;

/**
 * @author wangkai
 *
 */
public interface RecordLogReceiver {

	void received(Context context, Record record, long startId, long endId) throws Exception;

	void receivedFinal(Context context, Record record, long startId, long endId) throws Exception;

}
