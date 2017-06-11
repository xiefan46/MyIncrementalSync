package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;

/**
 * @author wangkai
 *
 */
public interface RecordLogReceiver {

	void received(Context context, RecordLog record) throws Exception;

	void receivedFinal(Context context, long pk, Record record) throws Exception;

}
