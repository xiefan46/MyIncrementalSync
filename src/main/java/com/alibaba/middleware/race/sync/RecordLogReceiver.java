package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.RecordLog;

/**
 * @author wangkai
 *
 */
public interface RecordLogReceiver {

	void received(Context context, RecordLog record) throws Exception;

}
