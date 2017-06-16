package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.RecordLog;

/**
 * @author wangkai
 *
 */
@Deprecated
public interface RecordLogReceiver {

	void received(Context context, RecordLog record) throws Exception;

}
