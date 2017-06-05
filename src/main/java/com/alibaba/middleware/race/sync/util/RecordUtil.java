package com.alibaba.middleware.race.sync.util;

import com.alibaba.middleware.race.sync.model.Record;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordUtil {

	public String formatToResult(Record record) {
		checkState(record.getAlterType() == Record.INSERT,
				"Fail to format result because of wrong alter type");
		return null;
	}
}
