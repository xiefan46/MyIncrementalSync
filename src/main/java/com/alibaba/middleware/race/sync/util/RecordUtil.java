package com.alibaba.middleware.race.sync.util;

import com.alibaba.middleware.race.sync.model.Column;
import com.alibaba.middleware.race.sync.model.Record;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordUtil {

	private static final String FIELD_SEPERATOR = "\t";

	public static String formatResultString(Record record) {
		checkState(record.getAlterType() == Record.INSERT,
				"Fail to format result because of wrong alter type");
		StringBuilder sb = new StringBuilder();
		sb.append(record.getPrimaryColumn().getValue());
		for (Column c : record.getColumns().values()) {
			sb.append(FIELD_SEPERATOR);
			sb.append(c.getValue());
		}
		return sb.toString();
	}
}
