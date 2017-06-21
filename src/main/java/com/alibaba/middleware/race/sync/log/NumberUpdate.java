package com.alibaba.middleware.race.sync.log;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.NumberColumnLog;
import com.alibaba.middleware.race.sync.model.RecordLog;

/**
 * Created by xiefan on 6/21/17.
 */
public class NumberUpdate extends ChangeLog {

	private int newValue;

	private byte index;

	public static final int LEN = 10;

	public NumberUpdate(int pk, NumberColumnLog columnLog) {
		this.pk = pk;
		this.index = columnLog.getNameIndex();
        this.alterType = Constants.NUMBER_UPDATE;
        this.newValue = columnLog.getValue();
	}
}
