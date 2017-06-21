package com.alibaba.middleware.race.sync.log;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.model.StringColumnLog;

/**
 * Created by xiefan on 6/21/17.
 */
public class StringUpdate extends ChangeLog {

	public static final int	LEN	= 8;

	private short			newValue;

	private byte index;

	public StringUpdate(int pk, StringColumnLog columnLog) {
		this.pk = pk;
		this.index = columnLog.getNameIndex();
		this.alterType = Constants.STR_UPDATE;
		this.newValue = columnLog.getId();
	}
}
