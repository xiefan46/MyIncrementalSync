package com.alibaba.middleware.race.sync.log;

import com.alibaba.middleware.race.sync.Constants;

/**
 * Created by xiefan on 6/21/17.
 */
public class PKUpdate extends ChangeLog {

	int oldPk;

	public static final int LEN = 9;

	public PKUpdate(int pk, int oldPk) {
		this.pk = pk;
		this.oldPk = oldPk;
		this.alterType = Constants.PK_UPDATE;
	}
}
