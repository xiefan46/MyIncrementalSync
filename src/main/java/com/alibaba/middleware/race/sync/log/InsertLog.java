package com.alibaba.middleware.race.sync.log;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.NumberColumnLog;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.StringColumnLog;

/**
 * Created by xiefan on 6/21/17.
 */
public class InsertLog extends ChangeLog {

	public static int	STR_VALUE_NUM		= 0;

	public static int	NUMBER_VALUE_NUM	= 0;

	public static int	LEN;

	private short[]	strValues;

	private int[]		numberValues;

	public static void setLen(int strNum, int numberNum) {
		STR_VALUE_NUM = strNum;
		NUMBER_VALUE_NUM = numberNum;
		LEN = STR_VALUE_NUM * 2 + NUMBER_VALUE_NUM * 4 + 1 + 4;
	}

	public InsertLog(RecordLog recordLog) {
		this.pk = recordLog.getPrimaryColumn().getLongValue();
		this.alterType = Constants.INSERT;
		this.strValues = new short[STR_VALUE_NUM];
		this.numberValues = new int[NUMBER_VALUE_NUM];
		for (ColumnLog c : recordLog.getColumns()) {
			if (c.isNumberCol()) {
				NumberColumnLog numberColumnLog = (NumberColumnLog) c;
				numberValues[numberColumnLog.getNameIndex()] = numberColumnLog.getValue();
			} else {
				StringColumnLog stringColumnLog = (StringColumnLog) c;
				strValues[stringColumnLog.getNameIndex()] = stringColumnLog.getId();
			}

		}
	}
}
