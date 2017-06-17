package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 */
public class PrimaryColumnLog extends ColumnLog {

	private long	beforeValue;

	private long	value;

	public long getBeforeValue() {
		return beforeValue;
	}

	public void setBeforeValue(long beforeValue) {
		this.beforeValue = beforeValue;
	}

	public boolean isPkChange() {
		return getLongValue() != getBeforeValue();
	}

	public long getLongValue() {
		return value;
	}

	public void setLongValue(long value) {
		this.value = value;
	}

}
