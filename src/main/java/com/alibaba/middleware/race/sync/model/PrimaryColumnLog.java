package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 */
public class PrimaryColumnLog extends ColumnLog {

	private int	beforeValue;

	private int	value;


	public boolean isPkChange() {
		return getLongValue() != getBeforeValue();
	}

	public int getBeforeValue() {
		return beforeValue;
	}

	public void setBeforeValue(int beforeValue) {
		this.beforeValue = beforeValue;
	}

	public int getLongValue() {
		return value;
	}

	public void setLongValue(int value) {
		this.value = value;
	}
}
