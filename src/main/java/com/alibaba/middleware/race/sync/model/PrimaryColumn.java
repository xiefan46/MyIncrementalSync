package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 */
public class PrimaryColumn extends Column {

	private long value;

	public long getLongValue() {
		return value;
	}

	public void setLongValue(long value) {
		this.value = value;
	}

}
