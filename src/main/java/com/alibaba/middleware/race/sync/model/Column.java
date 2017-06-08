package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 *
 */
public class Column {

	private long	lastUpdate;

	private byte[]	value;

	public void setValue(byte[] bytes) {
		this.value = bytes;
	}

	public byte[] getValue() {
		return value;
	}

	public long getLastUpdate() {
		return lastUpdate;
	}

	public void setLastUpdate(long lastUpdate) {
		this.lastUpdate = lastUpdate;
	}

}
