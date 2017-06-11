package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 */
public class Record {

	private byte		alterType;

	private byte[][]	columns;

	private long		oldPk;

	private boolean	isPkUpdate	= false;

	public Record(int cols) {
		this.columns = new byte[cols][];
	}

	public void setColum(int index, byte[] data) {
		columns[index] = data;
	}

	public byte[][] getColumns() {
		return columns;
	}

	public byte getAlterType() {
		return alterType;
	}

	public void setAlterType(byte alterType) {
		this.alterType = alterType;
	}

	public long getOldPk() {
		return oldPk;
	}

	public void setOldPk(long oldPk) {
		this.oldPk = oldPk;
	}

	public boolean isPkUpdate() {
		return isPkUpdate;
	}

	public void setPkUpdate(boolean pkUpdate) {
		isPkUpdate = pkUpdate;
	}
}
