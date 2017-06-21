package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 */
public class Record {

	private Short[]	strCols;

	private Integer[]	numberCols;

	private byte		count;

	private Integer finalPk;

	private byte alterType = 0;

	public Record(int strCol, int numberCol) {
		this.strCols = new Short[strCol];
		this.numberCols = new Integer[numberCol];
		count = (byte) (strCol + numberCol);
	}

	public Short[] getStrCols() {
		return strCols;
	}

	public void setStrCols(Short[] strCols) {
		this.strCols = strCols;
	}

	public Integer[] getNumberCols() {
		return numberCols;
	}

	public void setNumberCols(Integer[] numberCols) {
		this.numberCols = numberCols;
	}

	public void countDown() {
		count--;
	}

	public boolean dealOver() {
		return count <= 0;
	}

	public Integer getFinalPk() {
		return finalPk;
	}

	public void setFinalPk(Integer finalPk) {
		this.finalPk = finalPk;
	}

	public byte getAlterType() {
		return alterType;
	}

	public void setAlterType(byte alterType) {
		this.alterType = alterType;
	}
}
