package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 */
public class Record {

	private short[] strCols;

	private int[] numberCols;

	public Record(int strCol,int numberCol) {
		this.strCols = new short[strCol];
		this.numberCols = new int[numberCol];
	}

	public short[] getStrCols() {
		return strCols;
	}

	public void setStrCols(short[] strCols) {
		this.strCols = strCols;
	}

	public int[] getNumberCols() {
		return numberCols;
	}

	public void setNumberCols(int[] numberCols) {
		this.numberCols = numberCols;
	}
}
