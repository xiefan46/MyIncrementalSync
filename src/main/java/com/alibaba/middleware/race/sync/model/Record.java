package com.alibaba.middleware.race.sync.model;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author wangkai
 */
public class Record implements Serializable{

	private byte[][] columns;

	public Record(int cols) {
		this.columns = new byte[cols][];
	}

	public void setColum(int index, byte[] data) {
		columns[index] = data;
	}

	public byte[][] getColumns() {
		return columns;
	}

	@Override
	public String toString() {
		String result="";
		for(byte[] bytes:columns) {
			result+=Arrays.toString(bytes);
			result+="\n";
		}
		return result;
	}
}
