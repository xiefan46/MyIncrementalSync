package com.alibaba.middleware.race.sync.model;

import com.alibaba.middleware.race.sync.Constants;

/**
 * @author wangkai
 */
public class RecordLog {

	private byte edit;
	//  I(1)代表insert, U(2)代表update, D(0)代表delete
	private byte		alterType;
	// 该记录的列信息
	private byte[]	columns;
	// 该记录的主键
	
	private int beforePk;
	private int pk;
	
	private boolean read;
	
	public boolean isRead() {
		return read;
	}

	public void setRead(boolean read) {
		this.read = read;
	}

	public byte getAlterType() {
		return alterType;
	}

	public void setAlterType(byte alterType) {
		this.alterType = alterType;
	}

	public byte getColumn() {
		return edit++;
	}
	
	public byte[] getColumns() {
		return columns;
	}

	public void newColumns(int cols) {
		this.columns = new byte[cols * 8];
	}
	
	public void setColumn(byte index,byte name,byte[] bytes, int off, int len){
		setColumn(columns, index,name, bytes, off, len);
	}
	
	public static void setColumn(byte [] target,byte index,byte name,byte[] bytes, int off, int len){
		int tOff = index * 8;
		target[tOff++] = name;
		target[tOff++] = (byte)len;
		int end = off + len;
		for (int i = off; i < end; i++) {
			target[tOff++] = bytes[i];
		}
	}
	
	public static void setColumn1(byte [] target,byte index,byte name,byte[] bytes, int off, int len){
		int tOff = index * 8;
		target[tOff++] = name;
		target[tOff++] = (byte)len;
		System.arraycopy(bytes, off, target, tOff, len);
	}

	public void reset(){
		edit = 0;
		read = false;
	}
	
	public int getEdit() {
		return edit;
	}

	public boolean isPkUpdate() {
		return alterType == Constants.PK_UPDATE;
	}

	public boolean isPkUpdate4Codec() {
		return beforePk != pk;
	}
	
	public static RecordLog newRecordLog(int cols){
		RecordLog r = new RecordLog();
		r.newColumns(cols);
		return r;
	}

	public int getBeforePk() {
		return beforePk;
	}

	public int getPk() {
		return pk;
	}

	public void setBeforePk(int beforePk) {
		this.beforePk = beforePk;
	}

	public void setPk(int pk) {
		this.pk = pk;
	}

}
