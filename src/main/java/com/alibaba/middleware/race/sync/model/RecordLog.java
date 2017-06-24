package com.alibaba.middleware.race.sync.model;

import com.alibaba.middleware.race.sync.Constants;

/**
 * @author wangkai
 */
//FIXME 考虑合并schema-table
public class RecordLog {


	// 一个唯一的字符串编号,例子:000001:106
	//	private String			binaryId;
	// 数据变更发生的时间戳,毫秒精度
	//	private long			timestamp;
	//	// 数据变更对应的库名	
	//	private String			schema;
	//	// 数据变更对应的表名
	//	private String			table;

	private byte edit;
	//  I(1)代表insert, U(2)代表update, D(0)代表delete
	private byte		alterType;
	// 该记录的列信息
	private byte[]	columns;
	// 该记录的主键
	
	private int beforePk;
	private int pk;

	//	public long getTimestamp() {
	//		return timestamp;
	//	}
	//
	//	public void setTimestamp(long timestamp) {
	//		this.timestamp = timestamp;
	//	}

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
	
	public void setColumn(byte index,byte[] bytes, int off, int len){
		setColumn(columns, index, bytes, off, len);
	}
	
	public static void setColumn(byte [] target,byte index,byte[] bytes, int off, int len){
		int tOff = index * 8;
		target[tOff++] = index;
		target[tOff++] = (byte)len;
		int end = off + len;
		for (int i = off; i < end; i++) {
			target[tOff++] = bytes[i];
		}
//		System.arraycopy(bytes, off, target, tOff, len);
	}
	
	public static void setColumn1(byte [] target,byte index,byte[] bytes, int off, int len){
		int tOff = index * 8;
		target[tOff++] = index;
		target[tOff++] = (byte)len;
		System.arraycopy(bytes, off, target, tOff, len);
	}

	public void reset(){
		edit = 0;
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
