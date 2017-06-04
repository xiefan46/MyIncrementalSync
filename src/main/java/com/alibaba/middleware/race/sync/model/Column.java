package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 *
 */
public class Column {

	private static final byte	NUMBER_FLAG	= 0b01000000;
	private static final byte	PRIMARY_FLAG	= (byte) 0b10000000;
	// 列名
	private String				name;
	// 类型 类型主要分为1和2 1代表为数字类型，
	// 数字类型范围为0<= x <= 2^64-1 
	// 2代表为字符串类型，0<= len <= 65536
	// 类型:是否主键 是否主键:0或者1 (0代表否，1代表是) 
	private byte				flag;
	// 列值
	private Object				value;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isNumber() {
		return (flag & NUMBER_FLAG) != 0;
	}

	public void setNumber(boolean number) {
		if (number) {
			flag = (byte) (flag | NUMBER_FLAG);
		}
	}

	public boolean isPrimary() {
		return (flag & PRIMARY_FLAG) != 0;
	}

	public void setPrimary(boolean primary) {
		if (primary) {
			flag = (byte) (flag | PRIMARY_FLAG);
		}
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public byte getFlag() {
		return flag;
	}

}
