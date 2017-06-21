package com.alibaba.middleware.race.sync.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.alibaba.middleware.race.sync.codec.ByteArray2;

/**
 * @author wangkai
 *
 */
public class Table {

	private final ThreadLocal<ByteArray2>	arrayLocal		= new ThreadLocal<ByteArray2>() {
														@Override
														protected ByteArray2 initialValue() {
															return new ByteArray2(
																	null, 0, 0);
														}
													};

	private Map<ByteArray2, Byte>			strColNameIndex	= new HashMap<>();

	private Map<ByteArray2, Byte>			numberColNameIndex	= new HashMap<>();

	private static short				globalId			= 0;

	private Map<ByteArray2, Short>		colValueToId		= new HashMap<>();

	private Map<Short, ByteArray2>		idToColValue		= new HashMap<>();

	private byte						colSize			= 0;

	private byte						numberColSize		= 0;

	private byte						strColSize		= 0;

	public Table() {

	}

	public void addCol(byte[] buffer, int offset, int length, boolean isNumber) {
		byte[] bytes = new byte[length];
		System.arraycopy(buffer, offset, bytes, 0, length);
		ByteArray2 array = new ByteArray2(bytes, 0, bytes.length);
		if (isNumber) {
			numberColNameIndex.put(array, numberColSize++);
		} else {
			strColNameIndex.put(array, strColSize++);
		}
	}

	public byte getColNameId(byte[] array, int offset, int length,boolean isNumber) {
		ByteArray2 array2 = arrayLocal.get().reset(array, offset, length);
		Map<ByteArray2,Byte> map = isNumber ? numberColNameIndex : strColNameIndex;
		Byte id = map.get(array2);
		if (id == null) {
			throw new RuntimeException("Fail to encode col name");
		}
		return id;
	}

	public short getStrColValueId(byte[] array, int offset, int length) {
		ByteArray2 array2 = arrayLocal.get().reset(array, offset, length);
		Short id = colValueToId.get(array2);
		if (id == null) {
			id = globalId++;
			byte[] bytes = new byte[length];
			System.arraycopy(array, offset, bytes, 0, length);
			colValueToId.put(new ByteArray2(bytes, 0, bytes.length), id);
			idToColValue.put(id, new ByteArray2(bytes, 0, bytes.length));
		}
		return id;
	}


	public ByteArray2 getColArrayById(short id) {
		return idToColValue.get(id);
	}

	public Record newRecord() {
		return new Record(strColSize,numberColSize);
	}

	public Map<ByteArray2, Short> getColValueToId() {
		return colValueToId;
	}

	public void setColValueToId(Map<ByteArray2, Short> colValueToId) {
		this.colValueToId = colValueToId;
	}

}
