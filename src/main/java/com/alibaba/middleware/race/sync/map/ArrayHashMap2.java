package com.alibaba.middleware.race.sync.map;

import java.nio.ByteBuffer;

import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

/**
 * Created by xiefan on 6/24/17.
 */
public class ArrayHashMap2 {

	private byte[][]			resultsArray;

	private int				headerLength	= 1;

	private int				recordLength;

	private int				totalLength;

	private Table				table;

	private boolean			log			= true;

	private static final Logger	logger		= LoggerFactory.getLogger(ArrayHashMap2.class);

	//[start,end);
	private int				startId;

	private int				endId;

	public ArrayHashMap2(Table table, int startId, int endId) {
		this.table = table;
		this.recordLength = 8 * table.getColumnSize();
		this.totalLength = this.recordLength + this.headerLength;
		this.startId = startId;
		this.endId = endId;
		resultsArray = new byte[endId - startId][this.totalLength];
	}

	public void newRecord(int id, ByteBuffer buffer) {
		//stat(id);
		id = getId(id);
		byte[] target = resultsArray[id];
		target[0] = (byte) 1;
		buffer.get(target, 1, recordLength);
	}

	public void remove(int id) {
		//stat(id);
		id = getId(id);
		resultsArray[id][0] = (byte) 0;
	}

	public void move(int oldId, int newId) {
		throw new RuntimeException("这个方法不应该被调用");
	}

	public void setColumn(int id, byte index, byte[] bytes, int off, int len) {
		id = getId(id);
		byte[] target;
		int offset = 0;
		target = this.resultsArray[id];
		offset = 1; //1个byte表示是否有值
		int tOff = index * 8 + offset;
		target[tOff++] = index;
		target[tOff++] = (byte) len;
		int end = off + len;
		for (int i = off; i < end; i++) {
			target[tOff++] = bytes[i];
		}
		//		System.arraycopy(bytes, off, target, tOff, len);
	}

	private byte[] newRecord() {
		return new byte[recordLength];
	}

	public byte[][] getResultsArray() {
		return resultsArray;
	}

	public byte[] getRecord(int pk){
		pk = getId(pk);
		return resultsArray[pk];
	}

	private int getId(int pk) {
		if (!inRange(pk))
			throw new RuntimeException(
					"Pk不在范围内. pk : " + pk + " start : " + startId + " end : " + endId);
		return pk - startId;
	}

	private boolean inRange(int pk) {
		if (pk >= startId && pk < endId)
			return true;
		return false;
	}
}
