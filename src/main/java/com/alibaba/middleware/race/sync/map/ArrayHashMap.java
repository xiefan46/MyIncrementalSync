package com.alibaba.middleware.race.sync.map;

import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by xiefan on 6/24/17.
 */
public class ArrayHashMap {

	private List<Set<Integer>>	allId		= new ArrayList<>();

	private Map<Integer, byte[]>	resultsMap	= new HashMap<>();

	private byte[][]			resultsArray;

	public static final int		MAX_NUMBER	= 15000000;

	private int				headerLength	= 1;

	private int				recordLength;

	private int				totalLength;

	private Table				table;

	private boolean			log			= true;

	private static final Logger	logger		= LoggerFactory.getLogger(ArrayHashMap.class);

	public ArrayHashMap(Table table) {
		this.table = table;
		this.recordLength = 8 * table.getColumnSize();
		this.totalLength = this.recordLength + this.headerLength;
		resultsArray = new byte[MAX_NUMBER][this.totalLength];
		for (int i = 0; i < 10; i++) {
			allId.add(new TreeSet<Integer>());
		}
	}

	public void newRecord(int id) {
		stat(id);
		if (id < 0) {
			if (log) {
				logger.info("id < 0. ignore");
				log = false;
			}
			id = Math.abs(id);
		}
		if (id < MAX_NUMBER) {
			resultsArray[id][0] = (byte) 1;
		} else {
			resultsMap.put(id, newRecord());
		}
	}

	public void remove(int id) {
		stat(id);
		if (id < 0) {
			if (log) {
				logger.info("id < 0. ignore");
				log = false;
			}
			id = Math.abs(id);
		}
		if (id < MAX_NUMBER) {
			resultsArray[id][0] = (byte) 0;
		} else {
			resultsMap.remove(id);
		}
	}

	public void move(int oldId, int newId) {
		stat(oldId);
		stat(newId);
		if (oldId < 0 || newId < 0) {
			oldId = Math.abs(oldId);
			newId = Math.abs(newId);
			if (log) {
				logger.info("id < 0. ignore");
				log = false;
			}
		}
		if (oldId < MAX_NUMBER) {
			if (newId < MAX_NUMBER) {
				System.arraycopy(resultsArray[oldId], 0, resultsArray[newId], 0, totalLength);
			} else {
				byte[] result = newRecord();
				System.arraycopy(resultsArray[oldId], 1, result, 0, recordLength);
				resultsMap.put(newId, result);
			}
			resultsArray[oldId][0] = (byte) 0;

		} else {
			byte[] result = resultsMap.remove(oldId);
			if (result == null) {
				throw new RuntimeException("null record in pk update");
			}
			if (newId < MAX_NUMBER) {
				resultsArray[newId][0] = (byte) 1;
				System.arraycopy(result, 0, resultsArray[newId], 1, recordLength);
			} else {
				resultsMap.put(newId, result);
			}

		}
	}

	public void setColumn(int id, byte index, byte[] bytes, int off, int len) {
		if (id < 0) {
			if (log) {
				logger.info("id < 0. ignore");
				log = false;
			}
			id = Math.abs(id);
		}
		byte[] target;
		int offset = 0;
		if (id < MAX_NUMBER) {
			target = this.resultsArray[id];
			offset = 1; //1个byte表示是否有值
		} else {
			target = resultsMap.get(id);
		}
		int tOff = index * 8 + offset;
		target[tOff++] = index;
		target[tOff++] = (byte) len;
		int end = off + len;
		for (int i = off; i < end; i++) {
			target[tOff++] = bytes[i];
		}
		//		System.arraycopy(bytes, off, target, tOff, len);
	}

	//性能低,慎用
	private byte[] get(int id) {
		if (id < 0) {
			id = Math.abs(id);
		}
		if (id < MAX_NUMBER) {
			byte[] result = newRecord();
			System.arraycopy(resultsArray[id], 1, result, 0, recordLength);
			return result;
		} else {
			return resultsMap.get(id);
		}
	}

	private byte[] newRecord() {
		return new byte[recordLength];
	}

	public byte[][] getResultsArray() {
		return resultsArray;
	}

	private void stat(int id) { //统计1-5000W的id分布
		if (id < 0)
			return;
		int num = id / 5000000;
		if (num >= allId.size())
			num = allId.size() - 1;
		allId.get(num).add(id);
	}

	public String printIdStat() {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for (int i = 0; i < allId.size(); i++) {
			sb.append("i : " + allId.get(i).size() + " ");
		}
		sb.append("]");
		return sb.toString();
	}
}
