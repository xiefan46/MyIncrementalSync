package com.alibaba.middleware.race.sync.common;

import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.util.TreeMap;

/**
 * Created by xiefan on 6/26/17.
 */
public class RangeSearcher {

	TreeMap<Integer, Integer>	map	= new TreeMap<>();

	private int				startId;

	private int				endId;

	private int				count;

	private int				threadNum;

	//[startId,endId)
	public RangeSearcher(int startId, int endId, int threadNum) {
		this.startId = startId;
		this.endId = endId;
		int total = endId - startId;
		this.threadNum = threadNum;
		this.count = total / threadNum; //多出来的部分直接补给最后一个区间
		for (int i = 0; i < threadNum; i++) {
			System.out.println("i : " + i + " start : " + (startId + i * count));
			map.put(startId + i * count, i);
		}
	}

	public int searchForDealThread(int pk) {
		if (pk < startId || pk >= endId)
			throw new RuntimeException("id 不在范围内. id : " + pk);
		return map.floorEntry(pk).getValue();
	}

	public int getStartId(int threadId) {
		return startId + threadId * count;
	}

	public int getEndId(int threadId) {
		if (threadId == threadNum - 1)
			return endId;
		return getStartId(threadId + 1);
	}

}
