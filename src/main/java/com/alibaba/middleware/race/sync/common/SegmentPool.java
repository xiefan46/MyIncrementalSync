package com.alibaba.middleware.race.sync.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.middleware.race.sync.Config;

/**
 * Created by wubincen on 2017/6/19.
 */
public class SegmentPool {

	public static final int		SEGMENT_SIZE	= (1 << 20);

	private int				counter		= 0;
	private Map<Integer, Segment>	map			= new ConcurrentHashMap<>();

	public synchronized Segment newSegment() {
		Segment ret = new Segment(SEGMENT_SIZE, counter);
		map.put(counter++, ret);
		return ret;
	}

	public Segment getSegment(int segmentId) {
		return map.get(segmentId);
	}
}
