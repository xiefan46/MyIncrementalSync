package com.alibaba.middleware.race.sync.codec;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangkai
 *
 */
public class ByteArrayCache {

	private static ByteArrayCache instance = new ByteArrayCache();

	public static ByteArrayCache get() {
		return instance;
	}

	private Map<ByteArray, byte[]> map = new HashMap<>(1024 * 1024 * 16);

	public byte[] get(ByteArray2 array2) {
		byte[] res = map.get(array2);
		if (res == null) {
			synchronized (map) {
				res = map.get(array2);
				if (res != null) {
					return res;
				}
				res = array2.getBytes();
				map.put(new ByteArray(res), res);
				return res;
			}
		}
		return res;
	}

}
