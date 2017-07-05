package com.alibaba.middleware.race.sync.util;

import com.carrotsearch.hppc.IntByteHashMap;

public class MyIntByteHashMap extends IntByteHashMap {

	public MyIntByteHashMap(int expectedElements) {
		super(expectedElements);
	}

	public byte remove(int key, byte defaultValue) {
		final int mask = this.mask;
		if (((key) == 0)) {
			hasEmptyKey = false;
			byte previousValue = values[mask + 1];
			values[mask + 1] = ((byte) 0);
			return previousValue;
		} else {
			final int[] keys = this.keys;
			int slot = hashKey(key) & mask;

			int existing;
			while (!((existing = keys[slot]) == 0)) {
				if (((existing) == (key))) {
					final byte previousValue = values[slot];
					shiftConflictingKeys(slot);
					return previousValue;
				}
				slot = (slot + 1) & mask;
			}

			return (defaultValue);
		}
	}

}
