/*
 * Copyright 2015-2017 GenerallyCloud.com
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.middleware.race.sync.util;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * @author wangkai
 *
 */
public class RecordMap {

	private int				cols;

	private int				recordLen;

	private byte[]				data;

	private int[]				powers;

	private AtomicIntegerArray	lock;

	private int[]				versions;

	private int				capacity;

	private int				off;

	public RecordMap(int capacity, int off, int cols) {
		this.off = off;
		this.cols = cols;
		this.capacity = capacity;
		this.init(capacity, cols);
	}

	private void init(int capacity, int cols) {
		this.recordLen = 8 * cols;
		this.data = new byte[capacity * recordLen];
		this.versions = new int[cols * capacity];
		this.powers = new int[capacity];
		this.lock = new AtomicIntegerArray(capacity);
	}

	public int getResult(int pk) {
		int idx = ix(pk);
		if (powers[idx] == 1) {
			return idx * recordLen;
		}
		return -1;
	}

	public void powerIncrement(int pk) {
		int idx = ix(pk);
		lockRecordByIdx(idx);
		powers[idx]++;
		releaseRecordLockByIdx(idx);
	}

	public void powerDecrement(int pk) {
		int idx = ix(pk);
		lockRecordByIdx(idx);
		powers[idx]--;
		releaseRecordLockByIdx(idx);
	}

	private int ix(int pk) {
		return pk - off;
	}

	private void lockRecordByIdx(int idx) {
		AtomicIntegerArray lock = this.lock;
		try {
			if (!lock.compareAndSet(idx, 0, 1)) {
				for (; lock.compareAndSet(idx, 0, 1);) {
					Thread.currentThread().sleep(10);
				}
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

	}

	public void lockRecord(int pk) {
		int idx = ix(pk);
		AtomicIntegerArray lock = this.lock;
		try {
			if (!lock.compareAndSet(idx, 0, 1)) {
				for (; lock.compareAndSet(idx, 0, 1);) {
					Thread.currentThread().sleep(10);
				}
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void releaseRecordLock(int pk) {
		lock.set(ix(pk), 0);
	}

	private void releaseRecordLockByIdx(int idx) {
		lock.set(idx, 0);
	}

	public void setColumn(int pk, byte name, int version, byte[] src, int off, int len) {
		int iPk = ix(pk);
		int vI = iPk * cols + name;
		if (version < versions[vI]) {
			return;
		}
		versions[vI] = version;
		int tOff = name * 8 + iPk * recordLen;
		byte[] target = data;
		target[tOff++] = (byte) len;
		System.arraycopy(src, off, target, tOff, len);
	}

	/**
	 * @return the data
	 */
	public byte[] getData() {
		return data;
	}

}
