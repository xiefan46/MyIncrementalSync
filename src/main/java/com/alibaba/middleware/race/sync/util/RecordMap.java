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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wangkai
 *
 */
public class RecordMap {

	private int cols;
	
	private byte [] [] columns;
	
	private AtomicInteger [] powers;
	
	private AtomicBoolean [] locks;
	
	private int [] versions;

	private int	capacity;

	private int	off;

	public RecordMap(int capacity, int off,int cols) {
		this.off = off;
		this.cols = cols;
		this.capacity = capacity;
		this.init(capacity, cols);
	}
	
	private void init(int capacity,int cols){
		this.columns = new byte[capacity][];
		this.versions = new int[cols * capacity];
		this.powers = new AtomicInteger[capacity];
		this.locks = new AtomicBoolean[capacity];
		for (int i = 0; i < capacity; i++) {
			columns[i] = new byte[cols * 8];
			powers[i] = new AtomicInteger();
			locks[i] = new AtomicBoolean();
		}
	}

	public byte[] getResult(int pk) {
		int idx = ix(pk);
		if (powers[idx].get() == 1) {
			return columns[idx];
		}
		return null;
	}

	public byte[] getColumns(int pk) {
		return columns[ix(pk)];
	}

	public void powerIncrement(int pk) {
		powers[ix(pk)].incrementAndGet();
	}
	
	public void powerDecrement(int pk) {
		powers[ix(pk)].decrementAndGet();
	}
	
	private int ix(int pk){
		return pk - off;
	}

	public void lockRecord(int pk){
		AtomicBoolean lock = locks[ix(pk)];
		if (!lock.compareAndSet(false, true)) {
			for(;lock.compareAndSet(false, true);){
			}
		}
	}
	
	public void releaseRecordLock(int pk){
		locks[ix(pk)].set(false);
	}
	
	public void setColumn(int pk,byte name,int version, byte[] src, int off, int len){
		int iPk = ix(pk);
		int vI = iPk * cols + name;
		if (version < versions[vI]) {
			return;
		}
		versions[vI] = version;
		int tOff = name * 8;
		byte [] target = columns[iPk];
		target[tOff++] = (byte)len;
		int end = off + len;
		for (int i = off; i < end; i++) {
			target[tOff++] = src[i];
		}
	}

}
